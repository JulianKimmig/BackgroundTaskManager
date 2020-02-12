import asyncio
import logging
import random
import threading
import time
from asyncio import iscoroutine
from inspect import iscoroutinefunction


class BackgroundTask:
    def __init__(self, id, method, delay, max_callings=None):
        self.remain_callings = max_callings
        self.delay = delay
        self.method = method
        self.id = id
        self._last_run = time.time()

    def check_for_run(self, time=None):
        if self.remain_callings == 0:
            self.stop()
            return None

        if self.delay is None:
            return self.run(time)

        if time is None:
            time = time.time()
        if time >= self._last_run + self.delay:
            return self.run(time)

    def run(self, time=None):
        if time is None:
            time = time.time()
        self._last_run = time
        if self.remain_callings:
            self.remain_callings -= 1
        return self.method()

    def stop(self):
        del self


USE_ASYNCIO = False


def set_asyncio(use=USE_ASYNCIO):
    global USE_ASYNCIO
    USE_ASYNCIO = use


def get_asyncio():
    return USE_ASYNCIO


_DUMMYRUNTIME = 0.5


class AbstractBackgroundTaskRunner():
    ALL_RUNNINGS = set()

    def __init__(self, background_sleep_time=1):
        self.background_sleep_time = background_sleep_time
        self._running = False

        self.logger = getattr(self, "logger", logging.getLogger(getattr(self, "name", self.__class__.__name__)))
        self._background_tasks = {}

        # run watcher in background
        self._time = time.time()
        self._last_run = 0
        self.background_task = None

        AbstractBackgroundTaskRunner.ALL_RUNNINGS.add(self)

    def run_background_tasks(self):
        for id, task in self._background_tasks.items():
            task.check_for_run(self._time)

    def stop(self):
        for id in self._background_tasks.copy():
            self.stop_task(id)
        self._running = False

    def stop_task(self, id):
        if id in self._background_tasks:
            try:
                self._background_tasks[id].stop()
            except:
                pass
            try:
                del self._background_tasks[id]
            except:
                pass

    def __del__(self):
        self.stop()
        try:
            AbstractBackgroundTaskRunner.ALL_RUNNINGS.remove(self)
        except:
            pass

    def _check_run(self):
        self._time = time.time()
        if (self._time + self.background_sleep_time >= self._last_run):
            self._last_run = self._time
            self.run_background_tasks()

    def register_background_task(self, method, minimum_call_delay=None, max_callings=None):
        if not callable(method):
            raise ValueError("cannot register background task, method is not callable")

        if iscoroutinefunction(method):
            raise ValueError("background method should not be a croutine, so far")

        task_id = random.randint(1, 10 ** 6)
        while task_id in self._background_tasks:
            task_id = random.randint(1, 10 ** 6)
        self._background_tasks[task_id] = BackgroundTask(
            id=task_id, method=method, delay=minimum_call_delay, max_callings=max_callings
        )
        return task_id

    def run_forever(self):
        raise NotImplementedError("run_forever not extended")

    def run_in_background(self):
        raise NotImplementedError("run_in_background not extended")

    def run_once(self, func, callback=None, *args, **kwargs):
        if iscoroutinefunction(func):
            async def f():
                if callback:
                    return callback(await func(*args, **kwargs))
                else:
                    return await func(*args, **kwargs)
        else:
            def f():
                if callback:
                    return callback(func(*args, **kwargs))
                else:
                    return func(*args, **kwargs)
        self.register_background_task(f, minimum_call_delay=0, max_callings=1)


class ThreadedBackgroundTaskRunner(AbstractBackgroundTaskRunner):
    def __init__(self, background_sleep_time=1, start_in_background=True):
        AbstractBackgroundTaskRunner.__init__(self, background_sleep_time=background_sleep_time)
        if start_in_background:
            self.run_in_background()

    def run_in_background(self):
        new_task = threading.Thread(target=self.run_forever, daemon=True)
        new_task.start()
        while self.background_task:  # waits fo the old runner to finish
            time.sleep(0.1)
        self.background_task = new_task

    def run_forever(self):
        # if running, then stop
        if self._running:
            self.stop()
        self._running = True
        self.logger.info(f"start")
        while self._running:
            self._check_run()
            time.sleep(min(self.background_sleep_time, _DUMMYRUNTIME))

    def stop(self):
        AbstractBackgroundTaskRunner.stop(self)
        try:
            self.background_task.join(_DUMMYRUNTIME * 2)
        except:
            pass
        self.background_task = None

    def register_background_task(self, method, minimum_call_delay=None, max_callings=None):
        nm = method
        if iscoroutinefunction(method):
            def _nm():
                try:
                    loop = asyncio.get_event_loop()
                except:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                return loop.run_until_complete(method())

            nm = _nm
        return AbstractBackgroundTaskRunner.register_background_task(self, method=nm,
                                                                     minimum_call_delay=minimum_call_delay,
                                                                     max_callings=max_callings)

class AsyncBackgroundTaskRunner(AbstractBackgroundTaskRunner):
    def __init__(self, background_sleep_time=1, start_in_background=False):
        AbstractBackgroundTaskRunner.__init__(self, background_sleep_time=background_sleep_time)

        if start_in_background:
            self.run_in_background()

    def run_in_background(self):
        self.logger.critical(
            "start in background on a async runner will create a new thread containing a serperate event loop. please use the corona of 'run_forever' and work it manually!!!!")
        def threading_event_runner(loop):
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self.run_forever())

        self.background_task = asyncio.new_event_loop()
        threading.Thread(target=threading_event_runner, daemon=True, args=[self.background_task]).start()

    async def run_forever(self):
        # if running, then stop
        if self._running:
            self.stop()
        self._running = True
        self.logger.info(f"start")
        while self._running:
            self._check_run()
            await asyncio.sleep(min(self.background_sleep_time, _DUMMYRUNTIME))


class BackgroundTaskRunner(AbstractBackgroundTaskRunner):
    def __init__(self, background_sleep_time=1, start_in_background=False, use_asyncio=None):  # backwarts comp
        super().__init__(background_sleep_time)
        if use_asyncio is None:
            use_asyncio = get_asyncio()

        import inspect
        from functools import partial
        if use_asyncio:
            for pair in inspect.getmembers(AsyncBackgroundTaskRunner, predicate=inspect.isfunction):
                name, func = pair
                setattr(self, name, partial(func, self))

            AsyncBackgroundTaskRunner.__init__(self, background_sleep_time=background_sleep_time,
                                               start_in_background=start_in_background)
        else:
            for pair in inspect.getmembers(ThreadedBackgroundTaskRunner, predicate=inspect.isfunction):
                name, func = pair
                setattr(self, name, partial(func, self))

            ThreadedBackgroundTaskRunner.__init__(self, background_sleep_time=background_sleep_time,
                                                  start_in_background=start_in_background)

    def run_task(self, target, blocking=False, callback=None, *args, **kwargs):
        if blocking:
            r = target(*args, **kwargs)
            if iscoroutine(r):
                r = asyncio.get_event_loop().run_until_complete(asyncio.gather(r))
            if callback:
                return callback(r)
            else:
                return r
        else:
            return self.run_once(func=target, callback=callback, *args, **kwargs)

def run_all_until_done():
    asnyc_tasks = set()
    threading_tasks = set()
    threading_tasks_runner = set()
    async_tasks_runner = set()

    import functools
    for btr in AbstractBackgroundTaskRunner.ALL_RUNNINGS:
        rff = btr.run_forever
        while isinstance(rff, functools.partial):
            rff = rff.func
        if iscoroutinefunction(rff):
            async_tasks_runner.add(btr)
        else:
            threading_tasks_runner.add(btr)

    print(threading_tasks_runner)
    for runner in threading_tasks_runner:
        runner.run_in_background()
        threading_tasks.add(runner.background_task)

    print(async_tasks_runner)
    for runner in async_tasks_runner:
        asnyc_tasks.add(runner.run_forever())

    asyncio.get_event_loop().run_until_complete(asyncio.gather(*asnyc_tasks))

    for t in threading_tasks:
        if t.isAlive():
            t.join()
