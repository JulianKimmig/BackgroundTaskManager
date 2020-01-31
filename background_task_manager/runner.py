import asyncio
import logging
import random
import threading
import time
from asyncio import iscoroutine
from inspect import iscoroutinefunction


class BackgroundTask:
    def __init__(self, id, method, delay, callings=-1):
        self.callings = callings
        self.delay = delay
        self.method = method
        self.id = id
        self._last_run = time.time()

    def check_for_run(self, time=None):
        if self.callings == 0:
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
        return self.method()

    def stop(self):
        del self


USE_ASYNCIO = False


def set_asyncio(use=USE_ASYNCIO):
    global USE_ASYNCIO
    USE_ASYNCIO = use


def get_asyncio():
    return USE_ASYNCIO

class BackgroundTaskRunner:
    ALL_RUNNINGS = set()
    def __init__(self, background_sleep_time=1, start_in_background=True,
                 use_asyncio=None):
        """
        :param start_in_background: weather the task runner should start in background
        :type start_in_background: bool
        :param background_sleep_time: time in seconds betwen the call of the background tasks
        :type background_sleep_time: float
        """
        if use_asyncio is None:
            use_asyncio = get_asyncio()
        self.background_sleep_time = background_sleep_time
        self._running = False

        if not hasattr(self, "name"):
            self.name = self.__class__.__name__

        self.logger = getattr(self, "logger", logging.getLogger(self.name))
        self._background_tasks = {}

        # run watcher in background
        self._time = time.time()
        self.use_asyncio = use_asyncio

        if self.use_asyncio:
            self.background_task = asyncio.Task(self.async_run_forever())

        if start_in_background:
            if self.use_asyncio:
                pass
            else:
                self.background_task = threading.Thread(target=self.run_forever, daemon=True)
                self.background_task.start()
        BackgroundTaskRunner.ALL_RUNNINGS.add(self)

    def register_background_task(self, method, minimum_call_delay=None, callings=-1):
        if not callable(method):
            self.logger.error("cannot register background task, method is not callable")
            return

        task_id = random.randint(1, 10 ** 6)
        while task_id in self._background_tasks:
            task_id = random.randint(1, 10 ** 6)
        self._background_tasks[task_id] = BackgroundTask(
            id=task_id, method=method, delay=minimum_call_delay, callings=callings
        )
        return task_id

    def stop(self):
        for id in self._background_tasks:
            self.stop_task(id)

        self._running = False

    async def async_run_forever(self):
        # if running, then stop
        if self._running:
            self.stop()
        self._running = True
        self.logger.info(f"start {self.name}")
        while self._running:
            self._time = time.time()
            self.run_background_tasks()
            await asyncio.sleep(self.background_sleep_time)

    def run_forever(self):
        # if running, then stop
        if self._running:
            self.stop()
        self._running = True
        self.logger.info(f"start {self.name}")
        while self._running:
            self._time = time.time()
            self.run_background_tasks()
            time.sleep(self.background_sleep_time)

    def run_background_tasks(self):
        for id, task in self._background_tasks.items():
            task.check_for_run(self._time)

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
            if self.use_asyncio:
                async def wrapper():
                    if iscoroutinefunction(target):
                        sr = await target(*args, **kwargs)
                        if iscoroutinefunction(callback):
                            return await callback(sr)
                    else:
                        def thrad_func():
                            if callback:
                                callback(target(*args, **kwargs))
                            else:
                                target(*args, **kwargs)

                        threading.Thread(target=thrad_func, daemon=True).start()

                r = wrapper()
                try:
                    asyncio.get_event_loop().create_task(r)
                except:
                    asyncio.set_event_loop(asyncio.new_event_loop())
                    asyncio.get_event_loop().run_until_complete(r)
                    asyncio.set_event_loop(None)
            else:
                def wrapper():
                    r = target(*args, **kwargs)
                    if iscoroutine(r):
                        asyncio.set_event_loop(asyncio.new_event_loop())
                        r = asyncio.get_event_loop().run_until_complete(asyncio.gather(r))
                    if callback:
                        if iscoroutinefunction(callback):
                            asyncio.get_event_loop().run_until_complete(asyncio.gather(callback(r)))
                        else:
                            callback(r)

                threading.Thread(target=wrapper, daemon=True).start()

    def stop_task(self, id):
        if id in self._background_tasks:
            self._background_tasks[id].stop()
            del self._background_tasks[id]

    def __del__(self):
        self.stop()
        try:
            BackgroundTaskRunner.ALL_RUNNINGS.remove(self)
        except:
            pass


def run_all_until_done():
    asnyc_tasks = set()
    thrading_tasks = set()
    print(BackgroundTaskRunner.ALL_RUNNINGS)
    for btr in BackgroundTaskRunner.ALL_RUNNINGS:
        print(btr.background_task)
        if isinstance(btr.background_task, asyncio.Task):
            asnyc_tasks.add(btr.background_task)
        if isinstance(btr.background_task, threading.Thread):
            thrading_tasks.add(btr.background_task)

    print(asnyc_tasks, thrading_tasks)
    asyncio.get_event_loop().run_until_complete(asyncio.gather(*asnyc_tasks))
    for t in thrading_tasks:
        if t.isAlive():
            t.join()
