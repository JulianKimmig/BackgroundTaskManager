import logging
import random
import threading
import time


class BackgroundTask():
    def __init__(self, id, method, delay):
        self.delay = delay
        self.method = method
        self.id = id
        self._last_run = 0

    def check_for_run(self, time=None):
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
        self.method()


class BackgroundTaskRunner():
    def __init__(self, background_sleep_time=1, start_in_background=True):
        """
        :param start_in_background: weather the task runner should start in background
        :type start_in_background: bool
        :param background_sleep_time: time in seconds betwen the call of the background tasks
        :type background_sleep_time: float
        """
        self.background_sleep_time = background_sleep_time
        self._running = False

        self.name = getattr(self, "name", self.__class__.__name__)
        self.logger = logging.getLogger(self.name)
        self._background_tasks = {}
        # run watcher in background
        if start_in_background:
            threading.Thread(target=self.run_forever, daemon=True).start()

    def run_background_tasks(self):
        for id, task in self._background_tasks.items():
            task.check_for_run(self._time)

    def stop(self):
        self._running = False

    def __del__(self):
        self.stop()

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

    def register_background_task(self, method, minimum_call_delay=None):
        if not callable(method):
            self.logger.error("cannot register background task, method is not callable")
            return

        task_id = random.randint(1, 10 ** 6)
        while task_id in self._background_tasks:
            task_id = random.randint(1, 10 ** 6)
        self._background_tasks[task_id] = BackgroundTask(id=task_id, method=method, delay=minimum_call_delay)
        return task_id