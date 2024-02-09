import threading
from queue import Empty, Full
from collections import deque
from time import time

class ClosableQueue():

    def __init__(self, maxsize=0):
        self.maxsize = maxsize
        self._init(maxsize)

        self.mutex = threading.Lock()

        self.not_empty = threading.Condition(self.mutex)

        self.not_full = threading.Condition(self.mutex)

        self.all_tasks_done = threading.Condition(self.mutex)
        self.unfinished_tasks = 0
        self.closed = False

    def task_done(self):
        with self.all_tasks_done:
            unfinished = self.unfinished_tasks - 1
            if unfinished <= 0:
                if unfinished < 0:
                    raise ValueError('task_done() called too many times')
                self.all_tasks_done.notify_all()
            self.unfinished_tasks = unfinished

    def join(self):
        with self.all_tasks_done:
            while self.unfinished_tasks and not self.closed:
                self.all_tasks_done.wait()

    def qsize(self):
        with self.mutex:
            return self._qsize()

    def empty(self):
        with self.mutex:
            return not self._qsize()

    def full(self):
        with self.mutex:
            return 0 < self.maxsize <= self._qsize()

    def put_nowait(self, item):
        return self.put(item, block=False)

    def get_nowait(self):
        return self.get(block=False)

    def _init(self, maxsize):
        self.queue = deque()

    def _qsize(self):
        return len(self.queue)

    def _put(self, item):
        self.queue.append(item)

    def _get(self):
        return self.queue.popleft()

    def close(self):
        with self.mutex:
            self.closed = True
            self.not_empty.notify_all()
            self.not_full.notify_all()
            self.all_tasks_done.notify_all()

    def getall(self, block=True, timeout=None):
        with self.not_empty:
            if self.closed:
                raise ValueError('queue closed')
            if not block:
                if not self._qsize():
                    raise Empty
            elif timeout is None:
                while not self._qsize() and not self.closed:
                    self.not_empty.wait()
            elif timeout < 0:
                raise ValueError("'timeout' must be a non-negative number")
            else:
                endtime = time() + timeout
                while not self._qsize() and not self.closed:
                    remaining = endtime - time()
                    if remaining <= 0.0:
                        raise Empty
                    self.not_empty.wait(remaining)
            if self.closed:
                raise ValueError('queue closed')
            items = tuple(self.queue)
            self.queue.clear()
            self.not_full.notify()
            return items

    def clear(self):
        with self.not_empty:
            self.queue.clear()
            self.not_full.notify()

    def put(self, item, block=True, timeout=None):
        with self.not_full:
            if self.closed:
                raise ValueError('queue closed')
            if self.maxsize > 0:
                if not block:
                    if self._qsize() >= self.maxsize:
                        raise Full
                elif timeout is None:
                    while self._qsize() >= self.maxsize and not self.closed:
                        self.not_full.wait()
                elif timeout < 0:
                    raise ValueError("'timeout' must be a non-negative number")
                else:
                    endtime = time() + timeout
                    while self._qsize() >= self.maxsize and not self.closed:
                        remaining = endtime - time()
                        if remaining <= 0.0:
                            raise Full
                        self.not_full.wait(remaining)
            if self.closed:
                raise ValueError('queue closed')
            self._put(item)
            self.unfinished_tasks += 1
            self.not_empty.notify()

    def get(self, block=True, timeout=None):
        with self.not_empty:
            if self.closed:
                raise ValueError('queue closed')
            if not block:
                if not self._qsize():
                    raise Empty
            elif timeout is None:
                while not self._qsize() and not self.closed:
                    self.not_empty.wait()
            elif timeout < 0:
                raise ValueError("'timeout' must be a non-negative number")
            else:
                endtime = time() + timeout
                while not self._qsize() and not self.closed:
                    remaining = endtime - time()
                    if remaining <= 0.0:
                        raise Empty
                    self.not_empty.wait(remaining)
            if self.closed:
                raise ValueError('queue closed')
            item = self._get()
            self.not_full.notify()
            return item