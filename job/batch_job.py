# This is an implementation of layer executor

# design principles
# 1. Every *Task* will not be executed immediately, they will be merged into one *job*
# 2. Every *Job* is composed of several tasks
# 3. Tasks are accumulated by a fixed time interval or fixed account
# 4. Job has it's own executor, can be threadpool or multiprocess
# 5. Caller can fetch it's task result by a future object (The idea can be took from https://docs.python.org/3/library/concurrent.futures.html)


import weakref
import threading
from threading import Lock, Condition, Thread
import time as w_time
import logging
from job import _base

try:
    from Queue import Queue
except ImportError:
    from queue import Queue

MAX_QUEUE_SIZE = 100000

# test
FORMAT = '%(asctime)-15s  %(message)s'
# console = logging.StreamHandler()
# console.setLevel(logging.DEBUG)
logging.basicConfig(format=FORMAT, level=logging.DEBUG)
# logging.getLogger('').addHandler(console)
LOG = logging.getLogger(__name__)


class BatchJob(object):
    def __init__(self, batch_size=10, max_wait_seconds=10):
        self.batch_size = batch_size
        self.max_wait_seconds = max_wait_seconds
        self.task_queue = Queue(maxsize=MAX_QUEUE_SIZE)
        self._work_queue = Queue()
        self.shared_lock = Lock()
        self.job_condition = Condition(lock=self.shared_lock)

        # interval object
        self._handler = None
        self._threads = set()

    def submit(self, resource):
        with self.job_condition:
            self.task_queue.put(resource)
            self.job_condition.notify()

        LOG.debug("Added item to job queue.")

        return

    def _get_job_future(self):
        pass

    def start(self):
        if self._handler is None:
            return ValueError('Please set the proper ResourceHandler.')
        remaining = self.max_wait_seconds
        while True:
            with self.job_condition:
                LOG.debug("remaining: {}".format(remaining))
                end_time = w_time.time() + remaining
                if self.task_queue.qsize() >= self.batch_size:
                    self._run(with_size=self.batch_size)
                    remaining = self.max_wait_seconds
                else:
                    if remaining <= 0.0:
                        #  timed out
                        self._run(with_size=self.task_queue.qsize())
                        remaining = self.max_wait_seconds
                    else:
                        LOG.debug("waiting up to {}".format(remaining))
                        self.job_condition.wait(remaining)
                        remaining = end_time - w_time.time()

    def _run(self, with_size):
        if with_size == 0:
            return
        LOG.debug("Starting job with size=[{}]".format(with_size))
        i = 0
        items = []
        while (i < with_size):
            items.append(self.task_queue.get_nowait())
            i += 1
        # Spawn new thread for job run.
        try:
            fn = self._handler().compose(items)
            if callable(target):
                worker = self._compose_job(fn)

                # When the executor gets lost, the weakref callback will wake up
                # the worker threads.
                def weakref_cb(_, q=self._work_queue):
                    q.put(None)

                self._work_queue.put(worker)
                t = threading.Thread(target=_base._worker,
                                     args=(weakref.ref(self, weakref_cb),
                                           self._work_queue))
                t.daemon = True
                t.start()
                self._threads.add(t)
            else:
                LOG.warning("Not a callable.")
        except Exception:
            # Failed to start a job for sth.
            raise

    def _compose_job(self, fn):
        f = _base.Future()
        w = _base._WorkItem(f, fn)

        return w

    def set_handler_class(self, handler_clz):
        self._handler = handler_clz


class ResourceHandlerBase(object):
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def compose(self, items):
        """Return a callable for spawning new thread."""
        raise NotImplementedError


class ExampleHandler(ResourceHandlerBase):
    def __init__(self, *args, **kwargs):
        super(ExampleHandler, self).__init__(*args, **kwargs)

    def compose(self, items):
        def _inner():
            sum = 0
            for fn in items:
                sum += fn()

        return _inner


class Executor(object):
    def __init__(self, periodic=0):
        self.periodic = periodic
        self.fns = []

    def add_callable(self, fn):
        self.fns.append(fn)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self


def target(job):
    job.submit(lambda: 3 + 5)
    job.submit(lambda: 7 + 1)
    w_time.sleep(4.5)
    job.submit(lambda: 6 + 3)
    job.submit(lambda: 1 + 3)


def target1(job):
    import random
    for x in range(100):
        job.submit(lambda : x + 10)
        w_time.sleep(random.random())

if __name__ == "__main__":
    job = BatchJob(batch_size=10, max_wait_seconds=4)
    job.set_handler_class(ExampleHandler)
    t = Thread(target=target, args=(job,))
    t.start()
    t1 = Thread(target=target1, args=(job, ))
    t1.start()

    t2 = Thread(target=target1, args=(job,))
    t2.start()
    job.start()
