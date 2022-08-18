import logging
import os
import signal
import threading
import time
import multiprocessing
from multiprocessing.dummy import Pool as ThreadPool
from flask import current_app

HANDLED_SIGNALS = {signal.SIGINT, signal.SIGTERM}
if hasattr(signal, "SIGHUP"):  # pragma: no cover
    HANDLED_SIGNALS.add(signal.SIGHUP)
if hasattr(signal, "SIGBREAK"):  # pragma: no cover
    HANDLED_SIGNALS.add(signal.SIGBREAK)


def try_block_signals():
    """Blocks HANDLED_SIGNALS on platforms that support it."""
    if hasattr(signal, "pthread_sigmask"):
        signal.pthread_sigmask(signal.SIG_BLOCK, HANDLED_SIGNALS)


def try_unblock_signals():
    """Unblocks HANDLED_SIGNALS on platforms that support it."""
    if hasattr(signal, "pthread_sigmask"):
        signal.pthread_sigmask(signal.SIG_UNBLOCK, HANDLED_SIGNALS)


class ThreadPoolProcessor:
    """Executes a function in multiple threads.

    The passed `get_args_collection` function will be called from the
    main thread ad infinitum. It should return a list of tuples. Then
    for each of the tuples in the returned list, the `process_func`
    will be called in worker threads, until the list is exhausted. A
    pause of at least `wait_seconds` will be made between the
    sequential calls of `get_args_collection`.

    """

    def __init__(self, threads, *, get_args_collection, process_func, wait_seconds):
        self.logger = logging.getLogger(__name__)
        self.threads = threads
        self.get_args_collection = get_args_collection
        self.process_func = process_func
        self.wait_seconds = wait_seconds
        self.all_done = threading.Condition()
        self.pending = 0
        self.error_has_occurred = False

    def _wait_until_all_done(self):
        while self.pending > 0:
            self.all_done.wait()
        assert self.pending == 0

    def _mark_done(self, result=None):
        with self.all_done:
            self.pending -= 1
            if self.pending <= 0:
                self.all_done.notify()

    def _log_error(self, e):  # pragma: no cover
        self._mark_done()
        try:
            raise e
        except Exception:
            self.logger.exception('Caught error while processing objects.')

        self.error_has_occurred = True

    def run(self, *, quit_early=False):
        app = current_app._get_current_object()

        def push_app_context():
            ctx = app.app_context()
            ctx.push()

        pool = ThreadPool(self.threads, initializer=push_app_context)
        iteration_counter = 0

        while not (self.error_has_occurred or (quit_early and iteration_counter > 0)):
            iteration_counter += 1
            started_at = time.time()
            args_collection = self.get_args_collection()

            with self.all_done:
                self.pending += len(args_collection)

            for args in args_collection:
                pool.apply_async(self.process_func, args, callback=self._mark_done, error_callback=self._log_error)

            with self.all_done:
                self._wait_until_all_done()

            time.sleep(max(0.0, self.wait_seconds + started_at - time.time()))

        pool.close()
        pool.join()


def spawn_worker_processes(processes: int, target, **kwargs):
    """Spawns the specified number of processes, each executing the passed
    target function. In each worker process, the `target` function
    will be called with the passed keyword arguments (`kwargs`), and
    should performs its work ad infinitum.

    Note that each worker process inherits blocked SIGTERM and SIGINT
    signals from the parent process. The `target` function must
    unblock them at some point, by calling `try_unblock_signals()`.

    This function will not return until at least one of the worker
    processes has stopped. In this case, the rest of the workers will
    be terminated as well.

    """

    while processes < 1:  # pragma: no cover
        time.sleep(1)
    assert processes >= 1

    worker_processes = []
    worker_processes_have_been_terminated = False

    def worker(**kwargs):
        try:
            target(**kwargs)
        except Exception:
            logger = logging.getLogger(__name__)
            logger.exception("Uncaught exception occured in worker with PID %i.", os.getpid())

    def terminate_worker_processes():
        nonlocal worker_processes_have_been_terminated
        if not worker_processes_have_been_terminated:
            for p in worker_processes:
                p.terminate()
            worker_processes_have_been_terminated = True

    def sighandler(signum, frame):  # pragma: no cover
        logger.info('Received "%s" signal. Shutting down...', signal.strsignal(signum))
        terminate_worker_processes()

    # To prevent the main process from exiting due to signals after
    # worker processes have been defined but before the signal
    # handling has been configured for the main process, block those
    # signals that the main process is expected to handle.
    try_block_signals()

    logger = logging.getLogger(__name__)
    logger.info('Spawning %i worker processes...', processes)

    for _ in range(processes):
        p = multiprocessing.Process(target=worker, kwargs=kwargs)
        p.start()
        worker_processes.append(p)

    for sig in HANDLED_SIGNALS:
        signal.signal(sig, sighandler)

    assert all(p.pid is not None for p in worker_processes)
    try_unblock_signals()

    # This loop waits until all worker processes have exited. However,
    # as soon as one worker process exits, all remaining worker
    # processes will be forcefully terminated.
    while any(p.exitcode is None for p in worker_processes):
        for p in worker_processes:
            p.join(timeout=1)
            if p.exitcode is not None and not worker_processes_have_been_terminated:
                logger.warning("Worker with PID %r exited unexpectedly. Shutting down...", p.pid)
                terminate_worker_processes()
                break
