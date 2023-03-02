"""
Inversion-of-control micro-framework for parallel execution of a directed acyclic graph of actions. Useful for both
CPU-intensive and IO-bound tasks. Demonstrated in the RKSTR8 batch processing platform initialization process.

Author: Michael Gilson
"""
from __future__ import annotations
import abc
import enum
from typing import List, Dict, Callable, Any, Iterable
import queue
import threading
import multiprocessing
import traceback
import sys

MAX_THREADS_DEFAULT = multiprocessing.cpu_count() - 1

class State(enum.Enum):
    PENDING = 1
    READY = 2
    ENQUEUED = 3
    RUNNING = 4
    SUCCESS = 5
    FAIL = 6


class Action(abc.ABC):
    '''Combination of Command and Observer patterns for concurrent execution of a directed acyclic graph of actions.
       Execution of the top-level Action DAG is managed by a producer-consumer model using a queue and pool of consumer
       threads. Control flow within each Action (and its collaborating objects), as implemented by the work method,
       should be isolated from the remaining Actions and in-general use the call stack. The observer pattern is used to
       notify and enqueue pending actions only when their dependent actions are complete and dependencies available.
    '''

    def __init__(self, q: queue.Queue, produces: List[str], dependencies: Dict[str, Action]):
        self.q: queue.Queue = q
        self.observers: List[Action] = []
        # happens-before dependencies that do not involve data transfer between actions should use a unique key name
        # prefixed by the '$' character. E.g. {'$dep1': dep1, '$dep2': dep2}, since the dependencies are modeled by
        # dict which does not allow duplicate keys. dependencies without this prefix will have properties accessed by
        # their key name in the dict, and have this passed as a positional arg to the work method. happens-before deps
        # are not passed as positional args to the work method.
        self.dependencies: Dict[str, Action] = dependencies
        self.state: State = State.PENDING if len(dependencies) > 0 else State.READY
        self.produces: List[str] = produces
        # protects observering threads from interleaving calls to notify and enqueing this action multiple times
        self.mutex = threading.Lock()
        for dep in self.dependencies.values():
            dep.add_observer(self) # an action is an observer of all of its dependencies, they notify after exiting work

    def add_observer(self, observer: Action):
        self.observers.append(observer)

    @abc.abstractmethod
    def work(self):
        pass

    def cleanup(self):
        pass

    def execute(self, dry_run: bool=False):
        try:
            args = [getattr(dep, prop) for prop, dep in self.dependencies.items() if not prop.startswith('$')]
            products = self.work(*args, dry_run=dry_run)
            for i, prop in enumerate(self.produces):
                setattr(self, prop, products[i])
            self.state = State.SUCCESS
        except Exception as e:
            self.state = State.FAIL
            raise e # signals this action's consumer to set the shared stop event and stop all from consuming the queue
        finally:
            # final state is fixed before notifying observers. gaurentees that all dependencies of an action are in
            # their final state by the last call to notify so the conditions for enqueueing (or not) are correct.
            for observer in self.observers:
                observer.notify() # protected by mutex to prevent interleaving calls leading to multiple enqueueing

    def notify(self):
        with self.mutex:
            if self.state in [State.PENDING, State.READY]:
                if all([dep.state == State.SUCCESS for dep in self.dependencies.values()]):
                    try:
                        self.q.put(self) # uses a lock for the mutating deque
                    finally:
                        self.state = State.ENQUEUED # ensures waiting threads don't enqueue multiple times


class ActionConsumer(threading.Thread):

    def __init__(self, q: queue.Queue, stop: threading.Event, dry_run: bool):
        super().__init__()
        self.q = q
        self.stop = stop
        self.dry_run = dry_run

    def run(self):
        try:
            self.consume_q()
        finally:
            self.drain() # drain the queue of any remaining actions in case of failure

    def consume_q(self):
        while not self.stop.is_set():
            action = self.q.get()
            action.state = State.RUNNING
            try:
                action.execute(dry_run=self.dry_run)
            except Exception as e:
                print('Action failed: {}'.format(action.__class__.__name__))
                print(traceback.format_exc())
                print(sys.exc_info()[2])
                self.stop.set()
            finally:
                self.q.task_done()

    def drain(self):
        while not self.q.empty():
            self.q.get(block=False)
            self.q.task_done()


class Executor:

    def __init__(self, q: queue.Queue, actions: Iterable[Action]):
        self.q = q
        self.actions = actions
        # controls whether consumers should continue consuming the queue. triggered by an action failing.
        # this is all-or-nothing granularity and currently there are no rollbacks for completed actions.
        self.stop = threading.Event()

    def execute(self, dry_run: bool=False, parallel: bool=True, max_threads: int=MAX_THREADS_DEFAULT):
        q = self.q
        stop = self.stop
        actions = self.actions
        n_threads = max_threads if parallel else 1
        for _ in range(n_threads):
            consumer = ActionConsumer(q, stop, dry_run)
            consumer.daemon = True
            consumer.start()
        for action in actions:
            if action.state == State.READY:
                print('Enqueuing action: {}'.format(action.__class__.__name__))
                q.put(action)
        # consumers have a drain function to prevent indefinite waiting at join when the stop event is set on failure
        q.join()

