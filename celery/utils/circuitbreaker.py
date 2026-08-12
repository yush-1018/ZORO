import threading
from collections import deque
from time import monotonic

from celery import signals

STATE_CLOSED = 'closed'
STATE_OPEN = 'open'
STATE_HALF_OPEN = 'half_open'

_VALID_STATES = frozenset({STATE_CLOSED, STATE_OPEN, STATE_HALF_OPEN})


class CircuitBreakerMetrics:

    def __init__(self):
        self._lock = threading.Lock()
        self.total_calls = 0
        self.successful_calls = 0
        self.failed_calls = 0
        self.rejected_calls = 0
        self.trip_count = 0

    def record_call(self):
        with self._lock:
            self.total_calls += 1

    def record_success(self):
        with self._lock:
            self.successful_calls += 1

    def record_failure(self):
        with self._lock:
            self.failed_calls += 1

    def record_rejection(self):
        with self._lock:
            self.rejected_calls += 1

    def record_trip(self):
        with self._lock:
            self.trip_count += 1

    def error_rate(self):
        with self._lock:
            if self.total_calls == 0:
                return 0.0
            return (self.failed_calls / self.total_calls) * 100.0

    def as_dict(self):
        with self._lock:
            return {
                'total_calls': self.total_calls,
                'successful_calls': self.successful_calls,
                'failed_calls': self.failed_calls,
                'rejected_calls': self.rejected_calls,
                'trip_count': self.trip_count,
                'error_rate_pct': (
                    (self.failed_calls / self.total_calls * 100.0)
                    if self.total_calls > 0 else 0.0
                ),
            }


class CircuitBreakerListener:

    def on_state_change(self, breaker, old_state, new_state):
        pass

    def on_failure(self, breaker, exc):
        pass

    def on_success(self, breaker):
        pass


class CircuitBreaker:

    def __init__(self, task_name, threshold=5, recovery_timeout=60.0,
                 half_open_max_calls=1, failure_window=60.0,
                 exclude=None, clock=monotonic):
        if not task_name:
            raise ValueError('task_name must be a non-empty string')
        if threshold < 1:
            raise ValueError(
                f'threshold must be >= 1, got {threshold}')
        if recovery_timeout <= 0:
            raise ValueError(
                f'recovery_timeout must be > 0, got {recovery_timeout}')
        if half_open_max_calls < 1:
            raise ValueError(
                f'half_open_max_calls must be >= 1, got {half_open_max_calls}')
        if failure_window <= 0:
            raise ValueError(
                f'failure_window must be > 0, got {failure_window}')
        self._task_name = task_name
        self._threshold = threshold
        self._recovery_timeout = recovery_timeout
        self._half_open_max_calls = half_open_max_calls
        self._failure_window = failure_window
        self._exclude = tuple(exclude) if exclude else ()
        self._clock = clock
        self._lock = threading.Lock()
        self._state = STATE_CLOSED
        self._failure_timestamps = deque()
        self._opened_at = None
        self._half_open_call_count = 0
        self._half_open_successes = 0
        self._task_cls = None
        self._listeners = []
        self.metrics = CircuitBreakerMetrics()

    def __repr__(self):
        return (
            f'<CircuitBreaker: {self._task_name} '
            f'state={self._state} '
            f'failures={len(self._failure_timestamps)}/{self._threshold}>'
        )

    @property
    def task_name(self):
        return self._task_name

    @property
    def state(self):
        with self._lock:
            self._check_state_transition()
            return self._state

    @property
    def is_open(self):
        return self.state == STATE_OPEN

    @property
    def is_closed(self):
        return self.state == STATE_CLOSED

    @property
    def is_half_open(self):
        return self.state == STATE_HALF_OPEN

    @property
    def failure_count(self):
        with self._lock:
            self._purge_old_failures(self._clock())
            return len(self._failure_timestamps)

    @property
    def opened_at(self):
        with self._lock:
            return self._opened_at

    def add_listener(self, listener):
        if not isinstance(listener, CircuitBreakerListener):
            raise TypeError('listener must be an instance of CircuitBreakerListener')
        with self._lock:
            if listener not in self._listeners:
                self._listeners.append(listener)

    def remove_listener(self, listener):
        with self._lock:
            if listener in self._listeners:
                self._listeners.remove(listener)

    def set_task_cls(self, task_cls):
        self._task_cls = task_cls

    def should_exclude(self, exc):
        if not self._exclude:
            return False
        return isinstance(exc, self._exclude)

    def can_execute(self):
        with self._lock:
            self.metrics.record_call()
            self._check_state_transition()
            if self._state == STATE_CLOSED:
                return True
            if self._state == STATE_OPEN:
                self.metrics.record_rejection()
                return False
            if self._state == STATE_HALF_OPEN:
                if self._half_open_call_count < self._half_open_max_calls:
                    self._half_open_call_count += 1
                    return True
                self.metrics.record_rejection()
                return False
            return False

    def record_failure(self, exc=None):
        if exc is not None and self.should_exclude(exc):
            return
        with self._lock:
            now = self._clock()
            self._check_state_transition()
            self.metrics.record_failure()
            self._failure_timestamps.append(now)
            self._purge_old_failures(now)
            self._notify_listeners_failure(exc)
            if self._state == STATE_HALF_OPEN:
                self._transition_to_open(now)
                return
            if self._state == STATE_CLOSED:
                if len(self._failure_timestamps) >= self._threshold:
                    self._transition_to_open(now)

    def record_success(self):
        with self._lock:
            self._check_state_transition()
            self.metrics.record_success()
            self._notify_listeners_success()
            if self._state == STATE_HALF_OPEN:
                self._transition_to_closed()

    def reset(self):
        with self._lock:
            self._transition_to_closed()

    def stats(self):
        with self._lock:
            self._check_state_transition()
            self._purge_old_failures(self._clock())
            m = self.metrics.as_dict()
            return {
                'state': self._state,
                'failure_count': len(self._failure_timestamps),
                'threshold': self._threshold,
                'recovery_timeout': self._recovery_timeout,
                'failure_window': self._failure_window,
                'half_open_max_calls': self._half_open_max_calls,
                'half_open_call_count': self._half_open_call_count,
                'opened_at': self._opened_at,
                'total_calls': m['total_calls'],
                'successful_calls': m['successful_calls'],
                'failed_calls': m['failed_calls'],
                'rejected_calls': m['rejected_calls'],
                'trip_count': m['trip_count'],
                'error_rate_pct': m['error_rate_pct'],
            }

    def _check_state_transition(self):
        if self._state == STATE_OPEN and self._opened_at is not None:
            if self._clock() - self._opened_at >= self._recovery_timeout:
                old_state = self._state
                self._state = STATE_HALF_OPEN
                self._half_open_call_count = 0
                self._half_open_successes = 0
                signals.circuit_breaker_half_opened.send(
                    sender=None, task_name=self._task_name)
                self._notify_listeners_state_change(old_state, STATE_HALF_OPEN)

    def _transition_to_open(self, now):
        old_state = self._state
        if old_state == STATE_OPEN:
            return
        self._state = STATE_OPEN
        self._opened_at = now
        self._half_open_call_count = 0
        self._half_open_successes = 0
        self.metrics.record_trip()
        failure_count = len(self._failure_timestamps)
        signals.circuit_breaker_opened.send(
            sender=None, task_name=self._task_name,
            failure_count=failure_count)
        self._notify_listeners_state_change(old_state, STATE_OPEN)
        task_cls = self._task_cls
        if task_cls is not None and hasattr(task_cls, 'on_circuit_breaker_opened'):
            try:
                task_cls.on_circuit_breaker_opened(
                    task_name=self._task_name,
                    failure_count=failure_count,
                )
            except Exception:
                pass

    def _transition_to_closed(self):
        old_state = self._state
        if old_state == STATE_CLOSED:
            return
        self._state = STATE_CLOSED
        self._failure_timestamps.clear()
        self._opened_at = None
        self._half_open_call_count = 0
        self._half_open_successes = 0
        signals.circuit_breaker_closed.send(
            sender=None, task_name=self._task_name)
        self._notify_listeners_state_change(old_state, STATE_CLOSED)
        task_cls = self._task_cls
        if task_cls is not None and hasattr(task_cls, 'on_circuit_breaker_closed'):
            try:
                task_cls.on_circuit_breaker_closed(
                    task_name=self._task_name,
                )
            except Exception:
                pass

    def _purge_old_failures(self, now):
        cutoff = now - self._failure_window
        while self._failure_timestamps and self._failure_timestamps[0] < cutoff:
            self._failure_timestamps.popleft()

    def _notify_listeners_state_change(self, old_state, new_state):
        for listener in self._listeners:
            try:
                listener.on_state_change(self, old_state, new_state)
            except Exception:
                pass

    def _notify_listeners_failure(self, exc):
        for listener in self._listeners:
            try:
                listener.on_failure(self, exc)
            except Exception:
                pass

    def _notify_listeners_success(self):
        for listener in self._listeners:
            try:
                listener.on_success(self)
            except Exception:
                pass


class CircuitBreakerRegistry:

    def __init__(self):
        self._breakers = {}
        self._lock = threading.Lock()

    def __repr__(self):
        with self._lock:
            return (
                f'<CircuitBreakerRegistry: '
                f'{len(self._breakers)} breaker(s) registered>'
            )

    def __len__(self):
        with self._lock:
            return len(self._breakers)

    def __contains__(self, task_name):
        with self._lock:
            return task_name in self._breakers

    def get_or_create(self, task_name, threshold=5, recovery_timeout=60.0,
                      half_open_max_calls=1, failure_window=60.0,
                      exclude=None, clock=monotonic):
        with self._lock:
            if task_name not in self._breakers:
                self._breakers[task_name] = CircuitBreaker(
                    task_name=task_name,
                    threshold=threshold,
                    recovery_timeout=recovery_timeout,
                    half_open_max_calls=half_open_max_calls,
                    failure_window=failure_window,
                    exclude=exclude,
                    clock=clock,
                )
            return self._breakers[task_name]

    def get(self, task_name):
        with self._lock:
            return self._breakers.get(task_name)

    def all_stats(self):
        with self._lock:
            return {
                name: breaker.stats()
                for name, breaker in self._breakers.items()
            }

    def summary(self):
        with self._lock:
            result = {
                'total_breakers': len(self._breakers),
                'open_breakers': [],
                'half_open_breakers': [],
                'closed_breakers': [],
            }
            for name, breaker in self._breakers.items():
                st = breaker.state
                if st == STATE_OPEN:
                    result['open_breakers'].append(name)
                elif st == STATE_HALF_OPEN:
                    result['half_open_breakers'].append(name)
                else:
                    result['closed_breakers'].append(name)
            return result

    def reset(self, task_name):
        with self._lock:
            breaker = self._breakers.get(task_name)
            if breaker is not None:
                breaker.reset()
                return True
            return False

    def reset_all(self):
        with self._lock:
            count = 0
            for breaker in self._breakers.values():
                breaker.reset()
                count += 1
            return count

    def clear(self):
        with self._lock:
            self._breakers.clear()
