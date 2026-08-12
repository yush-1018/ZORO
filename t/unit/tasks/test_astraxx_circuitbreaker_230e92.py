from unittest.mock import Mock

import pytest

from celery import signals
from celery.exceptions import CircuitBreakerError
from celery.utils.circuitbreaker import (
    STATE_CLOSED,
    STATE_HALF_OPEN,
    STATE_OPEN,
    CircuitBreaker,
    CircuitBreakerRegistry,
)


class test_astraxx_CircuitBreaker_230e92:

    def test_astraxx_initial_state_is_closed(self):
        cb = CircuitBreaker(task_name='t.add', threshold=3)
        assert cb.state == STATE_CLOSED
        assert cb.can_execute() is True

    def test_astraxx_stays_closed_below_threshold(self):
        clock = Mock(return_value=100.0)
        cb = CircuitBreaker(task_name='t.add', threshold=3, clock=clock)
        cb.record_failure()
        cb.record_failure()
        assert cb.state == STATE_CLOSED
        assert cb.can_execute() is True

    def test_astraxx_opens_at_threshold(self):
        clock = Mock(return_value=100.0)
        cb = CircuitBreaker(task_name='t.add', threshold=2, clock=clock)
        cb.record_failure()
        cb.record_failure()
        assert cb.state == STATE_OPEN
        assert cb.can_execute() is False

    def test_astraxx_open_sends_signal(self):
        clock = Mock(return_value=100.0)
        received = []

        def handler(sender, task_name, failure_count, **kw):
            received.append((task_name, failure_count))

        signals.circuit_breaker_opened.connect(handler)
        try:
            cb = CircuitBreaker(task_name='t.add', threshold=1, clock=clock)
            cb.record_failure()
            assert len(received) == 1
            assert received[0] == ('t.add', 1)
        finally:
            signals.circuit_breaker_opened.disconnect(handler)

    def test_astraxx_transitions_to_half_open_after_timeout(self):
        time_ref = [100.0]
        cb = CircuitBreaker(
            task_name='t.add', threshold=1,
            recovery_timeout=10.0, clock=lambda: time_ref[0],
        )
        cb.record_failure()
        assert cb.state == STATE_OPEN

        time_ref[0] = 105.0
        assert cb.state == STATE_OPEN
        assert cb.can_execute() is False

        time_ref[0] = 111.0
        assert cb.can_execute() is True
        assert cb.state == STATE_HALF_OPEN

    def test_astraxx_half_open_sends_signal(self):
        time_ref = [100.0]
        received = []

        def handler(sender, task_name, **kw):
            received.append(task_name)

        signals.circuit_breaker_half_opened.connect(handler)
        try:
            cb = CircuitBreaker(
                task_name='t.add', threshold=1,
                recovery_timeout=10.0, clock=lambda: time_ref[0],
            )
            cb.record_failure()
            time_ref[0] = 111.0
            cb.can_execute()
            assert len(received) == 1
        finally:
            signals.circuit_breaker_half_opened.disconnect(handler)

    def test_astraxx_half_open_success_closes(self):
        time_ref = [100.0]
        cb = CircuitBreaker(
            task_name='t.add', threshold=1,
            recovery_timeout=10.0, half_open_max_calls=1,
            clock=lambda: time_ref[0],
        )
        cb.record_failure()
        time_ref[0] = 111.0
        assert cb.can_execute() is True
        cb.record_success()
        assert cb.state == STATE_CLOSED

    def test_astraxx_closed_sends_signal(self):
        time_ref = [100.0]
        received = []

        def handler(sender, task_name, **kw):
            received.append(task_name)

        signals.circuit_breaker_closed.connect(handler)
        try:
            cb = CircuitBreaker(
                task_name='t.add', threshold=1,
                recovery_timeout=10.0, half_open_max_calls=1,
                clock=lambda: time_ref[0],
            )
            cb.record_failure()
            time_ref[0] = 111.0
            cb.can_execute()
            cb.record_success()
            assert len(received) == 1
        finally:
            signals.circuit_breaker_closed.disconnect(handler)

    def test_astraxx_half_open_failure_reopens(self):
        time_ref = [100.0]
        cb = CircuitBreaker(
            task_name='t.add', threshold=1,
            recovery_timeout=10.0, clock=lambda: time_ref[0],
        )
        cb.record_failure()
        time_ref[0] = 111.0
        assert cb.can_execute() is True
        cb.record_failure()
        assert cb.state == STATE_OPEN

    def test_astraxx_half_open_limits_probe_calls(self):
        time_ref = [100.0]
        cb = CircuitBreaker(
            task_name='t.add', threshold=1,
            recovery_timeout=10.0, half_open_max_calls=2,
            clock=lambda: time_ref[0],
        )
        cb.record_failure()
        time_ref[0] = 111.0
        assert cb.can_execute() is True
        assert cb.can_execute() is True
        assert cb.can_execute() is False

    def test_astraxx_failure_window_purges_old(self):
        time_ref = [100.0]
        cb = CircuitBreaker(
            task_name='t.add', threshold=2,
            failure_window=10.0, clock=lambda: time_ref[0],
        )
        cb.record_failure()
        time_ref[0] = 115.0
        cb.record_failure()
        assert cb.state == STATE_CLOSED

    def test_astraxx_reset_returns_to_closed(self):
        cb = CircuitBreaker(task_name='t.add', threshold=1)
        cb.record_failure()
        assert cb.state == STATE_OPEN
        cb.reset()
        assert cb.state == STATE_CLOSED
        assert cb.can_execute() is True

    def test_astraxx_stats_returns_correct_dict(self):
        clock = Mock(return_value=100.0)
        cb = CircuitBreaker(
            task_name='t.add', threshold=5,
            recovery_timeout=30.0, failure_window=60.0,
            half_open_max_calls=1, clock=clock,
        )
        s = cb.stats()
        assert s['state'] == 'closed'
        assert s['failure_count'] == 0
        assert s['threshold'] == 5
        assert s['recovery_timeout'] == 30.0
        assert s['failure_window'] == 60.0
        assert s['half_open_max_calls'] == 1
        assert s['opened_at'] is None


class test_astraxx_CircuitBreakerRegistry_230e92:

    def test_astraxx_get_or_create(self):
        reg = CircuitBreakerRegistry()
        cb = reg.get_or_create('task1', threshold=3)
        assert cb is reg.get_or_create('task1')
        assert cb is reg.get('task1')

    def test_astraxx_get_missing_returns_none(self):
        reg = CircuitBreakerRegistry()
        assert reg.get('missing') is None

    def test_astraxx_all_stats(self):
        reg = CircuitBreakerRegistry()
        reg.get_or_create('t1')
        reg.get_or_create('t2')
        stats = reg.all_stats()
        assert 't1' in stats
        assert 't2' in stats

    def test_astraxx_reset_existing(self):
        reg = CircuitBreakerRegistry()
        cb = reg.get_or_create('t1', threshold=1)
        cb.record_failure()
        assert cb.state == STATE_OPEN
        assert reg.reset('t1') is True
        assert cb.state == STATE_CLOSED

    def test_astraxx_reset_missing(self):
        reg = CircuitBreakerRegistry()
        assert reg.reset('missing') is False

    def test_astraxx_clear(self):
        reg = CircuitBreakerRegistry()
        reg.get_or_create('t1')
        reg.clear()
        assert reg.get('t1') is None


class test_astraxx_CircuitBreakerError_230e92:

    def test_astraxx_str_representation(self):
        exc = CircuitBreakerError('t.add')
        assert str(exc) == 'Circuit breaker open for task t.add'

    def test_astraxx_task_name_attribute(self):
        exc = CircuitBreakerError('t.add')
        assert exc.task_name == 't.add'

    def test_astraxx_reduce(self):
        exc = CircuitBreakerError('t.add')
        cls, args = exc.__reduce__()
        assert cls is CircuitBreakerError
        assert args == ('t.add',)


class test_astraxx_circuit_breaker_integration_230e92:

    def test_astraxx_tracer_opens_circuit_after_failures(self, celery_app):
        @celery_app.task(
            name='cb_fail_task',
            circuit_breaker=True,
            circuit_breaker_threshold=2,
            circuit_breaker_recovery_timeout=60.0,
        )
        def failing_task():
            raise ValueError('boom')

        with pytest.raises(ValueError):
            failing_task.apply(throw=True)
        with pytest.raises(ValueError):
            failing_task.apply(throw=True)

        registry = getattr(celery_app, '_circuit_breaker_registry', None)
        assert registry is not None
        cb = registry.get('cb_fail_task')
        assert cb is not None
        assert cb.state == STATE_OPEN

    def test_astraxx_tracer_uses_all_global_app_defaults(self, celery_app):
        celery_app.conf.task_circuit_breaker_threshold = 2
        celery_app.conf.task_circuit_breaker_recovery_timeout = 45.0
        celery_app.conf.task_circuit_breaker_half_open_max_calls = 2
        celery_app.conf.task_circuit_breaker_failure_window = 90.0
        celery_app.conf.task_circuit_breaker_exclude = (TypeError,)

        @celery_app.task(
            name='cb_all_global_defaults_task',
            circuit_breaker=True,
        )
        def failing_task():
            raise ValueError('boom')

        with pytest.raises(ValueError):
            failing_task.apply(throw=True)
        with pytest.raises(ValueError):
            failing_task.apply(throw=True)

        registry = getattr(celery_app, '_circuit_breaker_registry', None)
        assert registry is not None
        cb = registry.get('cb_all_global_defaults_task')
        assert cb is not None
        assert cb.state == STATE_OPEN
        stats = cb.stats()
        assert stats['threshold'] == 2
        assert stats['recovery_timeout'] == 45.0
        assert stats['half_open_max_calls'] == 2
        assert stats['failure_window'] == 90.0

    def test_astraxx_tracer_rejects_when_open(self, celery_app):
        @celery_app.task(
            name='cb_reject_task',
            circuit_breaker=True,
            circuit_breaker_threshold=1,
        )
        def failing():
            raise ValueError('boom')

        with pytest.raises(ValueError):
            failing.apply(throw=True)

        res = failing.apply()
        assert isinstance(res.result, CircuitBreakerError)

    def test_astraxx_tracer_records_success(self, celery_app):
        @celery_app.task(
            name='cb_success_task',
            circuit_breaker=True,
            circuit_breaker_threshold=5,
        )
        def ok_task():
            return 42

        res = ok_task.apply()
        assert res.result == 42

    def test_astraxx_control_stats(self, celery_app):
        from celery.worker.control import circuit_breaker_stats

        @celery_app.task(
            name='cb_ctrl_task',
            circuit_breaker=True,
            circuit_breaker_threshold=1,
        )
        def fail():
            raise RuntimeError('fail')

        with pytest.raises(RuntimeError):
            fail.apply(throw=True)

        state_mock = Mock()
        state_mock.app = celery_app
        stats = circuit_breaker_stats(state_mock)
        assert 'cb_ctrl_task' in stats
        assert stats['cb_ctrl_task']['state'] == STATE_OPEN

    def test_astraxx_control_reset(self, celery_app):
        from celery.worker.control import circuit_breaker_reset

        @celery_app.task(
            name='cb_reset_task',
            circuit_breaker=True,
            circuit_breaker_threshold=1,
        )
        def fail():
            raise RuntimeError('fail')

        with pytest.raises(RuntimeError):
            fail.apply(throw=True)

        state_mock = Mock()
        state_mock.app = celery_app
        result = circuit_breaker_reset(state_mock, task_name='cb_reset_task')
        assert result == {'ok': 'circuit breaker reset for cb_reset_task'}

    def test_astraxx_control_reset_missing(self, celery_app):
        from celery.worker.control import circuit_breaker_reset
        state_mock = Mock()
        state_mock.app = celery_app
        result = circuit_breaker_reset(state_mock, task_name='nonexistent')
        assert result == {'error': 'no circuit breaker for nonexistent'}

    def test_astraxx_no_circuit_breaker_by_default(self, celery_app):
        @celery_app.task(name='cb_normal_task')
        def normal():
            raise ValueError('boom')

        with pytest.raises(ValueError):
            normal.apply(throw=True)
        with pytest.raises(ValueError):
            normal.apply(throw=True)

        assert not hasattr(celery_app, '_circuit_breaker_registry') or \
            celery_app._circuit_breaker_registry.get('cb_normal_task') is None

    def test_astraxx_excluded_exceptions(self, celery_app):
        @celery_app.task(
            name='cb_excluded_task',
            circuit_breaker=True,
            circuit_breaker_threshold=1,
            circuit_breaker_exclude=(KeyError,),
        )
        def task_with_keyerror():
            raise KeyError('missing key')

        with pytest.raises(KeyError):
            task_with_keyerror.apply(throw=True)

        registry = getattr(celery_app, '_circuit_breaker_registry', None)
        assert registry is not None
        cb = registry.get('cb_excluded_task')
        assert cb is not None
        assert cb.state == STATE_CLOSED

    def test_astraxx_validation_errors(self):
        with pytest.raises(ValueError):
            CircuitBreaker('')
        with pytest.raises(ValueError):
            CircuitBreaker('t', threshold=0)
        with pytest.raises(ValueError):
            CircuitBreaker('t', recovery_timeout=-1.0)
        with pytest.raises(ValueError):
            CircuitBreaker('t', half_open_max_calls=0)
        with pytest.raises(ValueError):
            CircuitBreaker('t', failure_window=0)
