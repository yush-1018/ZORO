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

    def test_astraxx_opens_at_threshold(self):
        clock = Mock(return_value=100.0)
        received = []

        def handler(sender, task_name, failure_count, **kw):
            received.append((task_name, failure_count))

        signals.circuit_breaker_opened.connect(handler)
        try:
            cb = CircuitBreaker(task_name='t.add', threshold=2, clock=clock)
            cb.record_failure()
            cb.record_failure()
            assert cb.state == STATE_OPEN
            assert cb.can_execute() is False
            assert len(received) == 1
            assert received[0] == ('t.add', 2)
        finally:
            signals.circuit_breaker_opened.disconnect(handler)

    def test_astraxx_half_open_and_closed_signals_payload(self):
        time_ref = [100.0]
        half_open_received = []
        closed_received = []

        def ho_handler(sender, task_name, **kw):
            half_open_received.append(task_name)

        def cl_handler(sender, task_name, **kw):
            closed_received.append(task_name)

        signals.circuit_breaker_half_opened.connect(ho_handler)
        signals.circuit_breaker_closed.connect(cl_handler)
        try:
            cb = CircuitBreaker(
                task_name='t.add', threshold=1,
                recovery_timeout=10.0, half_open_max_calls=1,
                clock=lambda: time_ref[0],
            )
            cb.record_failure()
            time_ref[0] = 111.0
            assert cb.can_execute() is True
            assert cb.state == STATE_HALF_OPEN
            assert half_open_received == ['t.add']
            cb.record_success()
            assert cb.state == STATE_CLOSED
            assert closed_received == ['t.add']
        finally:
            signals.circuit_breaker_half_opened.disconnect(ho_handler)
            signals.circuit_breaker_closed.disconnect(cl_handler)

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

    def test_astraxx_stats_schema(self):
        clock = Mock(return_value=100.0)
        cb = CircuitBreaker(task_name='t.add', threshold=5, clock=clock)
        s = cb.stats()
        assert s['state'] == STATE_CLOSED
        assert s['failure_count'] == 0
        assert s['recovery_timeout'] == 60.0

    def test_astraxx_single_success_closes_half_open_even_if_max_calls_higher(self):
        time_ref = [100.0]
        cb = CircuitBreaker(
            task_name='t.add', threshold=1, recovery_timeout=10.0,
            half_open_max_calls=5, clock=lambda: time_ref[0],
        )
        cb.record_failure()
        assert cb.state == STATE_OPEN
        time_ref[0] = 115.0
        assert cb.can_execute() is True
        assert cb.state == STATE_HALF_OPEN
        cb.record_success()
        assert cb.state == STATE_CLOSED


class test_astraxx_CircuitBreakerRegistry_230e92:

    def test_astraxx_registry_ops(self):
        reg = CircuitBreakerRegistry()
        cb = reg.get_or_create('t1', threshold=1)
        assert reg.get('t1') is cb
        assert 't1' in reg.all_stats()
        cb.record_failure()
        assert reg.reset('t1') is True
        assert reg.reset('missing') is False
        reg.clear()
        assert reg.get('t1') is None


class test_astraxx_CircuitBreakerError_230e92:

    def test_astraxx_error_attributes_and_reduce(self):
        exc = CircuitBreakerError('t.add')
        assert str(exc) == 'Circuit breaker open for task t.add'
        assert exc.task_name == 't.add'
        cls, args = exc.__reduce__()
        assert cls is CircuitBreakerError
        assert args == ('t.add',)


class test_astraxx_circuit_breaker_integration_230e92:

    def test_astraxx_tracer_integration_and_reject(self, celery_app):
        @celery_app.task(
            name='cb_fail_task',
            circuit_breaker=True,
            circuit_breaker_threshold=1,
        )
        def failing_task():
            raise ValueError('boom')

        with pytest.raises(ValueError):
            failing_task.apply(throw=True)

        res = failing_task.apply()
        assert isinstance(res.result, CircuitBreakerError)

    def test_astraxx_tracer_uses_global_exclude_fallback(self, celery_app):
        celery_app.conf.task_circuit_breaker_exclude = (KeyError,)
        @celery_app.task(
            name='cb_global_exclude_task',
            circuit_breaker=True,
            circuit_breaker_threshold=1,
        )
        def key_error_task():
            raise KeyError('missing')

        with pytest.raises(KeyError):
            key_error_task.apply(throw=True)

        registry = getattr(celery_app, '_circuit_breaker_registry', None)
        assert registry is not None
        cb = registry.get('cb_global_exclude_task')
        assert cb is not None
        assert cb.state == STATE_CLOSED

    def test_astraxx_control_stats_and_reset(self, celery_app):
        from celery.worker.control import circuit_breaker_reset, circuit_breaker_stats

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

        res_ok = circuit_breaker_reset(state_mock, task_name='cb_ctrl_task')
        assert res_ok == {'ok': 'circuit breaker reset for cb_ctrl_task'}

        res_nok = circuit_breaker_reset(state_mock, task_name='nonexistent')
        assert res_nok == {'error': 'no circuit breaker for nonexistent'}


class test_astraxx_CircuitBreakerDirectSignals_230e92:

    def test_astraxx_direct_signals_payloads_and_idempotency(self):
        time_ref = [100.0]
        opened_events = []
        closed_events = []
        half_opened_events = []

        def on_opened(sender, task_name, failure_count, **kw):
            opened_events.append((sender, task_name, failure_count))

        def on_closed(sender, task_name, **kw):
            closed_events.append((sender, task_name))

        def on_half_opened(sender, task_name, **kw):
            half_opened_events.append((sender, task_name))

        signals.circuit_breaker_opened.connect(on_opened)
        signals.circuit_breaker_closed.connect(on_closed)
        signals.circuit_breaker_half_opened.connect(on_half_opened)

        try:
            cb = CircuitBreaker(
                task_name='t.direct', threshold=2, recovery_timeout=10.0,
                half_open_max_calls=1, clock=lambda: time_ref[0],
            )

            # Initial reset while closed should NOT fire closed signal
            cb.reset()
            assert len(closed_events) == 0

            # 1st failure - below threshold, no signal
            cb.record_failure()
            assert len(opened_events) == 0

            # 2nd failure - hits threshold, transitions CLOSED -> OPEN
            cb.record_failure()
            assert len(opened_events) == 1
            assert opened_events[0] == (None, 't.direct', 2)

            # Subsequent failures while OPEN should NOT re-emit opened signal
            cb.record_failure()
            assert len(opened_events) == 1

            # Advance time past recovery_timeout
            time_ref[0] = 115.0

            # Multiple calls to can_execute/state in HALF_OPEN should fire half_opened signal EXACTLY ONCE
            assert cb.can_execute() is True
            assert cb.state == STATE_HALF_OPEN
            assert cb.can_execute() is False
            assert len(half_opened_events) == 1
            assert half_opened_events[0] == (None, 't.direct')

            # Record success in HALF_OPEN -> transitions HALF_OPEN -> CLOSED
            cb.record_success()
            assert cb.state == STATE_CLOSED
            assert len(closed_events) == 1
            assert closed_events[0] == (None, 't.direct')

            # Subsequent record_success/reset in CLOSED should NOT re-emit closed signal
            cb.record_success()
            cb.reset()
            assert len(closed_events) == 1

            # Trip to OPEN again
            time_ref[0] = 120.0
            cb.record_failure()
            cb.record_failure()
            assert len(opened_events) == 2

            # Reset from OPEN -> CLOSED should fire closed signal ONCE
            cb.reset()
            assert cb.state == STATE_CLOSED
            assert len(closed_events) == 2
            assert closed_events[1] == (None, 't.direct')

        finally:
            signals.circuit_breaker_opened.disconnect(on_opened)
            signals.circuit_breaker_closed.disconnect(on_closed)
            signals.circuit_breaker_half_opened.disconnect(on_half_opened)

