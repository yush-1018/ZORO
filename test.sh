#!/bin/bash
set -uo pipefail

MODE=""
OUTPUT_PATH=""
STATUS=0

while [[ $# -gt 0 ]]; do
    case $1 in
        --output_path)
            OUTPUT_PATH="$2"
            shift 2
            ;;
        base|new)
            MODE="$1"
            shift
            ;;
        *)
            echo "Usage: $0 [--output_path <path>] <base|new>" >&2
            echo "  base  - Run existing test suite (regression check)" >&2
            echo "  new   - Run newly added tests" >&2
            exit 1
            ;;
    esac
done

if [ -z "$MODE" ]; then
    echo "Error: mode required (base or new)" >&2
    echo "Usage: $0 [--output_path <path>] <base|new>" >&2
    exit 1
fi

run_tests_with_xml() {
    local mode="$1"
    local output_path="$2"
    if [ "$mode" = "base" ]; then
        echo "Running regression tests..."
        python -m pytest t/unit/tasks/test_trace.py t/unit/tasks/test_tasks.py --junitxml="$output_path" -v
    elif [ "$mode" = "new" ]; then
        echo "Running new challenge tests..."
        python -m pytest t/unit/tasks/test_astraxx_circuitbreaker_230e92.py --junitxml="$output_path" -v
    fi
}

run_tests_plain() {
    local mode="$1"
    if [ "$mode" = "base" ]; then
        echo "Running regression tests..."
        python -m pytest t/unit/tasks/test_trace.py t/unit/tasks/test_tasks.py -v
    elif [ "$mode" = "new" ]; then
        echo "Running new challenge tests..."
        python -m pytest t/unit/tasks/test_astraxx_circuitbreaker_230e92.py -v
    fi
}

if [ -n "$OUTPUT_PATH" ]; then
    run_tests_with_xml "$MODE" "$OUTPUT_PATH" || STATUS=$?
else
    run_tests_plain "$MODE" || STATUS=$?
fi

exit "$STATUS"
