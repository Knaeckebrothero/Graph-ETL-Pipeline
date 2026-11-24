#!/usr/bin/env bash
# See deploy/pilot/README.md. Arguments go to: pilot.py reset
exec python3 "$(dirname "$0")/pilot.py" reset "$@"
