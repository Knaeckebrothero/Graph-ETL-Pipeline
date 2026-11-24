#!/usr/bin/env bash
# See deploy/pilot/README.md. Arguments go to: pilot.py start
exec python3 "$(dirname "$0")/pilot.py" start "$@"
