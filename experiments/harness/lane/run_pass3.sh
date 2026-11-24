#!/usr/bin/env bash
# Third part of the pilot (KICKOFF-3.md): the twelve pilot units on pilot-s1-rdf alone, three runs one after
# another. After each run the graphs are exported; after runs 1 and 2 they are emptied (reset.sh files the
# records under the run id); after run 3 the graph stays and its records are filed as a copy.
set -euo pipefail
LANE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$LANE/../../.." && pwd)"
RUN="$REPO/deploy/pilot/run"
for i in 1 2 3; do
  label="pass3-s1rdf-run$i"
  python3 "$LANE/drive.py" --label "$label" --cells pilot-s1-rdf --units pilot
  rid="$(ls "$RUN/lane" | grep "pilot-$label-" | tail -1)"
  (cd "$LANE" && python3 summarise.py "$rid" > /dev/null && python3 inspect_pass.py "$rid" > /dev/null && python3 inspect_pass2.py "$rid" > /dev/null)
  python3 "$RUN/graphs/export_graphs.py" "$RUN/graphs/$rid" > /dev/null
  echo "run $i: $rid exported"
  if [ "$i" -lt 3 ]; then
    "$REPO/deploy/pilot/reset.sh" "$rid" > /dev/null
    echo "run $i: graphs emptied, records filed"
  else
    mkdir -p "$RUN/records/$rid" && cp "$RUN"/records/*.jsonl "$RUN/records/$rid/"
    echo "run $i: graph kept, records copied"
  fi
done
echo "all three runs done"
