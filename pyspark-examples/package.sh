#!/usr/bin/env bash
# Package the `tools` module as tools.zip, next to spark_submit.py.
set -euo pipefail
cd "$(dirname "$0")"

rm -f tools.zip
python3 - <<'PY'
import pathlib, zipfile

with zipfile.ZipFile("tools.zip", "w", zipfile.ZIP_DEFLATED) as archive:
    for path in sorted(pathlib.Path("tools").rglob("*.py")):
        archive.write(path)
PY
echo "Created tools.zip"
