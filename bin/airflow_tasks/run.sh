#!/bin/bash

ACT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd ../.. && pwd)"

LOG_FILE="$ACT_DIR/log/logs_main.log"
exec > >(tee -a "$LOG_FILE") 2>&1

VENV="$ACT_DIR/venv"
source "$VENV/bin/activate"
python3 "$ACT_DIR/main.py"
deactivate

echo "Process executed"