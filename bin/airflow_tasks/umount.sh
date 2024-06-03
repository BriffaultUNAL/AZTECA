#!/bin/bash

ACT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd ../.. && pwd)"

echo $ACT_DIR

LOG_FILE="$ACT_DIR/log/logs_main.log"
exec > >(tee -a "$LOG_FILE") 2>&1

sudo umount -f "$ACT_DIR/shared"
echo -e "Folder unmonted \n"

echo -e "Fin de ejecucion: $(date)\n \n \n"