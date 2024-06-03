#!/bin/bash

ACT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd ../.. && pwd)"

echo $ACT_DIR

LOG_FILE="$ACT_DIR/log/logs_main.log"
exec > >(tee -a "$LOG_FILE") 2>&1

echo -e "Inicio de ejecucion: $(date)\n"

echo "Mounting folder"
sudo mount -t cifs //172.10.7.5/fs-cos$/ "$ACT_DIR/shared" -o username=cesar.almeciga,password=Marzo2024*,uid=$(id -u),gid=$(id -g)
echo "Folder mounted succesfully"
