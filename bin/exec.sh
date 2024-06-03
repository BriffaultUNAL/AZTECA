#!/bin/bash

ACT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd)"

echo $ACT_DIR

LOG_FILE="$ACT_DIR/log/logs_main.log"
exec > >(tee -a "$LOG_FILE") 2>&1

start_time=$(date +%s)

echo -e "Inicio de ejecucion: $(date)\n"

if ! mountpoint -q "/home/usr-dwh/Escritorio/Compartida_Z";
then
    
    echo "Mounting folder"
    sudo mount -t cifs //172.10.7.5/fs-cos\$ /home/usr-dwh/Escritorio/Compartida_Z -o username=cesar.almeciga,password=Mayo2024*

else
    echo "Folder mounted succesfully"
fi

VENV="$ACT_DIR/venv"
source "$VENV/bin/activate"
python3 "$ACT_DIR/main.py"
deactivate

echo "Process executed"

#sudo umount -f "$ACT_DIR/shared"
echo -e "Folder unmonted \n"

end_time=$(date +%s)
runtime=$((end_time-start_time))
formatted_runtime=$(date -u -d @"$runtime" +'%M minutos y %S segundos')
echo -e "Fin de ejecucion: $(date)\n"
echo -e "Tiempo de ejecucion: $formatted_runtime \n \n \n"