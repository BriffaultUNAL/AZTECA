#!/usr/bin/python

import sys
import os
import logging
from src.utils import file_path
from src.utils import transform, load, read_file, dir_path, modification_datetime


def init():
    
    if read_file() == str(file := os.listdir(dir_path)[0]):
        
        logging.info(f'Archivo {file} sin modificaciones')
        print(f'Archivo {file} sin modificaciones')
    
    else:

        logging.info(file)
        logging.info(file_path)
        logging.info(modification_datetime)
        load(transform())


if __name__ == "__main__":

    init()
    