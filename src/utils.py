#!/usr/bin/python

import os
import sys
import logging
from urllib.parse import quote
import sqlalchemy as sa
from sqlalchemy import Engine, Connection
import yaml
import pandas as pd
from pandas import DataFrame
from src.telegram_bot import enviar_mensaje
import time
import re
from datetime import datetime
import asyncio

act_dir = os.path.dirname(os.path.abspath(__file__))
proyect_dir = os.path.join(act_dir, '..')
sys.path.append(proyect_dir)

yml_credentials_dir = os.path.join(proyect_dir, 'config', 'credentials.yml')
yml_query_dir = os.path.join(proyect_dir, 'config', 'querys.yml')
dir_path = os.path.join('/home/usr-dwh/Escritorio/Compartida_Z/',
                        'OPERACIONES/AZTECA/Falla Masiva')
last_exec = os.path.join(proyect_dir, 'last_exec.txt')
control = os.path.join(proyect_dir, 'control.txt')

modification_time = os.path.getmtime(file_path := os.path.join(
    '/home/usr-dwh/Escritorio/Compartida_Z/', 'OPERACIONES/AZTECA/Falla Masiva', str(os.listdir(dir_path)[0])))

modification_datetime = time.ctime(modification_time)


logging.basicConfig(
    level=logging.INFO,
    filename=(os.path.join(proyect_dir, 'log', 'logs_main.log')),
    format="%(asctime)s - %(levelname)s -  %(message)s",
    datefmt='%d-%b-%y %H:%M:%S'
)


def read_file():

    read = open(control, 'r')
    return read.read()


def write_file(texto: str, file: str, mode: str):

    write = open(file, mode)
    write.write(f'{texto}')


def get_engine(username: str, password: str, host: str, database: str, port: str = 3306, *_) -> Engine:
    return sa.create_engine(f"mysql+pymysql://{username}:{quote(password)}@{host}:{port}/{database}?charset=utf8mb4")


with open(yml_credentials_dir, 'r') as f:

    try:
        config = yaml.safe_load(f)
        source1 = config['source1']
    except yaml.YAMLError as e:
        logging.error(str(e), exc_info=True)


def engine_1() -> Connection:
    return get_engine(**source1).connect()


def filter_characters(texto) -> str:
    return re.sub(r'[^a-zA-Z0-9\s]', '', texto)


def extract(file_path_, columns) -> DataFrame:

    df = pd.read_csv(file_path_, sep=';', names=columns,
                     low_memory=False, dtype=str, encoding='latin1')
    return df


def transform() -> DataFrame:

    columns = ["Departamento", "Municipio", "Documento", "Cliente", "SegmentoFinal",
               "TipoCliente", "DanoMasivo", "Flag", "Final", "MarcaRegulado"]

    df_vicidial = extract(file_path, columns)

    df_vicidial = df_vicidial[["Departamento", "Municipio", "Documento", "Cliente",
                               "SegmentoFinal", "TipoCliente", "DanoMasivo", "Flag", "Final"]]

    df_vicidial['Departamento'] = df_vicidial['Departamento'].str.replace(
        "[-()\"#/@;:<>{}`+=~|.!?,]", "")

    df_vicidial['Departamento'] = df_vicidial['Departamento'].str.upper()

    df_vicidial['Departamento'] = df_vicidial['Departamento'].str.replace(
        "[Ñ]", "N")

    df_vicidial['Departamento'] = df_vicidial['Departamento'].str.replace(
        "[É]", "E")

    df_vicidial['Departamento'] = df_vicidial['Departamento'].str.replace(
        "[Í]", "I")

    df_vicidial['Departamento'] = df_vicidial['Departamento'].str.replace(
        "[Ó]", "0")

    df_vicidial['Departamento'] = df_vicidial['Departamento'].str.replace(
        "[Ú]", "U")

    df_vicidial['Cliente'] = df_vicidial['Cliente'].apply(lambda x: re.sub(
        r'[^a-zA-Z0-9\s]', '', str(x)) if x != None else None)

    df_vicidial['Documento'] = df_vicidial['Documento'].str.replace(
        "[-()\"#/@;:<>{}`+=~|.!?,]", "")

    df_vicidial['DanoMasivo'] = df_vicidial['DanoMasivo'].str.replace(
        "[-()\"#/@;:<>{}`+=~|.!?,]", "")

    df_vicidial['DanoMasivo'] = df_vicidial['DanoMasivo'].str.upper()

    df_vicidial["Fecha_Cargue"] = pd.to_datetime(
        datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    return df_vicidial


def load(df_vicidial):

    with engine_1() as con:

        try:

            df_vicidial.to_sql('custom_20003', con=con,
                               if_exists='replace', index=False)
            time_now = datetime.now().strftime('%Y-%m-%d %H:%M')
            logging.info(f'Hora Actual {time_now}')
            print(f'Hora Actual {time_now}')
            logging.info(f'Archivo con {(lendf := len(df_vicidial))} datos')
            print(f'Archivo con {lendf} datos')
            count = pd.read_sql_query(
                'SELECT count(*) FROM AZTECA.custom_20003;', con)['count(*)'][0]
            load_date = str(pd.read_sql_query(
                            'SELECT Fecha_Cargue FROM AZTECA.custom_20003 ORDER BY Fecha_Cargue DESC LIMIT 1;',
                            con)['Fecha_Cargue'][0])[:-3]
            logging.info(f"Datos cargados en la tabla {int(count)}")
            print(f"Datos cargados en la tabla {int(count)}")
            logging.info(f"Last load date {str(load_date)}")
            print(f"Last load date {str(load_date)}")

            if int(count) == lendf and str(time_now) == str(load_date):

                write_file(os.listdir(dir_path)[0], control, 'w')

            else:
                pass

            asyncio.run(enviar_mensaje("AZTECA PROCESS"))
            write_file(
                (f"Archivo {os.listdir(dir_path)[0]} \n"), last_exec, "w")
            asyncio.run(enviar_mensaje(str(os.listdir(dir_path)[0])))
            write_file((f"Archivo con {lendf } datos \n"), last_exec, "a")
            asyncio.run(enviar_mensaje(str(f"Archivo con {lendf } datos")))
            write_file(
                (f"Datos cargados en la tabla {int(count)} \n"), last_exec, "a")
            asyncio.run(enviar_mensaje(
                str(f"Datos cargados en la tabla {int(count)}")))
            write_file((f"Last load date {str(load_date)} \n"), last_exec, "a")
            asyncio.run(enviar_mensaje(
                str(f"Last load date {str(load_date)} \n")))
            asyncio.run(enviar_mensaje("____________________________________"))

        except KeyError as e:

            logging.error(str(e), exc_info=True)
