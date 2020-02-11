"""
Upload Stata files (.dta) to a SQL database using pandas 
"""


import os
import argparse
import pandas as pd
import ohio.ext.pandas
from pathlib import Path
from sqlalchemy import create_engine


def create_engine(creds_file):
    '''
    Create SQL connection object using a psycopg2 cursor and abiding to new
    dssg/dsapp db user configuration.
    Arguments:
        - credentials_yaml: .yaml file with db credentials
    '''
    with open(credentials_yaml) as f:
        configs = yaml.load(f)
    try:
        conn = create_engine("postgresql://{user}:{pass}@{host}:{port}/{db}".format(**configs))
    except:
        print("Error connecting to db.")

    return conn


def read_n_upload(path,
                 creds):

    engine = create_engine(creds)

    print(f'Loading file: {path}')

    df_dta = pd.read_stata(path)
    df_dta.pg_copy_to('energy_generation_data',
                      schema='energy_data',
                      index=False,
                      con=engine)


if name == '__main__':

    parser = argparse.ArgumentParser(description='Upload Stata files to a SQL database',
                                    formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-p', '--path_stata_files',
                       required=True,
                       action='store',
                       help='Path where all stata files are stored. If only one file, just the path')
    parser.add_path('-c', '--credential_file',
                   required=True,
                   action='store',
                   help='Path to credentials YAML file')

    args = parser.parse_args()
    paths_stata_files = Path(args.path_stata_files).rglob('*.dta')

    for file_dta in paths_stata_files:
        read_n_upload(file_data, args.credential_file)


