"""
Define functions to get geography codes for individual country, region, area.
The code file is imported from the folder "./metadata"
"""
from xlrd import open_workbook
import sqlite3 as pyo
import pandas as pd
import datetime
import time
from BSO.time_analysis import time_decorator

geo_file = 'metadata/geography.xlsx'
sitc2hs_file = 'metadata/sitc2hs.xlsx'
hs_code_file = 'metadata/ACON014_HSITEM.xls'
sitc_code_file = 'metadata/ACON014_SITC.xls'
industry_classification_file = 'metadata/industry.xlsx'

db_path = "merchandise_trades_DB"

@time_decorator
def import_geography_code(bk=geo_file):
    db = pyo.connect(db_path+"/"+"trades.db")
    dfs = pd.read_excel(bk, sheet_name=None)

    for table, df in dfs.items():
        try:
            df['UPLOAD_TO_DB_DATE'] = datetime.datetime.now()
            df.to_sql(table, db, index=False)
        except ValueError:
            print(f"{table} already exists\n")
        else:
            print(f"Successfully imported {table} into DB for mapping uses\n")

@time_decorator
def import_hs_code(bk=hs_code_file):
    db = pyo.connect(db_path+"/"+"trades.db")
    dfs = pd.read_excel(bk, sheet_name=None,
                        dtype=str)
    for table, df in dfs.items():
        try:
            df['UPLOAD_TO_DB_DATE'] = datetime.datetime.now()
            df.to_sql(table, db, index=False)
        except ValueError:
            print(f"{table} already exists\n")
        else:
            print(f"Successfully imported {table} into DB for mapping uses\n")

@time_decorator
def import_sitc_code(bk=sitc_code_file):
    db = pyo.connect(db_path+"/"+"trades.db")
    dfs = pd.read_excel(bk, sheet_name=None,
                        dtype=str)
    for table, df in dfs.items():
        try:
            df['UPLOAD_TO_DB_DATE'] = datetime.datetime.now()
            # right trim the whitespaces
            df['SITC_Code']=df['SITC_Code'].str.rstrip()
            df.to_sql(table, db, index=False)
        except ValueError:
            print(f"{table} already exists\n")
        else:
            print(f"Successfully imported {table} into DB for mapping uses\n")

@time_decorator
def import_sitctohs_code(bk=sitc2hs_file):
    db = pyo.connect(db_path+"/"+"trades.db")
    dfs = pd.read_excel(bk, sheet_name=None,
                        dtype={"SITC5":str,"HS8":str})
    for table, df in dfs.items():
        try:
            df['UPLOAD_TO_DB_DATE'] = datetime.datetime.now()
            df.to_sql(table, db, index=False)
        except ValueError:
            print(f"{table} already exists\n")
        else:
            print(f"Successfully imported {table} into DB for mapping uses\n")

@time_decorator
def import_industry_code(bk=industry_classification_file):
    db = pyo.connect(db_path+"/"+"trades.db")
    dfs = pd.read_excel(bk, sheet_name=None,
                        dtype={"SITC5":str,"HS8":str})
    for table, df in dfs.items():
        try:
            df['UPLOAD_TO_DB_DATE'] = datetime.datetime.now()
            df.to_sql(table, db, index=False)
        except ValueError:
            print(f"{table} already exists\n")
        else:
            print(f"Successfully imported {table} into DB for mapping uses\n")

if __name__ == '__main__':
    start = time.time()
    import_geography_code()
    import_sitctohs_code()
    import_hs_code()
    import_sitc_code()
    import_industry_code()
    end = time.time()
    print(f'used time: {end-start:.3f}s')
    input('Please Enter key to quit')
