"""
Define functions to get geography codes for individual country, region, area.
The code file is imported from the folder "./metadata"
"""
from xlrd import open_workbook
import sqlite3 as pyo
import pandas as pd
from BSO.time_analysis import time_decorator

geo_file = 'metadata/geography.xlsx'
sitc2hs_file = 'metadata/sitc2hs.xlsx'
product_code_file = 'metadata/product_code.xlsx'

db_path = "merchandise_trades_DB"

@time_decorator
def import_geography_code(bk=geo_file):
    db = pyo.connect(db_path+"/"+"trades.db")
    dfs = pd.read_excel(bk, sheet_name=None)

    for table, df in dfs.items():
        try:
            df.to_sql(table, db, index=False)
        except ValueError:
            print(f"{table} already exists\n")
        else:
            print(f"Successfully imported {table} into DB for mapping uses\n")


# make a dictionery(HS codes as keys, SITC code as values)
#@time_decorator
def get_hstositc_code(bk=sitc2hs_file):
    db = pyo.connect(db_path+"/"+"trades.db")
    dfs = pd.read_excel(bk, sheet_name=None,
                        dtype={"SITC5_text":str,"HS8_text":str})
    for table, df in dfs.items():
        try:
            df.to_sql(table, db, index=False)
        except ValueError:
            print(f"{table} already exists\n")
        else:
            print(f"Successfully imported {table} into DB for mapping uses\n")

'''
@time_decorator
def get_sitc_name(excelfile=product_code_file, sheet_name="sitc3" ):
    """Attribute for sheet: 1)"sitc3", 2)"sitc5" """
    book = open_workbook(excelfile)
    sheet = book.sheet_by_name(sheet_name)
    sitc = {}
    for row in range(sheet.nrows):
        if row == 0 or row == 1:
            continue
        if sheet_name=="sitc3":
            sitc[str(int(sheet.cell(row,1).value)).rjust(3,'0')] = str(sheet.cell(row,2).value)
        elif sheet_name=="sitc5":
            sitc[str(int(sheet.cell(row,0).value)).rjust(3,'0')] = str(sheet.cell(row,1).value)
    return sitc
'''

if __name__ == '__main__':
    import_geography_code()
    get_hstositc_code()

'''
There are two versions(SITC, HS) for classifying trade products
The fuctions are for geting the product codes.
The files for conversion for SITC to HS, product code, are imported from the folder "./metadata"

from xlrd import open_workbook
from .time_analysis import time_decorator
# define the path for sitc2hs, product_code
sitc2hs_file = './metadata/sitc2hs.xlsx'
product_code_file = './metadata/product_code.xlsx'

# make a dictionery(HS codes as keys, SITC code as values)
#@time_decorator
def get_hstositc_code(excelfile=sitc2hs_file, sheet="sitc2hs"):
    book = open_workbook(excelfile)
    sheet = book.sheet_by_name(sheet)
    hstositc = {}
    for row in range(sheet.nrows):
        if row == 0:
            continue
        hstositc[str(int(sheet.cell(row,1).value)).rjust(8,'0')] = str(int(sheet.cell(row,0).value)).rjust(5,'0')
    return hstositc
#@time_decorator
def get_sitc_name(excelfile=product_code_file, sheet_name="sitc3" ):
    """Attribute for sheet: 1)"sitc3", 2)"sitc5" """
    book = open_workbook(excelfile)
    sheet = book.sheet_by_name(sheet_name)
    sitc = {}
    for row in range(sheet.nrows):
        if row == 0 or row == 1:
            continue
        if sheet_name=="sitc3":
            sitc[str(int(sheet.cell(row,1).value)).rjust(3,'0')] = str(sheet.cell(row,2).value)
        elif sheet_name=="sitc5":
            sitc[str(int(sheet.cell(row,0).value)).rjust(3,'0')] = str(sheet.cell(row,1).value)
    return sitc


if __name__ == '__main__':
    adict = get_hstositc_code()
    print("1) example for conversion")
    print("conversion of HS 01011000: SITC %s" % adict['01011000'])
    print("conversion of HS 85285291: SITC %s" % adict['85285291'])

    print("2) example for geting SITC-3 product name")
    b = get_sitc_name()
    print("SITC-3-digit '001' product name: %s" %b['001'])
'''
