from BSO.time_analysis import time_decorator
import sqlite3 as pyo
import os
import datetime
import time

rawdata_folder="C&SD_raw_data"
db_path = "merchandise_trades_DB"

def read_file(file_handler, table):
    for line in file_handler:
        row = line.strip()      
        if table == 'hsccit':
            yield [
                row[0],
                row[1:9],
                row[9:12],
                row[12:30],
                row[30:48],
                row[48:66],
                row[66:84],
                row[84:102],
                row[102:120],
                row[120:138],
                row[138:156],
                row[156:174],
                row[174:192],
                row[192:210],
                row[210:228]
            ]
        
        elif table == 'hscoit':
            yield [
                row[0],
                row[1:9],
                row[9:12],
                row[12:30],
                row[30:48],
                row[48:66],
                row[66:84]
            ]
        
        elif table == 'hscoccit':
            yield [
                row[0],
                row[1:9],
                row[9:12],
                row[12:15],
                row[15:33],
                row[33:51],
                row[51:69],
                row[69:87]
            ]
        
        else:
            raise ValueError(f"Unknown table type: {table}")
#@time_decorator
class TradeDB:
    def __init__(self):
        if not os.path.exists(db_path):
            os.makedirs(db_path)
        self.con = pyo.connect(db_path+"/"+"trades.db")
        self.cursor = self.con.cursor()

        create_HSCCIT_query = ("CREATE TABLE IF NOT EXISTS hsccit(ID INTEGER PRIMARY KEY,"
        "TransactionType INTEGER," #1
        "HScode TEXT," #2
        "CountryConsignmentCode INTEGER," #3
        "ImportValueMonthly INTEGER," #4
        "ImportQuantityMonthly INTEGER," #5
        "ImportValueYTD INTEGER," #6
        "ImportQuantityYTD INTEGER," #7
        "DomesticExportValueMonthly INTEGER," #8
        "DomesticExportQuantityMonthly INTEGER," #9
        "DomesticExportValueYTD INTEGER," #10
        "DomesticExportQuantityYTD INTEGER," #11
        "ReExportValueMonthly INTEGER," #12
        "ReExportQuantityMonthly INTEGER," #13
        "ReExportValueYTD INTEGER," #14
        "ReExportQuantityYTD INTEGER," #15
        "ReportPeriod INTEGER," #16
        "UploadedToDBDate TIMESTAMP)" #17
        )

        create_HSCOIT_query = ("CREATE TABLE IF NOT EXISTS hscoit(ID INTEGER PRIMARY KEY,"
        "TransactionType INTEGER," #1
        "HScode TEXT," #2
        "CountryOriginCode INTEGER," #3
        "ImportByOriginValueMonthly INTEGER," #4
        "ImportByOriginQuantityMonthly INTEGER," #5
        "ImportByOriginValueYTD INTEGER," #6
        "ImportByOriginQuantityYTD INTEGER," #7
        "ReportPeriod INTEGER," #8
        "UploadedToDBDate TIMESTAMP)" #9
        )

        create_HSCOCCIT_query = ("CREATE TABLE IF NOT EXISTS hscoccit(ID INTEGER PRIMARY KEY,"
        "TransactionType INTEGER," #1
        "HScode TEXT," #2
        "CountryOriginCode INTEGER," #3
        "CountryDestinationCode INTEGER," #4
        "ReExportValueMonthly INTEGER," #5
        "ReExportQuantityMonthly INTEGER," #6
        "ReExportValueYTD INTEGER," #7
        "ReExportQuantityYTD INTEGER," #8
        "ReportPeriod INTEGER," #9
        "UploadedToDBDate TIMESTAMP)" #10
        )

        self.cursor.execute(create_HSCCIT_query)
        self.cursor.execute(create_HSCOIT_query)
        self.cursor.execute(create_HSCOCCIT_query)

        self.con.commit()
        print("You have connected to the database")
        print(self.con)

    @time_decorator
    def check_report_period(self,table):
        self.cursor.execute(f"SELECT DISTINCT ReportPeriod FROM {table}")
        rows = self.cursor.fetchall()
        return [i[0] for i in rows]

    #@time_decorator
    def insert_DB(self, table, year, month=12, path=rawdata_folder):

        period = f'{year}{month}'
        try:
            file_path = f'{path}/{period}/{table}.dat'
            print(path)
            open(file_path)
        except:
            file_path = f'{path}/{period}/{table}.txt'
            open(file_path)
            print(f"Import from txt file: {file_path}")
        else:
            print(f"Import from dat file: {file_path}")

        with open(file_path, 'r', encoding='utf-8') as file_object:
            for line in read_file(file_object,table):
                #print(type(i))
                line = line + [f'{year}{month}'] + [datetime.datetime.now()]
                #print(line)
                #print(len(line))
                self.insert_line(table, line)
            self.con.commit()

    def insert_line(self, table, values):
        if table == "hsccit":
            column_str = """TransactionType, HScode, CountryConsignmentCode,
                            ImportValueMonthly,
                            ImportQuantityMonthly,
                            ImportValueYTD,
                            ImportQuantityYTD,
                            DomesticExportValueMonthly,
                            DomesticExportQuantityMonthly,
                            DomesticExportValueYTD,
                            DomesticExportQuantityYTD,
                            ReExportValueMonthly,
                            ReExportQuantityMonthly,
                            ReExportValueYTD,
                            ReExportQuantityYTD,
                            ReportPeriod,
                            UploadedToDBDate
                            """
            insert_str = ("?, " * 17)[:-2]

        if table == "hscoit":
            column_str = """TransactionType, HScode,
                            CountryOriginCode,
                            ImportByOriginValueMonthly,
                            ImportByOriginQuantityMonthly,
                            ImportByOriginValueYTD,
                            ImportByOriginQuantityYTD,
                            ReportPeriod,
                            UploadedToDBDate
                            """
            insert_str = ("?, " * 9)[:-2]

        if table == "hscoccit":
            column_str = """TransactionType, HScode,
                            CountryOriginCode,
                            CountryDestinationCode,
                            ReExportValueMonthly,
                            ReExportQuantityMonthly,
                            ReExportValueYTD,
                            ReExportQuantityYTD,
                            ReportPeriod,
                            UploadedToDBDate
                            """
            insert_str = ("?, " * 10)[:-2]

        sql=(f"INSERT INTO {table} ({column_str}) VALUES ({insert_str})")
        self.cursor.executemany(sql,[values])

@time_decorator
def importdataDB(dbinstance, exist_periods_dict, startyear=2006, endyear=2020):
    for yr in range(startyear,endyear+1):
        for m in range(1,13):
            for table, existing_periods in exist_periods_dict.items():
                p = f'{yr}{m:02}'
                if p not in existing_periods:
                    try:
                        dbinstance.insert_DB(table,yr,month=f"{m:02}")

                    except FileNotFoundError:
                        print(f"{yr}{m:02} {table} does not exist\n")
                    else:
                        print(f"Successfully imported {yr}{m:02} {table} into DB\n")
                else:
                    print(f"Already had {yr}{m:02} {table} in DB\n")

if __name__ == '__main__':
    start = time.time()
    db = TradeDB()

    print("Please wait a moment to extract existing periods, should be in seconds")
    db_exist_periods={'hsccit': db.check_report_period('hsccit'),
                      'hscoit': db.check_report_period('hscoit'),
                      'hscoccit': db.check_report_period('hscoccit')}
    # extract existing periods
    for k, v in db_exist_periods.items():
        print(k, v, '\n')

    forward = input("randomly enter to go forward")
    importdataDB(db, db_exist_periods, startyear=2006, endyear=datetime.datetime.today().year)

    end = time.time()
    print(f'used time: {end-start:.3f}s') #242s to insert 200612 - 202004 data
    input('Please Enter key to quit')
