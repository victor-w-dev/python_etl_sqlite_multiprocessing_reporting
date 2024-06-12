# python_etl_sqlite_multiprocessing_reporting
python_etl_sqlite_multiprocessing_reporting is a Python program to generate meaningful reports of HK's external merchandise trade statistics from raw semi-structured data issued by authorized department with object-oriented and multiprocessing concept, using SQLite as database.

![link not valid](https://github.com/v-w-dev/tradestat_multiprocessing_with_DB/blob/master/transform.PNG?raw=true)

- 4 types of reports are provided
- HK's external merchandise trade by currency (HKD, USD) and dollar units (thousand, million) with: 
  Country (total number: 214 x 4 = 856 excel files in total)

### Developing or suggested working environment: 
- Python version 3.8 or above
- Window 10

### Dependencies: suggest most updated version 
1) [pandas](https://github.com/pandas-dev/pandas) 
2) [NumPy](https://www.numpy.org)
3) [xlsxwriter](https://pypi.org/project/XlsxWriter/)
4) [openpyxl](https://openpyxl.readthedocs.io/en/stable/index.html)
5) [multiprocessing](https://docs.python.org/3.8/library/multiprocessing.html)
6) [sqlite3](https://docs.python.org/3/library/sqlite3.html)

### Folders/Files description:
1) [C&SD_raw_data](https://github.com/v-w-dev/tradestat_multiprocessing_with_DB/tree/master/C%26SD_raw_data): raw data not included in this github
   
2) [Output](https://github.com/v-w-dev/tradestat_multiprocessing_with_DB/tree/master/Output):
   Full completed reports in Excel format as examples can be downloaded as demo

3) [trades_metafiles_into_DB.py](https://github.com/v-w-dev/tradestat_multiprocessing_with_DB/blob/master/trades_metafiles_into_DB.py):	
   trades_metafiles_into_DB.py will insert geography, commodity, industry, product code, etc in a sqlite database by following functions:<br>
    ```Python
   import_geography_code()
   import_sitctohs_code()
   import_hs_code()
   import_sitc_code()
   import_industry_code()
   ```
    
5) [trades_records_into_DB.py](https://github.com/v-w-dev/tradestat_multiprocessing_with_DB/blob/master/trades_records_into_DB.py):
   - Insert all the commodity trade records for HK with other countries into sqlite database in the folder [merchandise_trades_DB](https://github.com/v-w-dev/tradestat_multiprocessing_with_DB/tree/master/merchandise_trades_DB)
   - The read_file function is a generator that reads lines from a file and yields structured data based on the specified table type. It processes one line at a time for memory efficiency and raises a ValueError for unknown table types.
   ```Python
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
   ```
   ![the link not valid](https://github.com/v-w-dev/tradestat_multiprocessing_with_DB/blob/master/merchandise_trades_DB/sqlite%20DB%20view.PNG?raw=trueG)
    
   
6) [metadata](https://github.com/v-w-dev/tradestat_multiprocessing_with_DB/tree/master/metadata):
   stores the mapping table or metadata files
   
7) [GeneralTrades_getdata.py](https://github.com/v-w-dev/tradestat_multiprocessing_with_DB/blob/master/GeneralTrades_getdata.py)<br>
   [CommodityTrades_getdata.py](https://github.com/v-w-dev/tradestat_multiprocessing_with_DB/blob/master/CommodityTrades_getdata.py)<br>
   These 2 files involve SQL query embedded into python modules to acquire data from SQLite database
   
8) [TradeReports_analysis_multiprocessing.py](https://github.com/v-w-dev/tradestat_multiprocessing_with_DB/blob/master/TradeReports_analysis_multiprocessing.py):
   This module performs multiprocessing by apply_async function to export Excel Reports.
   It run around 230 seconds to export 856 excel reports (0.27 seconds used per one) by following codes:
   ```Python
   for row in reports.acquire_countries_info().itertuples():
            try:
                p.apply_async(CountryReport,(all_figs, periods, 10, row.CODE))

            except:
                print(f"{row.CODE} {row.DESC} has error\n")
                f = open(f"{row.CODE} {row.DESC}.txt", "w")
                f.write(str(sys.exc_info()[0]))
                f.write(str(sys.exc_info()[1]))
                f.close()
    p.close()
    p.join()
   ```
