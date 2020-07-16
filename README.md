# tradestat_multiprocessing_with_DB
tradestat_multiprocessing_with_DB is a Python module to generate meaningful reports of HK's external merchandise trade statistics from numerical raw data issued by authorized department with multiprocessing concept

![link not valid](https://raw.githubusercontent.com/oda-developer/tradestat/master/transform.PNG)

- 4 types of reports are provided
- HK's external merchandise trade by currency (HKD, USD) and dollar units (thousand, million) with: 
  Country (total number: 214)

### Developing or suggested working environment: 
- Python version 3.8 or above
- Window 10

### Dependencies: suggest most updated version 
1) [pandas](https://github.com/pandas-dev/pandas) 
2) [NumPy](https://www.numpy.org)
3) [xlsxwriter](https://pypi.org/project/XlsxWriter/)
4) [openpyxl](https://openpyxl.readthedocs.io/en/stable/index.html)
5) [multiprocessing](https://docs.python.org/3.8/library/multiprocessing.html)

### Folders/Files description:
1) [C&SD_raw_data](https://github.com/v-w-dev/tradestat_multiprocessing_with_DB/tree/master/C%26SD_raw_data)
   A few of periods of raw data in DAT format to demonstrate, and description file can be found
   
2) [Output](https://github.com/v-w-dev/tradestat_multiprocessing_with_DB/tree/master/Output)
   Full completed reports in Excel format as examples can be downloaded as demo

3) [trades_metafiles_into_DB.py](https://github.com/v-w-dev/tradestat_multiprocessing_with_DB/blob/master/trades_metafiles_into_DB.py)	
   trades_metafiles_into_DB.py will insert geography, commodity, industry, product code, etc in a sqlite database by following function
   import_geography_code()
   import_sitctohs_code()
   import_hs_code()
   import_sitc_code()
   import_industry_code()
 
4) [trades_records_into_DB.py](https://github.com/v-w-dev/tradestat_multiprocessing_with_DB/blob/master/trades_records_into_DB.py)
   insert all the commodity trade records for HK with other countries into sqlite database
   ![the link not valid](https://raw.githubusercontent.com/v-w-dev/tradestat_multiprocessing_with_DB/master/merchandise_trades_DB/sqlite%20DB.PNG)
   ![the link not valid](https://raw.githubusercontent.com/v-w-dev/tradestat_multiprocessing_with_DB/master/merchandise_trades_DB/sqlite%20DB%20view.PNG)
   
   
6) [metadata](https://github.com/v-w-dev/tradestat_multiprocessing_with_DB/tree/master/metadata)
   stores the mapping table or metadata files

### Instruction to use:
1) - Run one of the world.py, region.py, area.py, country.py each time
   - if not have Python or the above packages installed, run world.exe or region.exe for trial 
2) Enter start year, end period, number of products to display 
3) Process can be tracked by the pyprind percentage indicator 
4) Reports in Excel format will be generated in a new folder "Output"
