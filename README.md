# tradestat_multiprocessing_with_DB
tradestat_multiprocessing_with_DB is a Python program to generate meaningful reports of HK's external merchandise trade statistics from numerical raw data issued by authorized department with object-oriented and multiprocessing concept.

![link not valid](https://raw.githubusercontent.com/oda-developer/tradestat/master/transform.PNG)

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

### Folders/Files description:
1) [C&SD_raw_data](https://github.com/v-w-dev/tradestat_multiprocessing_with_DB/tree/master/C%26SD_raw_data)
   A few of periods of raw data in DAT format to demonstrate, and description file can be found
   
2) [Output](https://github.com/v-w-dev/tradestat_multiprocessing_with_DB/tree/master/Output)
   Full completed reports in Excel format as examples can be downloaded as demo

3) [trades_metafiles_into_DB.py](https://github.com/v-w-dev/tradestat_multiprocessing_with_DB/blob/master/trades_metafiles_into_DB.py)	
   trades_metafiles_into_DB.py will insert geography, commodity, industry, product code, etc in a sqlite database by following functions:<br>
   import_geography_code()<br>
   import_sitctohs_code()<br>
   import_hs_code()<br>
   import_sitc_code()<br>
   import_industry_code()<br>
 
4) [trades_records_into_DB.py](https://github.com/v-w-dev/tradestat_multiprocessing_with_DB/blob/master/trades_records_into_DB.py)
   insert all the commodity trade records for HK with other countries into sqlite database in the folder [merchandise_trades_DB](https://github.com/v-w-dev/tradestat_multiprocessing_with_DB/tree/master/merchandise_trades_DB)
   
   ![the link not valid](https://raw.githubusercontent.com/v-w-dev/tradestat_multiprocessing_with_DB/master/merchandise_trades_DB/sqlite%20DB.PNG)
   ![the link not valid](https://raw.githubusercontent.com/v-w-dev/tradestat_multiprocessing_with_DB/master/merchandise_trades_DB/sqlite%20DB%20view.PNG)
    
   
5) [metadata](https://github.com/v-w-dev/tradestat_multiprocessing_with_DB/tree/master/metadata)
   stores the mapping table or metadata files
   
6) [GeneralTrades_getdata.py](https://github.com/v-w-dev/tradestat_multiprocessing_with_DB/blob/master/GeneralTrades_getdata.py)<br>
   [CommodityTrades_getdata.py](https://github.com/v-w-dev/tradestat_multiprocessing_with_DB/blob/master/CommodityTrades_getdata.py)<br>
   These 2 files involve SQL query embedded into python module to acquire data from sqlite database
   
7) [TradeReports_analysis_multiprocessing.py](https://github.com/v-w-dev/tradestat_multiprocessing_with_DB/blob/master/TradeReports_analysis_multiprocessing.py)
   This module performs multiprocessing by apply_async function to export Excel Reports.
   It run around 230 seconds to export 856 excel reports (0.27 seconds used per one) by following codes:
   
   p = multiprocessing.Pool(processes = multiprocessing.cpu_count()) <br>

   for row in reports.acquire_countries_info().itertuples(): <br>
   &nbsp;&nbsp;try: <br>
   &nbsp;&nbsp;&nbsp;&nbsp;p.apply_async(CountryReport,(all_figs, periods, 10, row.CODE)) <br>
   p.close()<br>
   p.join()<br>
  
