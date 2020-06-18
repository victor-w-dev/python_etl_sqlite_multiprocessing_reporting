# tradestat
**tradestat** is a Python module to generate meaningful reports of HK's external merchandise trade statistics from numerical raw data issued by authorized department.

The reports include **HK's Export, Import, Re-Export, bilateral trades, etc. of different commodities with other countries, regions**

The official government database website as reference: https://tradeidds.censtatd.gov.hk/Index/a83bc112c6c24d04ab04f9a4c987181e

### Intuition  
- RHS is the report for **HK with USA**, describing general trade statistics with commodity classification
- LHS is one of the raw data in numerical format
![the link not valid](https://raw.githubusercontent.com/v-w-dep/tradestat/master/transform.PNG)

- 4 types of reports are provided
- HK's external merchandise trade by currency (HKD, USD) and dollar units (thousand, million) with: 
1) World   (total number:   1x4 =   4) 
2) Region  (total number:  16x4 =  64)
3) Area    (total number:   9x4 =  36)
4) Country (total number: 214x4 = 856)

- altogther, **960** reports in Excel format will be generated, supposed within **30 mins** (in my PC).

### Developing or suggested working environment: 
- Python version 3.6 or above
- Window 10

### Dependencies: suggest most updated version 
1) [pandas](https://github.com/pandas-dev/pandas) 
2) [NumPy](https://www.numpy.org)
3) [xlrd](https://github.com/python-excel/xlrd)
4) [xlsxwriter](https://pypi.org/project/XlsxWriter/)
5) [openpyxl](https://openpyxl.readthedocs.io/en/stable/index.html)
6) [pyprind](https://github.com/rasbt/pyprind)
7) [pypiwin32](https://github.com/mhammond/pywin32); try pip install pywin32 or pip install pypiwin32

### Folder description:
1) [BSO](https://github.com/v-w-dep/tradestat/tree/master/BSO)	
   self-implemented package focusing on reading raw data, calculating the trade values
   
   rawdata_pd_read_fwf_method.py, a module using [pandas.read_fwf](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_fwf.html) for reading fixed-width-format raw data.
   
   Relative import concept in Python is also used.
   
2) [C&SD_raw_data](https://github.com/v-w-dep/tradestat/tree/master/C%26SD_raw_data)
   A few of periods of raw data in DAT format to demonstrate, and description file can be found
   
3) EXE_spec can be ignored

4) [Output_completed_as_example](https://github.com/v-w-dep/tradestat/tree/master/Output_completed_as_example)
   Full completed reports in Excel format as examples can be downloaded 

5) [export](https://github.com/v-w-dep/tradestat/blob/master/export/export_file.py)	
   export_file.py mainly focus on exporting excel files with defined format using openpyxl
   and win32com.client for autofit column width in excel
   
6) [metadata](https://github.com/v-w-dep/tradestat/tree/master/metadata)
   define the country, area, region, industry, product codes and related information

### Instruction to use:
1) - Run one of the world.py, region.py, area.py, country.py each time
   - if not have Python or the above packages installed, run world.exe or region.exe for trial 
2) Enter start year, end period, number of products to display 
3) Process can be tracked by the pyprind percentage indicator 
4) Reports in Excel format will be generated in a new folder "Output"

#### Instruction video:
Can see video by clicking the image below:
- world   
[![link not valid](http://img.youtube.com/vi/xAyWChMQHxM/0.jpg)](http://www.youtube.com/watch?v=xAyWChMQHxM "tradestat instruction: world")
- region  
[![link not valid](http://img.youtube.com/vi/5oGL_wVnG8g/0.jpg)](http://www.youtube.com/watch?v=5oGL_wVnG8g "tradestat instruction: region")


