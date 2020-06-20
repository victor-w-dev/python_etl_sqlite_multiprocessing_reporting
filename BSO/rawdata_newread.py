"""
The raw data from Census and Statistics Department, HK, contain 3 parts:
1. Imports/domestic exports/re-exports by consignment country/territory by HS commodity item
2. Imports by origin country/territory by HS commodity item
3. Re-exports by origin country/territory by destination country/territory by HS commodity item
They can be found in folder "./C&SD_raw_data", grouped by year and month.

File names for above namely are:
1. HSCCIT.DAT
2. HSCOIT.DAT
3. HSCOCCIT.DAT
Define functions to get raw data from the 3 files, then merge in dataframe
"""
import pandas as pd
import numpy as np
from .time_analysis import time_decorator
#from .merge import hstositc_dict

rawdata_folder="C&SD_raw_data"
# conversion of hs to SITC
#hstositc_dict = get_hstositc_code()

def read_file(file_handler):
    for line in file_handler:
        row = line.strip()

        yield (row[0],
               row[1:9],
               row[9:12],
               row[48:66],
               row[66:84],
               row[120:138],
               row[138:156],
               row[192:210],
               row[210:228])

@time_decorator
def get_hsccit(year, month=12, path=rawdata_folder):
    try:
        file_path = f'{path}/{year}{month}/hsccit.dat'
        open(file_path)
    except:
        file_path = f'{path}/{year}{month}/hsccit.txt'
        open(file_path)
        print(f"Import from txt file: {file_path}")
    else:
        print(f"Import from dat file: {file_path}")

    # using readlines methods may be slow, but actually the program need
    #not to use all the column after spliting
    #it will be faster than using pd.read_fwf as this pd.read_fwf function
    #seem need to
    #import all columns
    with open(file_path, 'r', encoding='utf-8') as file_object:
        #lines = file_object.readlines()
        #print(lines[0])
        #pass
        #ls=[]
        #for l in read_large_file(file_object):
            #print(l)
        #labels = ['f1','f2','f3','IM','IM_Q','DX','DX_Q','RX','RX_Q']
        #ls=list(read_file(file_object))
        print(read_file(file_object))
        return read_file(file_object)
        #print(next(a))
        #df = pd.DataFrame(ls,columns=['f1','f2','f3','IM','IM_Q','DX','DX_Q','RX','RX_Q'])
        #df[[labels[0]]+labels[2:]] = df[[labels[0]]+labels[2:]].apply(pd.to_numeric)
        #df[[] = df[["a", "b"]].apply(pd.to_numeric)
        #print(df.dtypes)
        #print(np.array(ls))


    '''
    f1,f2,f3,f6,f7,f10,f11,f14,f15 = ([] for _ in range(9))

    for i, line in enumerate(lines):
        row = line.strip()
        # for first row(i==0) in the hsccit.dat file, length of the row is 229 instead of 228.
        # so need to add +1 for adjustment as following.
        f1.append(int(row[0]))
        f2.append(str(row[1:9]))
        f3.append(int(row[9:12]))
        f6.append(int(row[48:66]))
        f7.append(int(row[66:84]))
        f10.append(int(row[120:138]))
        f11.append(int(row[138:156]))
        f14.append(int(row[192:210]))
        f15.append(int(row[210:228]))

    df = pd.DataFrame({'f1':f1, 'f2':f2, 'f3':f3,
                        'IM': f6,'IM_Q':f7, 'DX':f10,'DX_Q':f11,'RX':f14,'RX_Q':f15})

    df['TX'] = df['DX'] + df['RX']
    df['TT'] = df['IM'] + df['TX']
    df['TX_Q'] = df['DX_Q'] + df['RX_Q']
    df['TT_Q'] = df['TX_Q'] + df['IM_Q']

    # select transaction type 1 (HS-8digit) only
    HS8only = df.f1.isin([1])
    #print(df)
    df = df[HS8only]

    # add HS2, HS4 and HS6 columns
    df['HS-2'] = [x[:2] for x in df.f2]
    df['HS-4'] = [x[:4] for x in df.f2]
    df['HS-6'] = [x[:6] for x in df.f2]

    df['SITC-1'] = [hstositc_dict.get(x, "NA")[:1] for x in df.f2]
    df['SITC-2'] = [hstositc_dict.get(x, "NA")[:2] for x in df.f2]
    df['SITC-3'] = [hstositc_dict.get(x, "NA")[:3] for x in df.f2]
    df['SITC-4'] = [hstositc_dict.get(x, "NA")[:4] for x in df.f2]
    df['SITC-5'] = [hstositc_dict.get(x, "NA") for x in df.f2]
    '''
    #df['reporting_time'] = f'{year}{month}'
    #print(df)
    #return df

#@time_decorator
def get_hscoit(year, month=12, path=rawdata_folder):
    try:
        file_path = f'{path}/{year}{month}/hscoit.dat'
        open(file_path)
    except:
        file_path = f'{path}/{year}{month}/hscoit.txt'
        print(f"Import from txt file: {file_path}")
    else:
        print(f"Import from dat file: {file_path}")

    with open(file_path, encoding='utf-8') as file_object:
        lines = file_object.readlines()

    f1,f2,f3,f6,f7 = ([] for _ in range(5))

    for i, line in enumerate(lines):
        row = line.strip()
        f1.append(int(row[0]))
        f2.append(str(row[1:9]))
        f3.append(int(row[9:12]))
        f6.append(int(row[48:66]))
        f7.append(int(row[66:84]))


    df = pd.DataFrame({'f1':f1,'f2':f2,'f3':f3,'IMbyO':f6,'IMbyO_Q':f7})
    '''
    # select for transaction type 1 (HS-8digit) only
    HS8only = df.f1.isin([1])
    df = df[HS8only]

    # add HS2, HS4 and HS6 columns
    df['HS-2'] = [x[:2] for x in df.f2]
    df['HS-4'] = [x[:4] for x in df.f2]
    df['HS-6'] = [x[:6] for x in df.f2]

    # conversion of hs to SITC
    df['SITC-1'] = [hstositc_dict.get(x, "NA")[:1] for x in df.f2]
    df['SITC-2'] = [hstositc_dict.get(x, "NA")[:2] for x in df.f2]
    df['SITC-3'] = [hstositc_dict.get(x, "NA")[:3] for x in df.f2]
    df['SITC-4'] = [hstositc_dict.get(x, "NA")[:4] for x in df.f2]
    df['SITC-5'] = [hstositc_dict.get(x, "NA") for x in df.f2]
    '''
    df['reporting_time'] = f'{year}{month}'
    return df

#@time_decorator
def get_hscoccit(year, month=12, path=rawdata_folder):
    try:
        file_path = f'{path}/{year}{month}/hscoccit.dat'
        open(file_path)
    except:
        file_path = f'{path}/{year}{month}/hscoccit.txt'
        print(f"Import from txt file: {file_path}")
    else:
        print(f"Import from dat file: {file_path}")

    with open(file_path, encoding='utf-8') as file_object:
        lines = file_object.readlines()

    f1,f2,f3,f4,f7,f8 = ([] for _ in range(6))

    for i, line in enumerate(lines):
        row = line.strip()
        f1.append(int(row[0]))
        f2.append(str(row[1:9]))
        f3.append(int(row[9:12]))
        f4.append(int(row[12:15]))
        f7.append(int(row[51:69]))
        f8.append(int(row[69:87]))

    df = pd.DataFrame({'f1':f1,'f2':f2,'f3':f3,'f4':f4,'RXbyO':f7,'RXbyO_Q':f8})
    df['reporting_time'] = f'{year}{month}'

    '''
    # select for transaction type 1 (HS-8digit) only
    HS8only = df.f1.isin([1])
    df = df[HS8only]

    # add HS2, HS4 and HS6 columns
    df['HS-2'] = [x[:2] for x in df.f2]
    df['HS-4'] = [x[:4] for x in df.f2]
    df['HS-6'] = [x[:6] for x in df.f2]

    # conversion of hs to SITC
    df['SITC-1'] = [hstositc_dict.get(x, "NA")[:1] for x in df.f2]
    df['SITC-2'] = [hstositc_dict.get(x, "NA")[:2] for x in df.f2]
    df['SITC-3'] = [hstositc_dict.get(x, "NA")[:3] for x in df.f2]
    df['SITC-4'] = [hstositc_dict.get(x, "NA")[:4] for x in df.f2]
    df['SITC-5'] = [hstositc_dict.get(x, "NA") for x in df.f2]
    '''

    return df

if __name__ == '__main__':
    #calculate time spent
    print("test")
    get_hsccit(2018, month='12') #3.19s 20200613 before modification
    #print(df)
    get_hscoit(2018, month='12') #1.51s 20200613 before modification
    get_hscoccit(2018, month='12') #5.07s 20200613 before modification
