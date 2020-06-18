#from .rawdata_array_passmethod import *
#from .rawdata_pd_read_fwf_method import *
from .rawdata_newread import *
from .time_analysis import time_decorator
from .hstositc import get_hstositc_code
import multiprocessing

hstositc_dict = get_hstositc_code()

def exclgold(df):
    # filter out the gold commodity
    gold_commodity = ["71081100", "71081210", "71081290", "71081300",
                      "71082010", "71082090", "71090000", "71123000",
                      "71129100", "71189000"]
    gold_T = df.f2.isin(gold_commodity)
    df_exclgold = df[~gold_T]
    return df_exclgold

def df_add_columns(df,type):
    df=df.copy(deep=True)

    if type=="hsccit":
        df['TX'] = df.loc[:,'DX'] + df.loc[:,'RX']
        df['TT'] = df.loc[:,'IM'] + df.loc[:,'TX']
        df['TX_Q'] = df.loc[:,'DX_Q'] + df.loc[:,'RX_Q']
        df['TT_Q'] = df.loc[:,'TX_Q'] + df['IM_Q']

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

    return df

@time_decorator
def mergedf(startyear=2016, endperiod=201907, type="hsccit"):

    get_rawdata = {"hsccit":get_hsccit,
                   "hscoit":get_hscoit,
                   "hscoccit":get_hscoccit}

    if (len(str(endperiod)) == 6) & (str(endperiod)[-2:] != '12'):
        # acquire yearly data
        yearly_df = [get_rawdata[type](year = yr) for yr in range(startyear, int(str(endperiod)[:4]))]
        yearly_df = pd.concat(yearly_df,sort=False)

        # acquire year-to-date data
        endmonth = str(endperiod)[-2:]
        df1 = get_rawdata[type](int(str(endperiod)[:4]), month=endmonth)
        df2 = get_rawdata[type](int(str(endperiod)[:4])-1, month=endmonth)

        yeartod_df = pd.concat([df1,df2],sort=False)

        result = pd.concat([yearly_df,yeartod_df],sort=False)

    elif (len(str(endperiod)) == 4):
        result = [get_rawdata[type](year = yr) for yr in range(startyear, endperiod+1)]
        result = pd.concat(result,sort=False)
    # final df will exclude gold commodity

    result=df_add_columns(result, type)

    #print(result)
    return exclgold(result)

@time_decorator
def mergedf_multiprocessing(startyear=2016, endperiod=201907, type="hsccit"):

    get_rawdata = {"hsccit":get_hsccit,
                   "hscoit":get_hscoit,
                   "hscoccit":get_hscoccit}

    multiprocessing.freeze_support()
    #use all cpu core for multiprocessing
    p = multiprocessing.Pool(processes = multiprocessing.cpu_count())
    print(f"testing multiprocessing: Total CPU count {multiprocessing.cpu_count()}")
    print(f"using {multiprocessing.cpu_count()} for merging dataset")

    results=[]
    if (len(str(endperiod)) == 6) & (str(endperiod)[-2:] != '12'):
        # acquire yearly data
        #yearly_df = [get_rawdata[type](year = yr) for yr in range(startyear, int(str(endperiod)[:4]))]
        #yearly_df = pd.concat(yearly_df,sort=True)

        for yr in range(startyear, int(str(endperiod)[:4])):
            result = p.apply_async(get_rawdata[type],(yr,))
            results.append(result)

        # acquire year-to-date data
        endmonth = str(endperiod)[-2:]
        # df1 = get_rawdata[type](int(str(endperiod)[:4]), month=endmonth)
        # df2 = get_rawdata[type](int(str(endperiod)[:4])-1, month=endmonth)
        df1 = p.apply_async(get_rawdata[type],(int(str(endperiod)[:4]), endmonth))
        df2 = p.apply_async(get_rawdata[type],(int(str(endperiod)[:4])-1, endmonth))
        results.append(df1)
        results.append(df2)

        #p.close()
        #p.join()
        #yeartod_df = pd.concat([df1.get(),df2.get()],sort=True)
        #results = pd.concat([res.get() for res in results],sort=False)
        #result = pd.concat([yearly_df,yeartod_df],sort=False)

    elif (len(str(endperiod)) == 4):

        # assign job for multiprocessing
        for yr in range(startyear, endperiod+1):
            result = p.apply_async(get_rawdata[type],(yr,))
            results.append(result)

    p.close()
    p.join()

    results = pd.concat([res.get() for res in results],sort=False)
        #print(results)
    results=df_add_columns(results, type)
    # final df will exclude gold commodity
    #print(results)
    return exclgold(results)

if __name__ == '__main__':
    mergedf(startyear=2016, endperiod=201907, type="hsccit")
    mergedf_multiprocessing(startyear=2016, endperiod=201907, type="hsccit")
