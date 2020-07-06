import time
import sqlite3 as pyo
import pandas as pd
import numpy as np
from GeneralTrades_real import GeneralTradesProfile
from CommodityTrades import CommodityTradesProfile
from BSO.time_analysis import time_decorator




class TradeReports(object):
    """docstring for ."""

    def __init__(self, db_path, periods):
        self._db_path = db_path
        self.periods = periods

    @property
    def periods(self):
        return self._periods

    @periods.setter
    def periods(self, periods):
        if isinstance(periods, tuple):
            self._periods = periods
        else:
            raise TypeError('periods must be tuple')
        if len(periods) > 5:
            raise ValueError('length of tuple of periods must be less than or equal to 5')

    def all_trades_figures(self):
        self.all_general_trades = GeneralTradesProfile(self._db_path, self.periods).get_figures_1lot()
        self.all_commodity_trades = CommodityTradesProfile(self._db_path, self.periods).get_commodity_trade()
        #print(self.all_general_trades)
        #print(self.all_commodity_trades)
        return {'general_trades': self.all_general_trades, 'commodity_trades': self.all_commodity_trades}

    def acquire_countries_info(self):
        self.con = pyo.connect(self._db_path+"/"+"trades.db")

        result = []
        country_info_sql = f"""
        SELECT CODE, DESC, CDESC from country
        """
        self.countries_info_df= pd.read_sql_query(country_info_sql, self.con)
        #print(self.df)

        return self.countries_info_df

class CountryReport(object):
    """docstring for ."""

    @time_decorator
    def __init__(self, country_code, periods_required, **all_trades_figures):
        self._country_code = country_code
        all_general_trades = all_trades_figures['general_trades']
        all_commodity_trades = all_trades_figures['commodity_trades']

        self._general_figures = all_general_trades[all_general_trades.countrycode.isin([country_code])]
        self._commodity_figures = all_commodity_trades[all_commodity_trades.CountryConsignmentCode.isin([country_code])]

        self._periods_required = list(periods_required)
        #self._periods = sorted(set(self._commodity_figures.ReportPeriod))
        print(self._general_figures)
        print(self._commodity_figures)
        print(self._periods_required)

    @time_decorator
    def TX_byproduct(self):
        '''
        if len(periods)==4:
            sorting=[lastperiod,periods[-2],periods[-3],periods[-4], codetype]
        '''
        if len(self._periods)==5:
            #sorting=['201907']
            column_sort_order = tuple(self._periods)#[::-1]
        '''
        # see the periods of commodity_figures
        periods_incl = set(self._commodity_figures.ReportPeriod)
        periods_lack = []

        # find out the periods not contained in df
        for p in periods:
            if p not in df_have_periods:
                nothave.append(p)
        if nothave:
            for p in nothave:
                df = df.append({'reporting_time' : str(p), tradetype:0, codetype:'N.A.'} , ignore_index=True)
                '''



        self.TX = pd.pivot_table(self._commodity_figures, values='TX', index=['SITC3','SITC3_eng_name'],columns=['ReportPeriod'],\
                  aggfunc=np.sum, fill_value=0, margins=True)\
                  .sort_values(by=column_sort_order,\
                  ascending=False)

        print(self.TX)
        self.TX.to_excel("checking3_TX_China_bySITC3.xlsx")


    @time_decorator
    def trades_byproduct(self, topnumber=10):
        '''
        Returns: the dictionary of DataFrame of commodity figures by 4 tradetype.
                'TX' : Total Export
                'DX' : Domestic Export
                'RX' : Re-Export
                'IM' : Import by consignment
        '''

        '''
        if len(periods)==4:
            sorting=[lastperiod,periods[-2],periods[-3],periods[-4], codetype]
        '''
        if len(self._periods_required)==5:
            #sorting=['201907']
            percent_chg_cols = [self._periods_required[-3],self._periods_required[-1]]
            print(percent_chg_cols)

            self._periods_required.pop(2)
            column_sort_order = self._periods_required[::-1]
            print(column_sort_order)
        # see the periods of commodity_figures
        periods_incl = set(self._commodity_figures.ReportPeriod)
        periods_lack = []

        # find out the periods not contained in df
        for p in self._periods_required:
            if p not in periods_incl:
                periods_lack.append(p)
        if periods_lack:
            for p in periods_lack:
                self._commodity_figures = self._commodity_figures.append({'ReportPeriod' : p, 'SITC3':'N.A.', 'SITC3_eng_name':'N.A.'} , ignore_index=True)

        #print('test')
        #print(self._commodity_figures)
        result={}
        for tradetype in ['TX','DX','RX','IM']:
            print(tradetype)

            df = pd.pivot_table(self._commodity_figures, values=tradetype, index=['SITC3','SITC3_eng_name'],columns=['ReportPeriod'],\
                 aggfunc=np.sum, fill_value=0, margins=True)\
                 .sort_values(by=column_sort_order,\
                 ascending=False)
            #df.to_excel(f"3_checking{tradetype}_China_bySITC3.xlsx")

            # drop col'All'
            df.drop(['All'], axis=1, inplace=True)
            # copy the row 'All', measure it is on the top row
            #print(df)
            #row_all = pd.DataFrame(df.loc['All'])
            #print(row_all)
            # remove the rows for 'All' and 'N.A.'
            df.drop(['All'], axis=0, inplace=True, level=0)

            if periods_lack: df.drop(['N.A.'], axis=0, inplace=True, level=0)

            #df = row_all.append(df)

            #print('test111')

            #print(row_all)

            #print(df)

            df_top = df.iloc[:topnumber].copy()
            OTHERS = df.iloc[topnumber:].copy().sum()
            #print(df_top)
            #print(OTHERS)
            # combine product of top no. and OTHERS
            df_top.loc[("OTHERS", "OTHERS"),:]=OTHERS
            df = df_top
            #print(df)

            # percentage share of total
            try:
                pct_share = df/df.sum().values*100
            except:
                # if exception occurs, suppose no data for the trade types
                # so there is no 'All' in row 0, just remain df unchanged
                input("pctshare has error, line 105 in BSO_R1 figures.py, press Y to continue")
            #print(pct_share)

            # percentage change of last period
            pct_chg = df[percent_chg_cols].pct_change(axis='columns')*100




            #print(tradetype)
            #print(df)
            result[tradetype] = {'figures': df, 'percent_share':pct_share, 'percent_change':pct_chg}
        #result.to_excel("checking3_tradetype_bySITC3.xlsx")
        return result





if __name__ == '__main__':
    #calculate time spent
    start_time = time.time()
    db_path = "merchandise_trades_DB"
    #periods=(201907, 201807, 201812, 201712, 201612)
    periods=(201612, 201712, 201807, 201812, 201907)

    #periods=(202004, 201904, 201912, 201812, 201712, 201612)

    '''
    gold_hs8_code = ("71081100", "71081210", "71081290", "71081300",
                      "71082010", "71082090", "71090000", "71123000",
                      "71129100", "71189000")
                      '''

    reports = TradeReports(db_path, periods)
    all_figs = reports.all_trades_figures()
    #print(reports.acquire_countries_info())
    print(type(all_figs))
    China=CountryReport(631, periods, **all_figs)
    china_dict = China.trades_byproduct(topnumber=10)

    for k, v in china_dict.items():
        print(k)

        for key, val in v.items():
            print(key)
            print('\n')

            print(val)
            print('\n')




    end_time = time.time()
    elapsed_time = end_time-start_time
    print("time used: ", elapsed_time, " seconds")
