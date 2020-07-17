import time
import sqlite3 as pyo
import pandas as pd
import numpy as np
import os
import itertools
from functools import partial
import sys
import multiprocessing
from GeneralTrades_getdata import GeneralTradesProfile
from CommodityTrades_getdata import CommodityTradesProfile
from Output import ExcelOutput
from BSO.create_R1_periods import R1_periods
from BSO.time_analysis import time_decorator

class TradeReports(object):
    """docstring for ."""
    _db_path = "merchandise_trades_DB"

    def __init__(self, periods):
        self._periods = periods

    @property
    def periods(self):
        return self._periods

    @periods.setter
    def periods(self, periods):
        if len(periods) > 5:
            raise ValueError('length of tuple of periods must be less than or equal to 5')
        if not isinstance(periods, tuple):
            raise TypeError('periods must be tuple')
        self._periods = periods

    def all_trades_figures(self):
        self.all_general_trades = GeneralTradesProfile(TradeReports._db_path, self._periods).get_figures_1lot()
        self.all_commodity_trades = CommodityTradesProfile(TradeReports._db_path, self._periods).get_commodity_trade()
        return {'general_trades': self.all_general_trades, 'commodity_trades': self.all_commodity_trades}

    @classmethod
    def acquire_countries_info(cls, *countrycode):
        cls.con = pyo.connect(cls._db_path+"/"+"trades.db")

        country_info_sql = f"""
        SELECT CODE, DESC, CDESC from country
        """

        if countrycode:
            if len(countrycode)==1:
                country_info_sql += f"where CODE = {countrycode[0]}"
            else: country_info_sql += f"where CODE in {countrycode}"

        cls.countries_info_df= pd.read_sql_query(country_info_sql, cls.con)

        return cls.countries_info_df

class CountryReport(object):
    """docstring for ."""
    countryreport_counter = itertools.count(1)
    #@time_decorator
    def __init__(self, all_trades_figures_dict, periods_required, toprank = 10, country_code=None):

        self._report_number = next(CountryReport.countryreport_counter)
        self._country_code = country_code
        self._toprank = toprank

        all_general_trades = all_trades_figures_dict['general_trades']
        all_commodity_trades = all_trades_figures_dict['commodity_trades']

        self.general_figures = all_general_trades.loc[all_general_trades.countrycode.isin([country_code]),:]
        self.commodity_figures = all_commodity_trades[all_commodity_trades.CountryConsignmentCode.isin([country_code])]

        # preprocessing the general_figures
        self.general_figures.set_index('ReportPeriod', inplace=True)
        self.general_figures = self.general_figures.T
        self._periods_required = list(periods_required)
        self._country_name = TradeReports.acquire_countries_info(country_code)['DESC'].values[0]
        print(f"no.{self._report_number:03d} {country_code} {self._country_name} exporting report")
        self.report_to_excel()

    @property
    def periods_lack(self):
        print(list(self.general_figures.columns))
        # see the periods of commodity_figures
        periods_incl = list(self.general_figures.columns)
        # find out the periods not contained in df
        self._periods_lack = [p for p in self._periods_required if p not in periods_incl]
        return self._periods_lack

    @time_decorator
    def TX_byproduct(self):
        '''
        if len(periods)==4:
            sorting=[lastperiod,periods[-2],periods[-3],periods[-4], codetype]
        '''
        if len(self._periods)==5:
            #sorting=['201907']
            column_sort_order = tuple(self._periods)#[::-1]

        self.TX = pd.pivot_table(self.commodity_figures, values='TX', index=['SITC3','SITC3_eng_name'],columns=['ReportPeriod'],\
                  aggfunc=np.sum, fill_value=0, margins=True)\
                  .sort_values(by=column_sort_order,\
                  ascending=False)

        self.TX.to_excel("checking3_TX_China_bySITC3.xlsx")

    def trades_general_dict(self):

        fig = self.general_figures.loc[['TX','DX','RX','IM','RXbyO','TT','TB'],:].astype('int64')
        rank = self.general_figures.loc[['TX_Rank','DX_Rank','RX_Rank','IM_Rank','RXbyO_Rank','TT_Rank'],:].astype('int64')
        # check the country data availability
        if fig.empty:
            print(f"Country Report no.{self._report_number:03d} has no data")
            return {'figures': pd.DataFrame(np.zeros((len(self._periods_required),7))),
            'rank': pd.DataFrame(np.zeros((6,2))),
            'percent_change': pd.DataFrame(np.zeros((5,3)))
            }
        if self.periods_lack:
            print("testing_periods_lack\n\n")
            print(self.periods_lack)

            for p in self.periods_lack:
                fig[p] = 0
                rank[p]= 0
        # make sure columns in ascending order
        fig.sort_index(axis=1, inplace=True)
        rank.sort_index(axis=1, inplace=True)

        if len(self._periods_required) == 5 and str(self._periods_required[-1])[-2:] != '12':
            fig_adjusted = fig[self._periods_required[:2]+self._periods_required[3:5]]
            rank_adjusted = rank[self._periods_required[3:5]]
        else:
            fig_adjusted = fig
            rank_adjusted = rank[self._periods_required[-2:]]
        #print(rank_adjusted)
        return {'figures': fig_adjusted, 'rank': rank_adjusted, 'percent_change': CountryReport.percent_change(fig[:-1])}

    def percent_change(data):
        periods=data.columns
        #print(periods)
        # for data combined with yearly and monthly(year to date)
        if int(str(periods[-1])[-2:])!=12:
            year=data.iloc[:,[0,1,3]].pct_change(axis='columns')
            ytd=data.iloc[:,[2,4]].pct_change(axis='columns')
            tablepcc=pd.concat([year,ytd],axis=1)
            tablepcc=tablepcc.iloc[:,[1,2,4]]
        # for data yearly only
        elif int(str(periods[-1])[-2:])==12:
            tablepcc=data.pct_change(axis='columns')
            tablepcc=tablepcc.iloc[:,[1,2,3]]

        # change pecentage columns name
        tablepcc.columns = [str(c)+"_% CHG" for c in tablepcc.columns]
        # make percentage times 100
        tablepcc*=100

        #print(tablepcc,'\n')
        # table_result = tablepcc.dropna(axis='columns', how='all')
        return tablepcc

    #@time_decorator
    def trades_byproduct(self):
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
        topnumber = self._toprank

        if len(self._periods_required)==5:
            #sorting=['201907']
            percent_chg_cols = [self._periods_required[-3],self._periods_required[-1]]
            #print(percent_chg_cols)

            column_sort_order = [*self._periods_required[-1:-3:-1],*self._periods_required[-4::-1]]
        else:
            percent_chg_cols = self._periods_required[-2:]
            column_sort_order = self._periods_required[::-1]
        #print(f"column sort order: {column_sort_order}")
        # see the periods of commodity_figures
        periods_incl = set(self.commodity_figures.ReportPeriod)
        periods_lack = []

        # find out the periods not contained in df
        for p in self._periods_required:
            if p not in periods_incl:
                periods_lack.append(p)
        if periods_lack:
            for p in periods_lack:
                self.commodity_figures = self.commodity_figures.append({'ReportPeriod' : p, 'SITC3':'N.A.', 'SITC3_eng_name':'N.A.'} , ignore_index=True)

        #print('test')
        #print(self.commodity_figures)
        result={}
        for tradetype in ['TX','DX','RX','IM']:
            #print(tradetype)

            df = pd.pivot_table(self.commodity_figures, values=tradetype, index=['SITC3','SITC3_eng_name'],columns=['ReportPeriod'],\
                 aggfunc=np.sum, fill_value=0, margins=True)\
                 .sort_values(by=column_sort_order,\
                 ascending=False)
            df.drop(['All'], axis=1, inplace=True)
            # copy the row 'All', measure it is on the top row
            #print(df)
            #row_all = pd.DataFrame(df.loc['All'])
            #print(row_all)
            # remove the rows for 'All' and 'N.A.'
            df.drop(['All'], axis=0, inplace=True, level=0)

            if periods_lack: df.drop(['N.A.'], axis=0, inplace=True, level=0)

            df_top = df.iloc[:topnumber].copy()
            OTHERS = df.iloc[topnumber:].copy().sum()
            #print(df_top)
            #print(OTHERS)
            # combine product of top no. and OTHERS
            df_top.loc[("OTHERS", "OTHERS"),:]=OTHERS
            df = df_top
            #print(df)
            if len(self._periods_required) == 5 and str(self._periods_required[-1])[-2:] != '12':
                df_adjusted = df[self._periods_required[:2]+self._periods_required[3:5]]
            else:
                df_adjusted = df
            # percentage share of total
            try:
                pct_share = df_adjusted/df_adjusted.sum().values*100
            except:
                # if exception occurs, suppose no data for the trade types
                # so there is no 'All' in row 0, just remain df unchanged
                raise ValueError("pct_share has problems.")
            #print(pct_share)
            #print(df_adjusted)
            pct_chg = df[percent_chg_cols].pct_change(axis='columns')*100
            result[tradetype] = {'figures': df_adjusted, 'percent_share':pct_share, 'percent_change':pct_chg}
        return result

    @time_decorator
    def report_to_excel(self):
        general_dict = self.trades_general_dict()
        byproduct_dict = self.trades_byproduct()
        f = partial(ExcelOutput,self._country_name, self._periods_required, self._toprank, general_dict, byproduct_dict, "Country")
        for i, currency in enumerate(['HKD','USD']):
          for j, money in enumerate(['TH','MN']):
                f(currency, money).export_results()

if __name__ == '__main__':
    #calculate time spent
    start_time = time.time()
    #periods=(201612, 201712, 201807, 201812, 201907)
    #periods=(201612, 201712, 201812,201912)
    #periods=(201512, 201612, 201712,201812)
    last_period = int(input("Please Enter Report Lastperiod in YYYYMM format: "))

    periods = R1_periods(last_period)


    '''
    gold_hs8_code = ("71081100", "71081210", "71081290", "71081300",
                      "71082010", "71082090", "71090000", "71123000",
                      "71129100", "71189000")
                      '''

    reports = TradeReports(periods)
    all_figs = reports.all_trades_figures()

    for row in reports.acquire_countries_info().itertuples():
        #if row.CODE in [199,695,883,631]:
        #if row.CODE == 631:
            try:
                R = CountryReport(all_figs, periods, 10, row.CODE)

            except:
                print(f"{row.CODE} {row.DESC} has error\n")
                f = open(f"{row.CODE} {row.DESC.replace('/',',')}.txt", "w")
                f.write(str(sys.exc_info()[0]))
                f.write(str(sys.exc_info()[1]))
                f.close()

    end_time = time.time()
    elapsed_time = end_time-start_time
    print("time used: ", elapsed_time, " seconds")
