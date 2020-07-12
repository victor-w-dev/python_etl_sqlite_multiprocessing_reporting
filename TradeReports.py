import time
import sqlite3 as pyo
import pandas as pd
import numpy as np
import os
import itertools
import sys
from GeneralTrades_real import GeneralTradesProfile
from CommodityTrades import CommodityTradesProfile
from Output import ExcelOutput
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
        #print(self.all_general_trades)
        #print(self.all_commodity_trades)
        return {'general_trades': self.all_general_trades, 'commodity_trades': self.all_commodity_trades}

    @classmethod
    def acquire_countries_info(cls, *countrycode):
        #self.con = pyo.connect(self._db_path+"/"+"trades.db")
        cls.con = pyo.connect(cls._db_path+"/"+"trades.db")

        country_info_sql = f"""
        SELECT CODE, DESC, CDESC from country
        """

        if countrycode:
            if len(countrycode)==1:
                country_info_sql += f"where CODE = {countrycode[0]}"
            else: country_info_sql += f"where CODE in {countrycode}"

        cls.countries_info_df= pd.read_sql_query(country_info_sql, cls.con)
        #print(cls.countries_info_df)

        return cls.countries_info_df

class CountryReport(object):
    """docstring for ."""
    countryreport_counter = itertools.count(1)
    #@time_decorator
    def __init__(self, country_code, periods_required, toprank = 10, **all_trades_figures):
        self._country_code = country_code
        self._toprank = toprank

        all_general_trades = all_trades_figures['general_trades']
        all_commodity_trades = all_trades_figures['commodity_trades']

        self.general_figures = all_general_trades[all_general_trades.countrycode.isin([country_code])]
        self.commodity_figures = all_commodity_trades[all_commodity_trades.CountryConsignmentCode.isin([country_code])]

        self._periods_required = list(periods_required)

        #self._periods = sorted(set(self._commodity_figures.ReportPeriod))
        #print(self.general_figures)
        #print(self.commodity_figures)
        #print(self._periods_required)
        self._country_name = TradeReports.acquire_countries_info(country_code)['DESC'].values[0]
        print(self._country_name)

    @property
    def periods_lack(self):
        # see the periods of commodity_figures
        periods_incl = set(self.commodity_figures.ReportPeriod)
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



        self.TX = pd.pivot_table(self.commodity_figures, values='TX', index=['SITC3','SITC3_eng_name'],columns=['ReportPeriod'],\
                  aggfunc=np.sum, fill_value=0, margins=True)\
                  .sort_values(by=column_sort_order,\
                  ascending=False)

        #print(self.TX)
        self.TX.to_excel("checking3_TX_China_bySITC3.xlsx")

    def trades_general_dict(self):
        self.general_figures.set_index('ReportPeriod', inplace = True)

        self.general_figures = self.general_figures.T
        #print(self.general_figures)
        #print('test\n\n\n')


        fig = self.general_figures.loc[['TX','DX','RX','IM','RXbyO','TT','TB'],:]
        rank = self.general_figures.loc[['TX_Rank','DX_Rank','RX_Rank','IM_Rank','RXbyO_Rank','TT_Rank'],:]

        # check the country data availability
        if fig.empty:
            print(f"Country Report no.{next(CountryReport.countryreport_counter):03d} has no data")
            return {'figures': pd.DataFrame(np.zeros((len(self._periods_required),7))),
            'rank': pd.DataFrame(np.zeros((6,2))),
            'percent_change': pd.DataFrame(np.zeros((5,3)))
            }
        if self.periods_lack:
            #print("testing_periods_lack\n\n")
            #fig[201907]=np.nan
            #print(type(self.periods_lack))
            for p in self.periods_lack:
                fig[p] = 0
                rank[p]= 0
        #print(fig)
        #print(rank)

        if len(self._periods_required) == 5 and str(self._periods_required[-1])[-2:] != '12':
            fig_adjusted = fig[self._periods_required[:2]+self._periods_required[3:5]]
            rank_adjusted = rank[self._periods_required[3:5]]
        #print(fig_adjusted)
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
            print(percent_chg_cols)

            column_sort_order = [*self._periods_required[-1:-3:-1],*self._periods_required[-4::-1]]
            print(f"column sort order: {column_sort_order}")
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
            print(tradetype)

            df = pd.pivot_table(self.commodity_figures, values=tradetype, index=['SITC3','SITC3_eng_name'],columns=['ReportPeriod'],\
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
            print(f"test here!!!!\n\n")
            print(df)
            print("see")
            pct_chg = df[percent_chg_cols].pct_change(axis='columns')*100



            #print(tradetype)
            #print(df)
            result[tradetype] = {'figures': df, 'percent_share':pct_share, 'percent_change':pct_chg}
        #result.to_excel("checking3_tradetype_bySITC3.xlsx")
        return result

    @time_decorator
    def report_to_excel(self):
        report = ExcelOutput(self._country_name, self._periods_required, self.trades_general_dict(), self.trades_byproduct(), "Country", currency='HKD',money='MN')
        #report.create_and_change_path()
        #report.money_conversion()
        report.part1_toexcel_generaltrade()
        #print(report.trades_byproduct_dict)
        #return report.trades_byproduct_dict



if __name__ == '__main__':
    #calculate time spent
    start_time = time.time()
    #periods=(201907, 201807, 201812, 201712, 201612)
    periods=(201612, 201712, 201807, 201812, 201907)

    #periods=(202004, 201904, 201912, 201812, 201712, 201612)

    '''
    gold_hs8_code = ("71081100", "71081210", "71081290", "71081300",
                      "71082010", "71082090", "71090000", "71123000",
                      "71129100", "71189000")
                      '''

    reports = TradeReports(periods)
    #print(TradeReports.__dict__)
    all_figs = reports.all_trades_figures()
    #reports.acquire_countries_info(111,811,631,191)
    for row in reports.acquire_countries_info().itertuples():

        try:
            CountryReport(row.CODE, periods, toprank = 10, **all_figs).report_to_excel()
        except:
            print(f"{row.CODE} {row.DESC} has error")
            f = open(f"{row.CODE} {row.DESC}.txt", "w")
            f.write(str(sys.exc_info()[0]))
            f.write(str(sys.exc_info()[1]))
            f.close()
            #continue
        #print(type(all_figs))
    '''
    for cty in [199,695,883]:
        report = CountryReport(cty, periods, toprank = 10, **all_figs)
        print(report.periods_lack)
        report.report_to_excel()

    R1=CountryReport(111, periods, toprank = 10, **all_figs)
    R1.report_to_excel()

    R2=CountryReport(811, periods, toprank = 10, **all_figs)
    #print(China.trades_general_dict())
    R2.report_to_excel()


    for k, v in china_dict.items():
        print(k)

        for key, val in v.items():
            print(key)
            print('\n')

            print(val)
            print('\n')

    '''


    end_time = time.time()
    elapsed_time = end_time-start_time
    print("time used: ", elapsed_time, " seconds")
