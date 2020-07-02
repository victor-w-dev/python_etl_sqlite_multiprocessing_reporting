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
    def __init__(self, country_code, **all_trades_figures):
        self._country_code = country_code
        all_general_trades = all_trades_figures['general_trades']
        all_commodity_trades = all_trades_figures['commodity_trades']

        self._general_figures = all_general_trades[all_general_trades.countrycode.isin([country_code])]
        self._commodity_figures = all_commodity_trades[all_commodity_trades.CountryConsignmentCode.isin([country_code])]
        print(self._general_figures)
        print(self._commodity_figures)

    @time_decorator
    def TX(self):
        '''
        if len(periods)==4:
            sorting=[lastperiod,periods[-2],periods[-3],periods[-4], codetype]
        '''
        if len(set(self._commodity_figures.ReportPeriod))==5:
            sorting=['201907']

        self.TX = pd.pivot_table(self._commodity_figures, values='TX', index=['SITC3','SITC3_eng_name'],columns=['ReportPeriod'],\
                  aggfunc=np.sum, fill_value=0, margins=True)\
                  .sort_values(by=sorting,\
                  ascending=False)
        print(self.TX)
        self.TX.to_excel("checking3_TX_China_bySITC3.xlsx")
    def DX(self):
        pass
    def RX(self):
        pass
    def IM(self):
        pass


if __name__ == '__main__':
    #calculate time spent
    start_time = time.time()
    db_path = "merchandise_trades_DB"
    periods=(201907, 201807, 201812, 201712, 201612, 201512)
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
    China=CountryReport(631, **all_figs)
    China.TX()

    end_time = time.time()
    elapsed_time = end_time-start_time
    print("time used: ", elapsed_time, " seconds")
