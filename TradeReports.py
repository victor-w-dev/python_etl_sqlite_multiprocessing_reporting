import time
import sqlite3 as pyo
import pandas as pd
from GeneralTrades_real import GeneralTradesProfile
from CommodityTrades import CommodityTradesProfile
#from BSO.time_analysis import time_decorator

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

    def acquire_trades_figures(self):
        self.general_trades = GeneralTradesProfile(self._db_path, self.periods).get_figures_1lot()
        self.commodity_trades = CommodityTradesProfile(self._db_path, self.periods).get_commodity_trade()
        return {'general_trades': self.general_trades, 'commodity_trades': self.commodity_trades}

    def acquire_country_dict(self):
        self.con = pyo.connect(self._db_path+"/"+"trades.db")

        result = []
        country_info_sql = f"""
        SELECT CODE, DESC, CDESC from country
        """
        country_info_df= pd.read_sql_query(country_info_sql, self.con)
        #print(self.df)

        return country_info_df

class CountryReports(object):
    """docstring for ."""

    def __init__(self, arg):
        self.arg = arg

if __name__ == '__main__':
    #calculate time spent
    start_time = time.time()
    db_path = "merchandise_trades_DB"
    #periods=(201907,)#, 201807, 201812, 201712, 201612, 201512)
    periods=(202004, 201904, 201912, 201812, 201712, 201612)

    '''
    gold_hs8_code = ("71081100", "71081210", "71081290", "71081300",
                      "71082010", "71082090", "71090000", "71123000",
                      "71129100", "71189000")
                      '''

    reports = TradeReports(db_path, periods)
    reports.acquire_trades_figures()
    print(reports.acquire_country_dict())


    end_time = time.time()
    elapsed_time = end_time-start_time
    print("time used: ", elapsed_time, " seconds")
