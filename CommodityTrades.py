import pandas as pd
import time
import os
import sqlite3 as pyo
import datetime
import multiprocessing as mp
from BSO.time_analysis import time_decorator

class CommodityTradesProfile():
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

    @time_decorator
    def get_commodity_trade(self):
        self.con = pyo.connect(self._db_path+"/"+"trades.db")

        result = []
        general_trades_sql = f"""
        WITH trade AS (
            SELECT CountryConsignmentCode,
                   ReportPeriod,
                   HScode,
                   SITC5,
                   substr(SITC5, 1, 3) AS SITC3,
                   sum(DomesticExportValueYTD + ReExportValueYTD) AS TX,
                   sum(DomesticExportValueYTD) AS DX,
                   sum(ReExportValueYTD) AS RX,
                   sum(ImportValueYTD) AS IM
              FROM hsccit
                   LEFT JOIN
                   sitc2hs ON hsccit.HScode = sitc2hs.HS8
             WHERE TransactionType = 1 AND
                   ReportPeriod IN (201907, 201807, 201812, 201712, 201612) AND
                   HScode NOT IN ("71081100", "71081210", "71081290", "71081300", "71082010", "71082090", "71090000", "71123000", "71129100", "71189000")-- AND
             /* CountryConsignmentCode = 631 */GROUP BY ReportPeriod,
                      CountryConsignmentCode,
                      SITC3
        )
        SELECT *
          FROM (
                   SELECT CountryConsignmentCode,
                          ReportPeriod,
                          HScode,
                          SITC3,
                          sitc_english_description AS SITC3_eng_name,
        				  TX,
        				  DX,
        				  RX,
        				  IM

                     FROM trade
                          LEFT JOIN
                          sitc_item ON trade.SITC3 = sitc_item.SITC_Code
                    WHERE sitc_item.SITC_Code_Level = 3
                    ORDER BY CountryConsignmentCode,
                             ReportPeriod
               );

        """
        #print(general_trades_sql)
        data_p= pd.read_sql_query(general_trades_sql, self.con)
        #print(profile.get_figures(db_path=db_path))
        result.append(data_p)
        df=pd.concat(result)
        #print(self.df)

        return df




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

    print("TradeCommodity:")

    profile = CommodityTradesProfile(db_path, periods)

    #profile = GeneralTradeProfile(201712,201812)
    #print(profile.get_figures((201907,)))
    profile.get_commodity_trade().to_excel("checking2_trades_type_SITC3.xlsx")
    #profile.con.close()
    #profile.get_figures()#.to_excel("checking.xlsx")
    #profile.get_figures_multiprocessing()#.to_excel("checking.xlsx")

    end_time = time.time()
    elapsed_time = end_time-start_time
    print("time used: ", elapsed_time, " seconds")
