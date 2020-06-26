import pandas as pd
import time
#import pyprind
import os
#from BSO.merge import mergedf_multiprocessing as mergedf
#from BSO.R1_figures import country_R1_fig, major_commodity_fig
#import export.export_file as ex
#import multiprocessing
import sqlite3 as pyo
import datetime

class GeneralTradeProfile():
    def __init__(self, periods):
        self._periods = periods

    @property
    def periods(self):
        return self._periods

    @periods.setter
    def periods(self, periods):
        if isinstance(periods, tuple):
            self_periods = periods
        else:
            raise TypeError('periods must be tuple')

    def get_figures(self,db_path):
        self.con = pyo.connect(db_path+"/"+"trades.db")

        general_trades_sql = f"""
        SELECT A.ReportPeriod,
               CountryConsignmentCode countrycode,
               country_name,
               TX,
               DX,
               RX,
               IM,
               B.RXbyO,
               TT,
               TX - IM TB
          FROM (
                   SELECT ReportPeriod,
                          CountryConsignmentCode,
                          country.[DESC] AS country_name,
                          sum(DomesticExportValueYTD) AS DX,
                          sum(ReExportValueYTD) AS RX,
                          sum(DomesticExportValueYTD + ReExportValueYTD) AS TX,
                          sum(ImportValueYTD) AS IM,
                          sum(DomesticExportValueYTD + ReExportValueYTD + ImportValueYTD) AS TT,
                          sum(ReExportValueYTD) AS RXbyO
                     FROM hsccit
                          LEFT JOIN
                          country ON CountryConsignmentCode = country.CODE
                    WHERE TransactionType = 1 AND
                          ReportPeriod IN {self.periods} AND
                          hscode NOT IN ("71081100", "71081210", "71081290", "71081300", "71082010", "71082090", "71090000", "71123000", "71129100", "71189000")
                    GROUP BY CountryConsignmentCode,
                             ReportPeriod
               )
               A
               LEFT JOIN
               (
                   SELECT ReportPeriod,
                          CountryOriginCode,
                          sum(ReExportValueYTD) AS RXbyO
                     FROM hscoccit
                    WHERE TransactionType = 1 AND
                          ReportPeriod IN {self.periods} AND
                          hscode NOT IN ("71081100", "71081210", "71081290", "71081300", "71082010", "71082090", "71090000", "71123000", "71129100", "71189000")
                    GROUP BY CountryOriginCode,
                             ReportPeriod
               )
               B ON A.CountryConsignmentCode = B.CountryOriginCode AND
                    A.ReportPeriod = B.ReportPeriod;
        """

        #print(general_trades_sql)
        self.df = pd.read_sql_query(general_trades_sql, self.con)
        return self.df





if __name__ == '__main__':
    #calculate time spent
    start_time = time.time()
    db_path = "merchandise_trades_DB"
    periods=(201907, 201807, 201812, 201712, 201612, 201512)

    '''
    gold_hs8_code = ("71081100", "71081210", "71081290", "71081300",
                      "71082010", "71082090", "71090000", "71123000",
                      "71129100", "71189000")
    '''

    print("GeneralTradeProfile:")
    profile = GeneralTradeProfile(periods)
    print(profile.get_figures(db_path=db_path))

    profile.get_figures(db_path=db_path).to_excel("checking.xlsx")


    end_time = time.time()
    elapsed_time = end_time-start_time
    print("time used: ", elapsed_time, " seconds")
