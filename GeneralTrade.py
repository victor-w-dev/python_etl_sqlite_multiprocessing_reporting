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


db_path = "merchandise_trades_DB"
gold_hs8_code = ("71081100", "71081210", "71081290", "71081300",
                  "71082010", "71082090", "71090000", "71123000",
                  "71129100", "71189000")

periods='201812, 201712, 201612, 201512'
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
                  ReportPeriod IN ({periods}) AND
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
                  ReportPeriod IN ({periods}) AND
                  hscode NOT IN ("71081100", "71081210", "71081290", "71081300", "71082010", "71082090", "71090000", "71123000", "71129100", "71189000")
            GROUP BY CountryOriginCode,
                     ReportPeriod
       )
       B ON A.CountryConsignmentCode = B.CountryOriginCode AND
            A.ReportPeriod = B.ReportPeriod;
"""



class GeneralTradeProfile():
    def __init__(self, startperiod, endperiod):
        self.start = startperiod
        self.end = endperiod

    def get_figures(self,db_path):
        self.con = pyo.connect(db_path+"/"+"trades.db")
        #self.cursor = self.con.cursor()
        #self.cursor.execute(general_trades_sql)
        #rows = self.cursor.fetchall()
        print(general_trades_sql)
        self.df = pd.read_sql_query(general_trades_sql, self.con)
        return self.df





if __name__ == '__main__':
    #calculate time spent
    start_time = time.time()
    print("GeneralTradeProfile:")
    profile = GeneralTradeProfile(2017,2018)
    print(profile.get_figures(db_path=db_path))

    profile.get_figures(db_path=db_path).to_excel("checking.xlsx")


    end_time = time.time()
    elapsed_time = end_time-start_time
    print("time used: ", elapsed_time, " seconds")
