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
import multiprocessing as mp
from BSO.time_analysis import time_decorator

class GeneralTradesProfile():
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
    def get_figures(self):
        self.con = pyo.connect(self._db_path+"/"+"trades.db")

        result = []
        for p in self._periods:
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
                              ReportPeriod IN ({p}) AND
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
                              ReportPeriod IN ({p}) AND
                              hscode NOT IN ("71081100", "71081210", "71081290", "71081300", "71082010", "71082090", "71090000", "71123000", "71129100", "71189000")
                        GROUP BY CountryOriginCode,
                                 ReportPeriod
                   )
                   B ON A.CountryConsignmentCode = B.CountryOriginCode AND
                        A.ReportPeriod = B.ReportPeriod;
            """
            #print(general_trades_sql)
            data_p= pd.read_sql_query(general_trades_sql, self.con)
            #print(profile.get_figures(db_path=db_path))
            result.append(data_p)
            self.df=pd.concat(result)
        #print(self.df)
        self.con.close()

        return self.df

    @time_decorator
    def get_figures_1lot(self):
        self.con = pyo.connect(self._db_path+"/"+"trades.db")

        result = []
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
               TX - IM TB,
               RANK () OVER (
                PARTITION BY A.ReportPeriod
                ORDER BY TX DESC
                ) TX_Rank ,
                RANK () OVER (
                PARTITION BY A.ReportPeriod
                ORDER BY DX DESC
                ) DX_Rank,
                RANK () OVER (
                PARTITION BY A.ReportPeriod
                ORDER BY RX DESC
                ) RX_Rank,
                RANK () OVER (
                PARTITION BY A.ReportPeriod
                ORDER BY IM DESC
                ) IM_Rank,
                RANK () OVER (
                PARTITION BY A.ReportPeriod
                ORDER BY B.RXbyO DESC
                ) RXbyO_Rank,
                RANK () OVER (
                PARTITION BY A.ReportPeriod
                ORDER BY TT DESC
                ) TT_Rank,
                RANK () OVER (
                PARTITION BY A.ReportPeriod
                ORDER BY (TX - IM) DESC
                ) TB_Rank


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
                          ReportPeriod IN {self._periods} AND
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
                          ReportPeriod IN {self._periods} AND
                          hscode NOT IN ("71081100", "71081210", "71081290", "71081300", "71082010", "71082090", "71090000", "71123000", "71129100", "71189000")
                    GROUP BY CountryOriginCode,
                             ReportPeriod
               )
               B ON A.CountryConsignmentCode = B.CountryOriginCode AND
                    A.ReportPeriod = B.ReportPeriod;
        """
        #print(general_trades_sql)
        data_p= pd.read_sql_query(general_trades_sql, self.con)
        #print(profile.get_figures(db_path=db_path))
        result.append(data_p)
        df1=pd.concat(result)
        #print(self.df)

        return df1

    #@time_decorator
    def get_figures_single_period(self, period):
        #print('here')
        self.con = pyo.connect(self._db_path+"/"+"trades.db")

        result = []
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
                          ReportPeriod IN ({period}) AND
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
                          ReportPeriod IN ({period}) AND
                          hscode NOT IN ("71081100", "71081210", "71081290", "71081300", "71082010", "71082090", "71090000", "71123000", "71129100", "71189000")
                    GROUP BY CountryOriginCode,
                             ReportPeriod
               )
               B ON A.CountryConsignmentCode = B.CountryOriginCode AND
                    A.ReportPeriod = B.ReportPeriod;
        """
        #print(general_trades_sql)
        #self.con.close()

        return pd.DataFrame(pd.read_sql_query(general_trades_sql, self.con))
        #print(profile.get_figures(db_path=db_path))
        #result.append(data_p)
        #self.df=pd.concat(result)
        #print(self.df)


    @time_decorator
    def get_figures_multiprocessing(self):
        #self.con = pyo.connect(self._db_path+"/"+"trades.db")

        pool = mp.Pool(processes = mp.cpu_count())
        jobs = []

        #queries=[]
        for p in self._periods:
            #queries.append(p)
            jobs.append(pool.apply_async(self.get_figures_single_period, (p,)))
            #print(general_trades_sql)
            #data_p= pd.read_sql_query(general_trades_sql, self.con)
            #print(profile.get_figures(db_path=db_path))
            #result.append(data_p)
            #self.df=pd.concat(result)
            #print(self.df)
            #return self.df

        #wait for all jobs to finish
            #print(job.get())
        df = pd.concat([job.get() for job in jobs])
        #clean up
        pool.close()
        pool.join()
        return df



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

    print("GeneralTradeProfile:")

    profile = GeneralTradesProfile(db_path, periods)

    #profile = GeneralTradeProfile(201712,201812)
    #print(profile.get_figures((201907,)))
    profile.get_figures_1lot().to_excel("checking1_general_trade.xlsx")
    #profile.con.close()
    #profile.get_figures()#.to_excel("checking.xlsx")
    #profile.get_figures_multiprocessing()#.to_excel("checking.xlsx")

    end_time = time.time()
    elapsed_time = end_time-start_time
    print("time used: ", elapsed_time, " seconds")
