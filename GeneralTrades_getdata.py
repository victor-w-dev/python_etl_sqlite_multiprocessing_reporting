import pandas as pd
import time
import os
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
            data_p= pd.read_sql_query(general_trades_sql, self.con)
            result.append(data_p)
            self.df=pd.concat(result)
        self.con.close()

        return self.df

    @time_decorator
    def get_figures_1lot(self):
        self.con = pyo.connect(self._db_path+"/"+"trades.db")

        result = []
        general_trades_sql = f"""
                WITH A AS (
            SELECT ReportPeriod,
                   CountryConsignmentCode countrycode,
                   sum(DomesticExportValueYTD) AS DX,
                   sum(ReExportValueYTD) AS RX,
                   sum(DomesticExportValueYTD + ReExportValueYTD) AS TX,
                   sum(ImportValueYTD) AS IM,
                   sum(DomesticExportValueYTD + ReExportValueYTD + ImportValueYTD) AS TT
              FROM hsccit
             WHERE TransactionType = 1 AND
                   ReportPeriod IN {self._periods} AND
                   hscode NOT IN ("71081100", "71081210", "71081290", "71081300", "71082010", "71082090", "71090000", "71123000", "71129100", "71189000")
             GROUP BY CountryConsignmentCode,
                      ReportPeriod
        ),
        B AS (
            SELECT ReportPeriod,
                   CountryOriginCode countrycode,
                   sum(ReExportValueYTD) AS RXbyO
              FROM hscoccit
             WHERE TransactionType = 1 AND
                   ReportPeriod IN {self._periods} AND
                   hscode NOT IN ("71081100", "71081210", "71081290", "71081300", "71082010", "71082090", "71090000", "71123000", "71129100", "71189000")
             GROUP BY CountryOriginCode,
                      ReportPeriod
        ),
        C AS (
            SELECT A.*,
                   B.RXbyO
              FROM A
                   LEFT JOIN
                   B ON A.countrycode = B.countrycode AND
                        A.ReportPeriod = B.ReportPeriod
            UNION
            SELECT B.ReportPeriod,
                   B.countrycode,
                   ifnull(DX, 0),
                   ifnull(RX, 0),
                   ifnull(TX, 0),
                   ifnull(IM, 0),
                   ifnull(TT, 0),
                   ifnull(RXbyO, 0)
              FROM B
                   LEFT JOIN
                   A ON A.countrycode = B.countrycode AND
                        A.ReportPeriod = B.ReportPeriod
        )
        SELECT ReportPeriod,
               countrycode,
               DESC,
               DX,
               RX,
               TX,
               IM,
               TT,
               RXbyO,
               TX - IM TB,
              RANK () OVER (
              PARTITION BY ReportPeriod
              ORDER BY TX DESC
              ) TX_Rank ,
              RANK () OVER (
              PARTITION BY ReportPeriod
              ORDER BY DX DESC
              ) DX_Rank,
              RANK () OVER (
              PARTITION BY ReportPeriod
              ORDER BY RX DESC
              ) RX_Rank,
              RANK () OVER (
              PARTITION BY ReportPeriod
              ORDER BY IM DESC
              ) IM_Rank,
              RANK () OVER (
              PARTITION BY ReportPeriod
              ORDER BY RXbyO DESC
              ) RXbyO_Rank,
              RANK () OVER (
              PARTITION BY ReportPeriod
              ORDER BY TT DESC
              ) TT_Rank,
              RANK () OVER (
              PARTITION BY ReportPeriod
              ORDER BY (TX - IM) DESC
              ) TB_Rank

          FROM C
          LEFT JOIN
          country ON C.countrycode = country.CODE;

          """

        data_p= pd.read_sql_query(general_trades_sql, self.con)
        result.append(data_p)
        df=pd.concat(result)
        df.fillna(0.0, inplace = True)
        return df

    #@time_decorator
    def get_figures_single_period(self, period):
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
        return pd.DataFrame(pd.read_sql_query(general_trades_sql, self.con)).fillna(0, inplace = True)

if __name__ == '__main__':
    #calculate time spent
    start_time = time.time()
    db_path = "merchandise_trades_DB"
    periods=(201907, 201807, 201812, 201712, 201612, 201512)

    print("GeneralTradeProfile:")

    profile = GeneralTradesProfile(db_path, periods)
    profile.get_figures_1lot().to_excel("checking1_general_trade.xlsx")

    end_time = time.time()
    elapsed_time = end_time-start_time
    print("time used: ", elapsed_time, " seconds")
