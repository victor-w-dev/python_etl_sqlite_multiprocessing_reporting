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
                  ReportPeriod IN (201907, 201807, 201812, 201712, 201612, 201512) AND 
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
                  ReportPeriod IN (201907, 201807, 201812, 201712, 201612, 201512) AND 
                  hscode NOT IN ("71081100", "71081210", "71081290", "71081300", "71082010", "71082090", "71090000", "71123000", "71129100", "71189000") 
            GROUP BY CountryOriginCode,
                     ReportPeriod
       )
       B ON A.CountryConsignmentCode = B.CountryOriginCode AND 
            A.ReportPeriod = B.ReportPeriod;
