SELECT A.ReportPeriod,
       CountryConsignmentCode,
       CountryOriginCode,
       country_name,
       DX,
       RX,
       B.RXbyO,
       TX,
       IM,
       TT
  FROM (
           SELECT ReportPeriod,
                  CountryConsignmentCode,
                  sum(DomesticExportValueYTD) AS DX,
                  sum(ReExportValueYTD) AS RX,
                  sum(DomesticExportValueYTD + ReExportValueYTD) AS TX,
                  sum(ImportValueYTD) AS IM,
                  sum(DomesticExportValueYTD + ReExportValueYTD + ImportValueYTD) AS TT,
                  sum(ReExportValueYTD) AS RXbyO
             FROM hsccit
            WHERE TransactionType = 1 AND 
                  ReportPeriod IN (201812/* , 201712, 201612, 201512 */) AND 
                  hscode NOT IN ("71081100", "71081210", "71081290", "71081300", "71082010", "71082090", "71090000", "71123000", "71129100", "71189000") 
            GROUP BY CountryConsignmentCode
       )
       A
       LEFT JOIN
       (
           SELECT ReportPeriod,
                  CountryOriginCode,
                  country.[DESC] AS country_name,
                  sum(ReExportValueYTD) AS RXbyO
             FROM hscoccit
                  LEFT JOIN
                  country ON hscoccit.CountryOriginCode = country.CODE
            WHERE TransactionType = 1 AND 
                  ReportPeriod IN (201812/* , 201712, 201612, 201512 */) AND 
                  hscode NOT IN ("71081100", "71081210", "71081290", "71081300", "71082010", "71082090", "71090000", "71123000", "71129100", "71189000") 
            GROUP BY CountryOriginCode
       )
       B ON A.CountryConsignmentCode = B.CountryOriginCode;
