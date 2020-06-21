SELECT --TransactionType,
       --hscode,
       ReportPeriod,
       sum(DomesticExportValueYTD) AS DX,
       sum(ReExportValueYTD) AS RX,
       sum(DomesticExportValueYTD + ReExportValueYTD) AS TX
      
  FROM hsccit
 WHERE TransactionType = 1 AND 
       --ReportPeriod IN (201812, 201712, 201612, 201512) AND 
       hscode NOT IN ("71081100", "71081210", "71081290", "71081300", "71082010", "71082090", "71090000", "71123000", "71129100", "71189000") 
 GROUP BY ReportPeriod;
