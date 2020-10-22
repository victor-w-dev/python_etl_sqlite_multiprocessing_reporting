SELECT ReportPeriod,
       CountryConsignmentCode,
       HScode,
       SITC5,
       (DomesticExportValueYTD + ReExportValueYTD) AS TX
  FROM hsccit
       LEFT JOIN
       sitc2hs ON hsccit.HScode = sitc2hs.HS8
 WHERE TransactionType = 1 AND 
       ReportPeriod IN (201812) AND 
       HScode NOT IN ("71081100", "71081210", "71081290", "71081300", "71082010", "71082090", "71090000", "71123000", "71129100", "71189000") AND 
       CountryConsignmentCode = 631;
