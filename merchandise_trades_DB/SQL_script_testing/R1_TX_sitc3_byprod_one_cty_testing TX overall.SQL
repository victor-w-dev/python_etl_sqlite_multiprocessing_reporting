SELECT CountryConsignmentCode,
       ReportPeriod,
       HScode,
       SITC5,
       substr(SITC5, 1, 3) AS SITC3,
       sum(DomesticExportValueYTD + ReExportValueYTD) AS TX/* sum(DomesticExportValueYTD) AS DX,
       sum(ReExportValueYTD) AS RX,
       sum(ImportValueYTD) AS IM */
  FROM hsccit
       LEFT JOIN
       sitc2hs ON hsccit.HScode = sitc2hs.HS8
 WHERE TransactionType = 1 AND 
       ReportPeriod IN (201907) AND 
       HScode NOT IN ("71081100", "71081210", "71081290", "71081300", "71082010", "71082090", "71090000", "71123000", "71129100", "71189000") AND 
       CountryConsignmentCode = 631
       --and sitc5 is null
group by SITC3