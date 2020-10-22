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
           ReportPeriod IN (201907, 201807, 201812, 201712, 201612) AND 
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
           ReportPeriod IN (201907, 201807, 201812, 201712, 201612, 201512) AND 
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
       TX - IM TB
  FROM C
       LEFT JOIN
       country ON C.countrycode = country.CODE;

  
/* and CountryConsignmentCode = 199 */;
