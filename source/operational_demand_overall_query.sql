WITH DF AS (

select INTERVAL_DATETIME, REGIONID, OPERATIONAL_DEMAND, LASTCHANGED, MONTH(INTERVAL_DATETIME) as "MONTH"
FROM  mms.demandoperationalactual

),
ALLDF AS (
select INTERVAL_DATETIME, 'AUS1' as "REGIONID", SUM(OPERATIONAL_DEMAND), MAX(LASTCHANGED), MONTH(INTERVAL_DATETIME) as "MONTH"
FROM  mms.demandoperationalactual
GROUP BY INTERVAL_DATETIME
),
NEWDF AS (
SELECT * FROM DF
UNION
SELECT * FROM ALLDF
),
TOPTEN AS (
select INTERVAL_DATETIME, MONTH, REGIONID, OPERATIONAL_DEMAND, LASTCHANGED, ROW_NUMBER()
OVER (
  PARTITION BY REGIONID, MONTH
  ORDER BY OPERATIONAL_DEMAND
) as RowNo
from NEWDF
),
BOTTEN AS (
select INTERVAL_DATETIME, MONTH, REGIONID, OPERATIONAL_DEMAND, LASTCHANGED, ROW_NUMBER()
OVER (
  PARTITION BY REGIONID, MONTH
  ORDER BY OPERATIONAL_DEMAND DESC
) as RowNo
from NEWDF
)

SELECT * FROM TOPTEN WHERE RowNo = 1
UNION
SELECT * FROM BOTTEN WHERE RowNo = 1
