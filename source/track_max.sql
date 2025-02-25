WITH base_demand AS (
 SELECT INTERVAL_DATETIME, REGIONID, OPERATIONAL_DEMAND 
 FROM mms.demandoperationalactual
), 
aus_total AS (
 SELECT 
   INTERVAL_DATETIME, 
   'AUS1' as REGIONID, 
   SUM(OPERATIONAL_DEMAND) as OPERATIONAL_DEMAND
 FROM base_demand
 GROUP BY INTERVAL_DATETIME
),
combined_demand AS (
 SELECT * FROM base_demand
 UNION ALL 
 SELECT * FROM aus_total
),
demand_history AS (
 SELECT 
   INTERVAL_DATETIME,
   REGIONID,
   OPERATIONAL_DEMAND,
   MAX(OPERATIONAL_DEMAND) OVER (
     PARTITION BY REGIONID 
     ORDER BY INTERVAL_DATETIME 
     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
   ) as peak_to_date
 FROM combined_demand d
)
SELECT 
 INTERVAL_DATETIME,
 REGIONID,
 OPERATIONAL_DEMAND
FROM demand_history
WHERE OPERATIONAL_DEMAND = peak_to_date
ORDER BY REGIONID, INTERVAL_DATETIME;
