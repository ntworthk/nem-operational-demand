select INTERVAL_DATETIME, REGIONID, OPERATIONAL_DEMAND, LASTCHANGED
from mms.demandoperationalactual
where INTERVAL_DATETIME between '{date_1} 00:05:00' and '{date_2} 00:00:00'
