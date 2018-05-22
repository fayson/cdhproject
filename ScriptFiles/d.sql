use iot_test;
SELECT 
 nvl(A.TOTALGPRSUSEDFLOW,0) as TOTALGPRSUSEDFLOW, nvl(A.TOTALSMSUSEDFLOW,0) as TOTALSMSUSEDFLOW, B.USEDDATE AS USEDDATE 
FROM ( SELECT SUM(GPRSUSEDFLOW) AS TOTALGPRSUSEDFLOW, SUM(SMSUSEDFLOW) AS TOTALSMSUSEDFLOW, cast(STATSDATE as timestamp) AS USEDDATE 
FROM impala_view SIMFLOW 
WHERE SIMFLOW.subdir = '10' AND SIMFLOW.CUSTID = '10099' 
 AND cast(SIMFLOW.STATSDATE as timestamp) >= to_date(date_sub(current_timestamp(),7)) 
 AND cast(SIMFLOW.STATSDATE as timestamp) < to_date(current_timestamp()) 
 GROUP BY STATSDATE ) A 
RIGHT JOIN ( 
 SELECT to_date(date_sub(current_timestamp(),7)) AS USEDDATE UNION ALL
 SELECT to_date(date_sub(current_timestamp(),1)) AS USEDDATE UNION ALL
 SELECT to_date(date_sub(current_timestamp(),2)) AS USEDDATE UNION ALL
 SELECT to_date(date_sub(current_timestamp(),3)) AS USEDDATE UNION ALL
 SELECT to_date(date_sub(current_timestamp(),4)) AS USEDDATE UNION ALL
 SELECT to_date(date_sub(current_timestamp(),5)) AS USEDDATE UNION ALL
 SELECT to_date(date_sub(current_timestamp(),6)) AS USEDDATE 
) B on to_date(A.USEDDATE) = to_date(B.USEDDATE) ORDER BY B.USEDDATE
