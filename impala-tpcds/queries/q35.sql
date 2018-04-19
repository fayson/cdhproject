
with table1 as (
select ws_bill_customer_sk sk
from 
	web_sales,date_dim
where
	ws_sold_date_sk between  2451180 and 2451453 and
	d_year = 1999 and
	d_qoy < 4
UNION ALL
select cs_ship_customer_sk sk
from 
	catalog_sales,date_dim
where
	cs_sold_date_sk between  2451180 and 2451453 and
	d_year = 1999 and
	d_qoy < 4
)
select   
  ca_state,
  cd_gender,
  cd_marital_status,
  count(*) cnt1,
  max(cd_dep_count),
  max(cd_dep_count),
  avg(cd_dep_count),
  cd_dep_employed_count,
  count(*) cnt2,
  max(cd_dep_employed_count),
  max(cd_dep_employed_count),
  avg(cd_dep_employed_count),
  cd_dep_college_count,
  count(*) cnt3,
  max(cd_dep_college_count),
  max(cd_dep_college_count),
  avg(cd_dep_college_count)
 from
  customer c,customer_address ca,customer_demographics
 where
  c.c_current_addr_sk = ca.ca_address_sk and
  cd_demo_sk = c.c_current_cdemo_sk and 
  exists (select *
          from store_sales,date_dim
          where c.c_customer_sk = ss_customer_sk and
                ss_sold_date_sk = d_date_sk and
                ss_sold_date_sk between  2451180 and 2451453 and
                d_year = 1999 and
                d_qoy < 4) and
   exists (select * from table1 
   			where c.c_customer_sk = sk)
 group by ca_state,
          cd_gender,
          cd_marital_status,
          cd_dep_count,
          cd_dep_employed_count,
          cd_dep_college_count
 order by ca_state,
          cd_gender,
          cd_marital_status,
          cd_dep_count,
          cd_dep_employed_count,
          cd_dep_college_count
limit 100;

