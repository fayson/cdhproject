
select
  cd_gender,
  cd_marital_status,
  cd_education_status,
  count(*) cnt1,
  cd_purchase_estimate,
  count(*) cnt2,
  cd_credit_rating,
  count(*) cnt3,
  cd_dep_count,
  count(*) cnt4,
  cd_dep_employed_count,
  count(*) cnt5,
  cd_dep_college_count,
  count(*) cnt6
 from
  customer c,customer_address ca,customer_demographics
 where
  c.c_current_addr_sk = ca.ca_address_sk and
  ca_county in ('Benton County','Owen County','Hamilton County','Boone County','Asotin County') and
  cd_demo_sk = c.c_current_cdemo_sk and
  exists (select *
          from store_sales,date_dim
          where c.c_customer_sk = ss_customer_sk and
                ss_sold_date_sk = d_date_sk and
                ss_sold_date_sk between 2451970 and 2452091 and
                d_year = 2001 and
                d_moy between 3 and 3+3) and
   exists ( select * from (select ws_bill_customer_sk c_sk
   -- exists ( select *
            from web_sales,date_dim
            where -- c.c_customer_sk = ws_bill_customer_sk and
                  ws_sold_date_sk between 2451970 and 2452091 and
                  ws_sold_date_sk = d_date_sk and -- or
		  -- (c_customer_sk = cs_ship_customer_sk and
                  -- cs_sold_date_sk = d_date_sk and
                  -- cs_sold_date_sk between 2451970 and 2452091) and	
                  d_year = 2001 and
                  d_moy between 3 ANd 3+3 --or
 
	union all
          select cs_ship_customer_sk c_sk
            from catalog_sales,date_dim
  --          where c.c_customer_sk = cs_ship_customer_sk and
              where    cs_sold_date_sk = d_date_sk and
                cs_sold_date_sk between 2451970 and 2452091 and
                  d_year = 2001 and
                  d_moy between 3 and 3+3) tab1 where c_sk = c.c_customer_sk)
 group by cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating,
          cd_dep_count,
          cd_dep_employed_count,
          cd_dep_college_count
 order by cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating,
          cd_dep_count,
          cd_dep_employed_count,
          cd_dep_college_count
 limit 100;

