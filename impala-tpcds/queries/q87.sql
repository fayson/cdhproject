
select count(*)
           from
           (select
              c_last_name,c_first_name,d_date,sum(q17.c3) c3,count(*) c4
            from
              (select
                 c_last_name,c_first_name,d_date,-1 as c3
               from
                 customer,date_dim,web_sales 
               where
                 (d_month_seq between 1177 and 1177+11) and
                 (ws_bill_customer_sk = c_customer_sk) and
                 (ws_sold_date_sk = d_date_sk) and
                 (ws_sold_date_sk between 2450846 and 2451210)
               union all
               select
		 		 c_last_name,c_first_name,d_date,1 as c3
               from
                 (select
                    c_last_name,c_first_name,d_date 
                  from
                    (select
						c_last_name,c_first_name,d_date,sum(q13.c3) c3,	count(*) c4
                     from
                       (select
                          c_last_name,c_first_name,d_date,-1 as c3
                        from
                          customer,date_dim,catalog_sales 
                        where
                          (d_month_seq between 1177 and 1177+11) and
                          (cs_bill_customer_sk = c_customer_sk) and
                          (cs_sold_date_sk = d_date_sk) and
                          (cs_sold_date_sk between 2450846 and 2451210)
                        union all
                        select
                          c_last_name,c_first_name,d_date,1
                        from
                          customer,date_dim,store_sales 
                        where
                          (d_month_seq between 1177 and 1177+11) and
                          (ss_customer_sk = c_customer_sk) and
                          (ss_sold_date_sk = d_date_sk) and
                          (ss_sold_date_sk between 2450846 and 2451210)
                       ) as q13
                     group by
                       c_last_name,
                       c_first_name,
                       d_date
                    ) as q14
                  where
                    (q14.c4 = q14.c3)
                 ) as q15
              ) as q17
            group by
              c_last_name,
              c_first_name,
              d_date
           ) as q18
         where
           (q18.c4 = q18.c3)
;
