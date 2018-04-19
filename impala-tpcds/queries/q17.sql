
select  i_item_id
       ,i_item_desc
       ,s_state
       ,count(ss_quantity) as store_sales_quantitycount
       ,avg(ss_quantity) as store_sales_quantityave
       ,stddev_samp(ss_quantity) as store_sales_quantitystdev
       ,stddev_samp(ss_quantity)/avg(ss_quantity) as store_sales_quantitycov
       ,count(sr_return_quantity) as_store_returns_quantitycount
       ,avg(sr_return_quantity) as_store_returns_quantityave
       ,stddev_samp(sr_return_quantity) as_store_returns_quantitystdev
       ,stddev_samp(sr_return_quantity)/avg(sr_return_quantity) as store_returns_quantitycov
       ,count(cs_quantity) as catalog_sales_quantitycount ,avg(cs_quantity) as catalog_sales_quantityave
       ,stddev_samp(cs_quantity)/avg(cs_quantity) as catalog_sales_quantitystdev
       ,stddev_samp(cs_quantity)/avg(cs_quantity) as catalog_sales_quantitycov
 from (select * from store_sales,item, store where ss_sold_date_sk between 2450815 and 2450905 and i_item_sk = ss_item_sk
   and s_store_sk = ss_store_sk) v1
     ,(select * from store_returns where sr_returned_date_sk between 2450815 and 2451088 ) v2
     ,(select * from catalog_sales where cs_sold_date_sk between 2450815 and 2451088) v3
     ,date_dim d1
     ,date_dim d2
     ,date_dim d3
  --   ,store
  --   ,item
 where d1.d_quarter_name = '1998Q1'
   and d1.d_date_sk = v1.ss_sold_date_sk
--   and i_item_sk = v1.ss_item_sk
--   and s_store_sk = v1.ss_store_sk
   and v1.ss_customer_sk = v2.sr_customer_sk
   and v1.ss_item_sk = v2.sr_item_sk
   and v1.ss_ticket_number = v2.sr_ticket_number
   and v2.sr_returned_date_sk = d2.d_date_sk
   and d2.d_quarter_name in ('1998Q1','1998Q2','1998Q3')
   and v2.sr_customer_sk = v3.cs_bill_customer_sk
   and v2.sr_item_sk = v3.cs_item_sk
   and v3.cs_sold_date_sk = d3.d_date_sk
   -- and v3.cs_sold_date_sk between 2450815 and 2451088
   and d3.d_quarter_name in ('1998Q1','1998Q2','1998Q3')
 group by i_item_id
         ,i_item_desc
         ,s_state
 order by i_item_id
         ,i_item_desc
         ,s_state
 limit 100;

