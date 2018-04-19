
select
   sum(ws_ext_discount_amt)  as "Excess_Discount_Amount"
from
    web_sales
   ,item
   ,date_dim
where
i_manufact_id = 594
and i_item_sk = ws_item_sk
and d_date between '2002-01-03' and
        (cast('2002-01-03' as timestamp) + interval 90 days)
and d_date_sk = ws_sold_date_sk
and ws_sold_date_sk between 2452278 and 2452368
and ws_ext_discount_amt
     > (
         SELECT
            1.3 * avg(ws_ext_discount_amt)
         FROM
            web_sales
           ,date_dim
         WHERE
              ws_item_sk = i_item_sk
          and d_date between '2002-01-03' and
                             (cast('2002-01-03' as timestamp) + interval 90 days)
          and d_date_sk = ws_sold_date_sk
          and ws_sold_date_sk between 2452278 and 2452368
      )
order by sum(ws_ext_discount_amt)
limit 100;

