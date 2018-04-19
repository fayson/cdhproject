with ssales as
(select STRAIGHT_JOIN c_last_name
,c_first_name
,s_store_name
,ca_state
,s_state
,item.i_color
,i_current_price
,i_manager_id
,i_units
,i_size
,sum(ss_ext_sales_price) netpaid
from (select sr_ticket_number,sr_item_sk,i_color from store_returns, item where  sr_item_sk = i_item_sk) store_returns,
(select ss_ext_sales_price,ss_ticket_number,ss_item_sk, ss_customer_sk,ss_store_sk,i_color from store_sales,store, item where s_market_id=5 and ss_store_sk = s_store_sk and  ss_item_sk = i_item_sk)store_sales
,item item
,(select c_last_name ,c_first_name, c_customer_sk,c_birth_country from customer where c_birth_country in (select upper(ca_country) as ca_country from store, customer_address where s_zip = ca_zip and s_market_id=5) )customer
,(select upper(ca_country) as ca_country, ca_state,ca_zip from customer_address where ca_zip in  (select s_zip from store where s_market_id=5)) customer_address
,store
where
ss_ticket_number = sr_ticket_number
and ss_item_sk = sr_item_sk
and  ss_customer_sk = c_customer_sk
and ss_item_sk = i_item_sk
and store_sales.i_color = store_returns.i_color
and store_sales.i_color = item.i_color
and ss_store_sk = s_store_sk
and c_birth_country = upper(ca_country)
and s_zip = ca_zip
and s_market_id=5
group by c_last_name
,c_first_name
,s_store_name
,ca_state
,s_state
,i_color
,i_current_price
,i_manager_id
,i_units
,i_size)
select c_last_name, c_first_name, s_store_name, paid
from (
select c_last_name
,c_first_name
,s_store_name
,sum(netpaid) paid
from ssales
where i_color = 'azure'
group by c_last_name
,c_first_name
,s_store_name
) a1
, (select 0.05*avg(netpaid) paid2
from ssales
) a2
where a1.paid > a2.paid2
;
with ssales as
(select STRAIGHT_JOIN c_last_name
,c_first_name
,s_store_name
,ca_state
,s_state
,item.i_color
,i_current_price
,i_manager_id
,i_units
,i_size
,sum(ss_ext_sales_price) netpaid
from (select sr_ticket_number,sr_item_sk,i_color from store_returns, item where  sr_item_sk = i_item_sk) store_returns,
(select ss_ext_sales_price,ss_ticket_number,ss_item_sk, ss_customer_sk,ss_store_sk,i_color from store_sales,store, item where s_market_id=5 and ss_store_sk = s_store_sk and  ss_item_sk = i_item_sk)store_sales
,item item
,(select c_last_name ,c_first_name, c_customer_sk,c_birth_country from customer where c_birth_country in (select upper(ca_country) as ca_country from store, customer_address where s_zip = ca_zip and s_market_id=5) )customer
,(select upper(ca_country) as ca_country, ca_state,ca_zip from customer_address where ca_zip in  (select s_zip from store where s_market_id=5)) customer_address
,store
where
ss_ticket_number = sr_ticket_number
and ss_item_sk = sr_item_sk
and  ss_customer_sk = c_customer_sk
and ss_item_sk = i_item_sk
and store_sales.i_color = store_returns.i_color
and store_sales.i_color = item.i_color
and ss_store_sk = s_store_sk
and c_birth_country = upper(ca_country)
and s_zip = ca_zip
and s_market_id=5
group by c_last_name
,c_first_name
,s_store_name
,ca_state
,s_state
,i_color
,i_current_price
,i_manager_id
,i_units
,i_size)
select c_last_name, c_first_name, s_store_name, paid
from (
select c_last_name
,c_first_name
,s_store_name
,sum(netpaid) paid
from ssales
where i_color = 'pink'
group by c_last_name
,c_first_name
,s_store_name
) a1
, (select 0.05*avg(netpaid) paid2
from ssales
) a2
where a1.paid > a2.paid2;

