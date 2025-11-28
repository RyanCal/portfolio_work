/*
=============================================================================
Project: E-Commerce Sales & Pricing Analysis
Description: Multi-channel sales analysis with product performance tracking
Author: Ryan
Database: SQL Server / Snowflake
=============================================================================

BUSINESS PROBLEM:
Analyze sales performance across multiple e-commerce channels including
marketplace and direct-to-consumer platforms. Track product launches,
inventory levels, and marketing effectiveness.

KEY FEATURES:
- Window functions for product launch date calculations
- Complex CTEs for data transformation
- Multi-channel platform categorization
- Inventory weeks calculation
- Customer segmentation (new vs existing)
- Marketing cost attribution
- Union of actuals vs targets

TABLES USED:
- datamart.dm_fact_sales_b2c (Sales transactions)
- reporting.dim_products (Product master data)
- reporting.fact_inventory_history (Inventory levels)
- reporting.dim_product_pricing (Price tracking)
- reporting.planned_sold_units (Sales targets)

=============================================================================
*/

with launch_asin_date as (
select
distinct dp.master_id as "MASTER_ID",
first_value (dp.product_launched_on) over (partition by dp.master_id order by fs.order_purchased_on asc) as "PRODUCT_LAUNCHED_ON",
first_value (fs.order_purchased_on) over (partition by dp.master_id order by fs.order_purchased_on asc) as "CALCULATED_LAUNCHED_ON"

from datamart.dm_fact_sales_b2c fs
left join reporting.dim_products dp
on fs.item_variation_id = dp.item_variation_id
where fs.order_referrer_grouping in ('Webshop Country1','Webshop Country2','Amazon','Amazon DE','Amazon ES',
      'Amazon IT','Amazon FR','Amazon NL','Amazon SE','Amazon PL','Pan-EU','Marketplace Partner')
--and fs.order_purchased_on >= '2021-04-01'
and fs.order_purchased_on < current_date()
--group by 1
),

country_mapping as
(select
fs.order_referrer_grouping as "ORDER_REFERRER_GROUPING",
fs.order_referrer_name as "ORDER_REFERRER_NAME",
case 
   when fs.order_referrer_grouping in ('Webshop Country1','Webshop Country2','Shopify')
      then 'DE'
   when fs.order_referrer_name in ('Amazon FBA Germany','Amazon Germany','Amazon')
      then 'DE'
   when fs.order_referrer_name in ('Amazon FBA Italy')
      then 'IT'
   when fs.order_referrer_name in ('Amazon FBA Spain')
      then 'ES'
   when fs.order_referrer_name in ('Amazon FBA France')
      then 'FR'
   when fs.order_referrer_name in ('Amazon FBA Netherlands')
      then 'NL'
   when fs.order_referrer_name in ('Amazon FBA Sweden')
      then 'SW'
   when fs.order_referrer_name in ('Amazon FBA Poland')
      then 'PO'
   when fs.order_referrer_name in ('Marketplace Partner')
      then 'DE'
   when fs.order_referrer_grouping in ('B2B')
      then 'DE'
   when fs.order_referrer_grouping like '%China%'
      then 'CH'
   else 'Other' end as "COUNTRY_CODE"

   from datamart.dm_fact_sales_b2c fs
   group by 1,2,3

)

-- sales union table
select 
--fs.order_purchased_on as "date_report",
to_char(fs.order_purchased_on,'YYYY') as "year",
to_char(fs.order_purchased_on,'MM') as "month",
concat(to_char(fs.order_purchased_on,'YYYY'),'-',to_char(fs.order_purchased_on,'MM')) as "year_month",
quarter(fs.order_purchased_on) as "quarter",
concat(to_char(date_trunc('week',fs.order_purchased_on),'YYYY'),'-',to_char(date_trunc('week',fs.order_purchased_on),'MM'),'-',to_char(date_trunc('week',fs.order_purchased_on),'DD')) as "week_ISO",
--dayofweek(fs.order_purchased_on) as "weekday", -- 0 is sunday
week(fs.order_purchased_on) as "week_number",
concat(to_char(date_trunc('week',lad.PRODUCT_LAUNCHED_ON),'YYYY'),'-',to_char(date_trunc('week',lad.PRODUCT_LAUNCHED_ON),'MM'),'-',to_char(date_trunc('week',lad.PRODUCT_LAUNCHED_ON),'DD')) as "product_launch_week",
concat(to_char(date_trunc('week',lad.CALCULATED_LAUNCHED_ON),'YYYY'),'-',to_char(date_trunc('week',lad.CALCULATED_LAUNCHED_ON),'MM'),'-',to_char(date_trunc('week',lad.CALCULATED_LAUNCHED_ON),'DD')) as "calculated_launch_week",

fs.data_source as "data_source",
fs.order_status_group as "order_status_group",
fs.order_brand_name as "brand_name",
fs.item_group as "item_group", -- products or bundle components
dp.product_type as "product_type", -- Protein & Supplements, Plant-based
dp.category_level_1 as "category_level_1", -- Supplements, Personal Care
dp.product_corner_type as "product_corner_type", -- Protein, Vitamins, Beauty
dp.product_title_quantity as "product_title_quantity",
dp.product_title as "product_title",
dp.quantity as "product_size_quantity", -- 210, 500
dp.master_id as "master_id", --dp.item_variation_id as "master_id",
dp.product_asin as "product_asin",

case 
   when fs.order_referrer_grouping in ('Webshop Country1','Webshop Country2')
      then 'D2C'
   when fs.order_referrer_grouping in ('Amazon','Amazon DE','Amazon ES','Amazon IT','Amazon FR',
                  'Amazon NL','Amazon SE','Amazon PL','Pan-EU','Marketplace Partner')
      then 'Marketplace'
   when fs.order_referrer_grouping in ('B2B')
      then 'B2B-Retail'
   when lower(fs.order_referrer_grouping) like '%china%'
      then 'China'
   else 'Other' end as "main_platform",
case 
   when fs.order_referrer_grouping in ('Webshop Country1')
      then 'Webshop Country1'
   when fs.order_referrer_grouping in ('Webshop Country2')
      then 'Webshop Country2'
   when fs.order_referrer_grouping in ('Amazon DE','Amazon')
      then 'Amazon DE'
   when fs.order_referrer_grouping like 'Amazon%'
      then 'Amazon Pan-EU'
   when fs.order_referrer_grouping in ('Marketplace Partner')
      then 'Marketplace Partner'
   when fs.order_referrer_grouping in ('B2B')
      then 'B2B'
   when fs.order_referrer_grouping like '%China%'
      then 'China'
   else 'Other' end as "platform_detailed",
case 
   when fs.order_referrer_grouping in ('Webshop Country1')
      then 'Webshop Country1'
   when fs.order_referrer_grouping in ('Webshop Country2')
      then 'Webshop Country2'
   when fs.order_referrer_name in ('Amazon FBA Germany','Amazon Germany')
      then 'Amazon DE'
   when fs.order_referrer_name in ('Amazon FBA Italy')
      then 'Amazon IT'
   when fs.order_referrer_name in ('Amazon FBA Spain')
      then 'Amazon ES'
   when fs.order_referrer_name in ('Amazon FBA France')
      then 'Amazon FR'
   when fs.order_referrer_name in ('Amazon FBA Netherlands')
      then 'Amazon NL'
   when fs.order_referrer_name in ('Amazon FBA Sweden')
      then 'Amazon SW'
   when fs.order_referrer_name in ('Amazon FBA Poland')
      then 'Amazon PO'
   when fs.order_referrer_name in ('Marketplace Partner')
      then 'Marketplace Partner'
   when fs.order_referrer_grouping in ('B2B')
      then 'B2B'
   when fs.order_referrer_grouping like '%China%'
      then 'China'
   else 'Other' end as "platform_detailed_country",

-- Flag for overstocked products (example logic)
case
   when (fs.order_brand_name = 'Brand A' and dp.product_asin in ('B0XYZ12345','SKU1234567','B01ABC9999','SKU9876543'))
         or (fs.order_brand_name = 'Brand B' and dp.product_asin in ('B0QRS67890','B07XYZ8888','B08ABC7777','B077DEF6666'))   
      then 1
   else 0
   end as "overstocked_flag",
round(avg(fih.daily_total_quantity),0) as "avg_available_qty",
nullif(avg(fih.daily_total_quantity),0)*100 / sum(fs.item_quantity_4er * fs.shipping_probability)  as "weeks_of_inventory",
avg(dpp.list_price) as "amazon_price",
avg(dpp.list_price_d2c) as "d2c_price",
sum(fs.item_revenue_gross * fs.shipping_probability) as "gross_revenue",
sum(fs.item_revenue_net * fs.shipping_probability) as "net_revenue",
sum(fs.item_revenue_net * (1 - fs.shipping_probability)) as "cancelled_net_revenue",
sum(fs.item_revenue_gross * fs.shipping_probability - fs.item_revenue_net * fs.shipping_probability) as "VAT",
sum(fs.item_quantity_4er * fs.shipping_probability) as "items_sold",
sum(case
   when fs.item_quantity_4er = 0
      then 1.04
   when fs.item_quantity_4er is null
      then null
   else fs.item_quantity_4er * (1 - fs.shipping_probability)
   end) as "cancelled_items_sold",
count(distinct fs.order_id) as "orders_plenty_quantity",
count(distinct fs.order_external_id) as "orders_external_quantity",
sum(case
   when fs.is_existing_customer = 'FALSE'
      then fs.item_revenue_gross * fs.shipping_probability
   else 0 end) as "global_NC_gross_revenue",
sum(case
   when fs.is_existing_customer_channel = 'FALSE'
      then fs.item_revenue_gross * fs.shipping_probability
   else 0 end) as "channel_NC_gross_revenue",
sum(case
   when fs.is_existing_customer = 'FALSE'
      then fs.item_revenue_net * fs.shipping_probability
   else 0 end) as "global_NC_net_revenue",
sum(case
   when fs.is_existing_customer_channel = 'FALSE'
      then fs.item_revenue_net * fs.shipping_probability
   else 0 end) as "channel_NC_net_revenue",
count(distinct(case
   when fs.is_existing_customer = 'FALSE'
      then fs.customer_surrogate_key
   else null end)) as "global_NC",
count(distinct(case
   when fs.is_existing_customer_channel = 'FALSE'
      then fs.customer_surrogate_key
   else null end)) as "channel_NC",
count(distinct(fs.customer_surrogate_key)) as "customers_quantity",
sum(fs.unit_cost * fs.item_quantity_4er * fs.shipping_probability) as "product_cost",
sum(fs.fulfillment_cost * fs.shipping_probability) as "fulfillment_cost",
sum(fs.referrer_fee * fs.shipping_probability) as "sales_fee_cost",
0 as "clicks",
0 as "impressions",
0 as "marketing_cost",
0 as "conversions_pixel",
0 as "conversions_value",
0 as "asin_sessions",
0 as "asin_page_views",
0 as "target_units",
0 as "target_net_revenue"

from datamart.dm_fact_sales_b2c fs
left join reporting.dim_products dp
   on fs.item_variation_id = dp.item_variation_id
left join launch_asin_date lad
   on dp.master_id = lad.MASTER_ID
left join reporting.fact_inventory_history fih
   on dp.master_id = fih.master_id
   and fs.order_purchased_on = fih.inventory_date
left join reporting.dim_product_pricing dpp
   on dp.product_asin = dpp.product_asin
   and fs.order_purchased_on = dpp.pricing_date
where fs.order_referrer_grouping in ('Webshop Country1','Webshop Country2','Amazon','Amazon DE','Amazon ES',
      'Amazon IT','Amazon FR','Amazon NL','Amazon SE','Amazon PL','Pan-EU','Marketplace Partner')
and fs.order_purchased_on >= '2021-04-01'
and fs.order_purchased_on < current_date()
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24

-- Marketing costs union
union select
to_char(fac.date,'YYYY') as "year",
to_char(fac.date,'MM') as "month",
concat(to_char(fac.date,'YYYY'),'-',to_char(fac.date,'MM')) as "year_month",
quarter(fac.date) as "quarter",
concat(to_char(date_trunc('week',fac.date),'YYYY'),'-',to_char(date_trunc('week',fac.date),'MM'),'-',to_char(date_trunc('week',fac.date),'DD')) as "week_ISO",
week(fac.date) as "week_number",
concat(to_char(date_trunc('week',lad.PRODUCT_LAUNCHED_ON),'YYYY'),'-',to_char(date_trunc('week',lad.PRODUCT_LAUNCHED_ON),'MM'),'-',to_char(date_trunc('week',lad.PRODUCT_LAUNCHED_ON),'DD')) as "product_launch_week",
concat(to_char(date_trunc('week',lad.CALCULATED_LAUNCHED_ON),'YYYY'),'-',to_char(date_trunc('week',lad.CALCULATED_LAUNCHED_ON),'MM'),'-',to_char(date_trunc('week',lad.CALCULATED_LAUNCHED_ON),'DD')) as "calculated_launch_week",

'marketing_costs' as "data_source",
null as "order_status_group",
daap.BRAND as "brand_name",
null as "item_group",
dp.PRODUCT_TYPE as "product_type",
dp.CATEGORY_LEVEL_1 as "category_level_1",
dp.PRODUCT_CORNER_TYPE as "product_corner_type",
dp.PRODUCT_TITLE_QUANTITY as "product_title_quantity",
dp.PRODUCT_TITLE as "product_title",
dp.QUANTITY as "product_size_quantity",
dp.MASTER_ID as "master_id",
daap.CHILD_ASIN as "product_asin",

case 
   when fac.cost_platform in ('AMAZON')
      then 'Marketplace'
   when fac.cost_platform in ('FACEBOOK','GOOGLE','MICROSOFT')
      then 'D2C'
   else 'Other' end as "main_platform",
'Amazon DE' as "platform_detailed",
'Amazon DE' as "platform_detailed_country",

case
   when (daap.BRAND = 'Brand A' and daap.CHILD_ASIN in ('B0XYZ12345','SKU1234567','B01ABC9999','SKU9876543'))
         or (daap.BRAND = 'Brand B' and daap.CHILD_ASIN in ('B0QRS67890','B07XYZ8888','B08ABC7777','B077DEF6666'))   
      then 1
   else 0
   end as "overstocked_flag",
null as "avg_available_qty",
null as "weeks_of_inventory",
null as "amazon_price",
null as "d2c_price",
0 as "gross_revenue",
0 as "net_revenue",
0 as "cancelled_net_revenue",
0 as "VAT",
0 as "items_sold",
0 as "cancelled_items_sold",
0 as "orders_plenty_quantity",
0 as "orders_external_quantity",
0 as "global_NC_gross_revenue",
0 as "channel_NC_gross_revenue",
0 as "global_NC_net_revenue",
0 as "channel_NC_net_revenue",
0 as "global_NC",
0 as "channel_NC",
0 as "customers_quantity",
0 as "product_cost",
0 as "fulfillment_cost",
0 as "sales_fee_cost",
sum(fac.clicks) as "clicks",
sum(fac.impressions) as "impressions",
sum(fac.spend) as "marketing_cost",
sum(fac.conversions) as "conversions_pixel",
sum(fac.conversions_value) as "conversions_value",
0 as "asin_sessions",
0 as "asin_page_views",
0 as "target_units",
0 as "target_net_revenue"

from reporting.fact_aggregated_costs fac
left join (select
daap.PARENT_ASIN,
daap.CHILD_ASIN,
daap.BRAND_ASIN as "BRAND",
daap.MERCHANT_CUSTOMER_ID
from reporting.dim_advertising_asins_products daap
group by 1,2,3,4) daap
   on fac.asin = daap.PARENT_ASIN
   and fac.merchant_customer_id = daap.MERCHANT_CUSTOMER_ID
left join (select distinct
last_value(dp.brand) over (partition by dp.product_asin order by dp.item_created_at asc) as "BRAND",
last_value(dp.product_asin) over (partition by dp.product_asin order by dp.item_created_at asc) as "PRODUCT_ASIN",
last_value(dp.product_type) over (partition by dp.product_asin order by dp.item_created_at asc) as "PRODUCT_TYPE",
last_value(dp.category_level_1) over (partition by dp.product_asin order by dp.item_created_at asc) as "CATEGORY_LEVEL_1",
last_value(dp.product_corner_type) over (partition by dp.product_asin order by dp.item_created_at asc) as "PRODUCT_CORNER_TYPE",
last_value(dp.product_title_quantity) over (partition by dp.product_asin order by dp.item_created_at asc) as "PRODUCT_TITLE_QUANTITY",
last_value(dp.product_title) over (partition by dp.product_asin order by dp.item_created_at asc) as "PRODUCT_TITLE",
last_value(dp.quantity) over (partition by dp.product_asin order by dp.item_created_at asc) as "QUANTITY",
last_value(dp.master_id) over (partition by dp.product_asin order by dp.item_created_at asc) as "MASTER_ID"
from reporting.dim_products dp
--group by 1,2
) dp
   on daap.CHILD_ASIN = dp.PRODUCT_ASIN --and daap.BRAND = dp.BRAND
left join launch_asin_date lad
   on dp.MASTER_ID = lad.MASTER_ID
where fac.date >= '2021-04-01'
and fac.date < current_date() and fac.cost_type = 'Marketing'
and fac.spend > 0
and fac.cost_platform in ('AMAZON','MICROSOFT','GOOGLE','FACEBOOK')
and ((fac.spend is not null and fac.spend != '0')
or (fac.impressions is not null and fac.impressions != '0')
or (fac.clicks is not null and fac.clicks != '0')
or (fac.conversions is not null and fac.conversions != '0'))
-- Exclude test campaigns and non-product campaigns
and lower(fac.campaign_name) not like '%test%'
and lower(fac.campaign_name) not like '%brand%'
and lower(fac.campaign_name) not like '%video%'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24

-- Targets union table
union select 
to_char(tg.REPORT_DATE,'YYYY') as "year",
to_char(tg.REPORT_DATE,'MM') as "month",
concat(to_char(tg.REPORT_DATE,'YYYY'),'-',to_char(tg.REPORT_DATE,'MM')) as "year_month",
quarter(tg.REPORT_DATE) as "quarter",
concat(to_char(date_trunc('week',tg.REPORT_DATE),'YYYY'),'-',to_char(date_trunc('week',tg.REPORT_DATE),'MM'),'-',to_char(date_trunc('week',tg.REPORT_DATE),'DD')) as "week_ISO",
week(tg.REPORT_DATE) as "week_number",
concat(to_char(date_trunc('week',lad.PRODUCT_LAUNCHED_ON),'YYYY'),'-',to_char(date_trunc('week',lad.PRODUCT_LAUNCHED_ON),'MM'),'-',to_char(date_trunc('week',lad.PRODUCT_LAUNCHED_ON),'DD')) as "product_launch_week",
concat(to_char(date_trunc('week',lad.CALCULATED_LAUNCHED_ON),'YYYY'),'-',to_char(date_trunc('week',lad.CALCULATED_LAUNCHED_ON),'MM'),'-',to_char(date_trunc('week',lad.CALCULATED_LAUNCHED_ON),'DD')) as "calculated_launch_week",

'targets_table' as "data_source",
null as "order_status_group",
tg.BRAND as "brand_name",
null as "item_group",
dp.product_type as "product_type",
dp.category_level_1 as "category_level_1",
dp.product_corner_type as "product_corner_type",
dp.product_title_quantity as "product_title_quantity",
dp.product_title as "product_title",
dp.quantity as "product_size_quantity",
dp.master_id as "master_id",
dp.product_asin as "product_asin",

case
   when tg.SALES_CHANNEL in ('Webshop Country1','Webshop Country2')
      then 'D2C'
   when tg.SALES_CHANNEL in ('Amazon DE','Amazon ES', 'Amazon FR','Amazon IT','Marketplace Partner','Pan-EU')
      then 'Marketplace'
   when tg.SALES_CHANNEL in ('B2B')
      then 'B2B-Retail'
   when tg.SALES_CHANNEL in ('TMALL')
      then 'China'
   else 'Other' end as "main_platform",
case 
   when tg.SALES_CHANNEL in ('Webshop Country1')
      then 'Webshop Country1'
   when tg.SALES_CHANNEL in ('Webshop Country2')
      then 'Webshop Country2'
   when tg.SALES_CHANNEL in ('Amazon DE','Amazon')
      then 'Amazon DE'
   when tg.SALES_CHANNEL like 'Amazon%'
      then 'Amazon Pan-EU'
   when tg.SALES_CHANNEL in ('Marketplace Partner')
      then 'Marketplace Partner'
   when tg.SALES_CHANNEL in ('B2B')
      then 'B2B'
   when tg.SALES_CHANNEL in ('TMALL')
      then 'China'
   else 'Other' end as "platform_detailed",
case 
   when tg.SALES_CHANNEL in ('Webshop Country1')
      then 'Webshop Country1'
   when tg.SALES_CHANNEL in ('Webshop Country2')
      then 'Webshop Country2'
   when tg.SALES_CHANNEL in ('Amazon DE')
      then 'Amazon DE'
   when tg.SALES_CHANNEL in ('Amazon IT')
      then 'Amazon IT'
   when tg.SALES_CHANNEL in ('Amazon SP')
      then 'Amazon ES'
   when tg.SALES_CHANNEL in ('Marketplace Partner')   
      then 'Marketplace Partner'
   when tg.SALES_CHANNEL in ('B2B')
      then 'B2B'
   when tg.SALES_CHANNEL in ('TMALL')
      then 'China'
   else 'Other' end as "platform_detailed_country",

case
   when (tg.BRAND = 'Brand A' and dp.product_asin in ('B0XYZ12345','SKU1234567','B01ABC9999','SKU9876543'))
         or (tg.BRAND = 'Brand B' and dp.product_asin in ('B0QRS67890','B07XYZ8888','B08ABC7777','B077DEF6666'))   
      then 1
   else 0
   end as "overstocked_flag",
null as "avg_available_qty",
null as "weeks_of_inventory",
null as "amazon_price",
null as "d2c_price",
0 as "gross_revenue",
0 as "net_revenue",
0 as "cancelled_net_revenue",
0 as "VAT",
0 as "items_sold",
0 as "cancelled_items_sold",
0 as "orders_plenty_quantity",
0 as "orders_external_quantity",
0 as "global_NC_gross_revenue",
0 as "channel_NC_gross_revenue",
0 as "global_NC_net_revenue",
0 as "channel_NC_net_revenue",
0 as "global_NC",
0 as "channel_NC",
0 as "customers_quantity",
0 as "product_cost",
0 as "fulfillment_cost",
0 as "sales_fee_cost",
0 as "clicks",
0 as "impressions",
0 as "marketing_cost",
0 as "conversions_pixel",
0 as "conversions_value",
0 as "asin_sessions",
0 as "asin_page_views", 
sum(tg.TARGET_UNITS) as "target_units",
0 as "target_net_revenue"

from (
select 
psu.date as "REPORT_DATE",
psu.brand as "BRAND",
psu.sales_channel as "SALES_CHANNEL",
psu.asin as "PRODUCT_ASIN",
sum(psu.sold_units) as "TARGET_UNITS",
0 as "TARGET_NET_REVENUE"
from reporting.planned_sold_units psu
where psu.plan_version = '2024-05-17'
and psu.date < current_date()
and psu.sales_channel in ('Amazon DE','Amazon ES','Webshop Country1','Webshop Country2','Marketplace Partner')
group by 1,2,3,4
) tg
left join reporting.dim_products dp
   on tg.PRODUCT_ASIN = dp.product_asin
left join launch_asin_date lad
   on dp.master_id = lad.MASTER_ID
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24

/*
=============================================================================
QUERY OUTPUT:
- Time dimensions (year, month, quarter, week)
- Product attributes (brand, category, ASIN, title)
- Sales metrics (revenue, units, orders)
- Customer metrics (new vs existing, total customers)
- Cost metrics (product cost, fulfillment, marketing)
- Inventory metrics (weeks of inventory, available qty)
- Target metrics (planned units)

BUSINESS USE CASES:
1. Weekly sales performance tracking
2. Marketing ROI analysis by ASIN
3. Inventory management and restocking alerts
4. New customer acquisition tracking
5. Multi-channel performance comparison
6. Product launch performance analysis
=============================================================================
*/
