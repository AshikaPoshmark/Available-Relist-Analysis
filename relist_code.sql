---------------------------------------------------------- Overall summary of Available Relist as of the given date ( 16th Sept) ------------------------------------------------------------------------------------

-- only Available Relist Listings  Table

SELECT dw_users.home_domain  AS home_domain,
count(distinct case when listing_status = 'published' and inventory_status = 'available' THEN  dw_listings.listing_id END ) as available_listings,
count(distinct case when listing_status = 'published' and inventory_status = 'available' THEN  a.listing_id END ) as available_relist_listings,
count(distinct case when listing_status = 'published' and inventory_status = 'available' THEN  b.listing_id END ) as available_source_listings,
count(distinct case when listing_status = 'published' and inventory_status = 'available' THEN  c.listing_id END ) as available_root_listings,
count(distinct case when listing_status = 'published' and inventory_status = 'available' and c.relist_source_listing_id != c.relist_root_listing_id  THEN  c.listing_id END ) as available_root_listings
from analytics.dw_listings
LEFT JOIN athena_scratch.dw_listings_manual_relist as a on dw_listings.listing_id = a.listing_id
LEFT JOIN athena_scratch.dw_listings_manual_relist as b on dw_listings.listing_id = b.relist_source_listing_id
LEFT JOIN athena_scratch.dw_listings_manual_relist as c on dw_listings.listing_id = c.relist_root_listing_id
LEFT JOIN analytics.dw_users  AS dw_users ON dw_listings.seller_id  = dw_users.user_id
WHERE is_valid_listing is true and deleted_at is null and home_domain in ('us','ca')
AND (NOT (coalesce((datediff(day,(coalesce(dw_users.guest_joined_at, dw_users.joined_at)),(CASE WHEN dw_users.user_status = 'restricted' THEN dw_users.status_updated_at ELSE NULL END)) + 1) <= 30, FALSE) ) OR (coalesce((datediff(day,(coalesce(dw_users.guest_joined_at, dw_users.joined_at)),(CASE WHEN dw_users.user_status = 'restricted' THEN dw_users.status_updated_at ELSE NULL END)) + 1) <= 30, FALSE) ) IS NULL)
group by 1;

--- only NFS Listings (Not For Sale )


SELECT dw_users.home_domain  AS home_domain,
count(distinct case when listing_status = 'published' and inventory_status = 'not_for_sale' THEN  dw_listings.listing_id END ) as nfs_listings,
count(distinct case when listing_status = 'published' and inventory_status = 'not_for_sale' THEN  a.listing_id END ) as nfs_relist_listings,
count(distinct case when listing_status = 'published' and inventory_status = 'not_for_sale' THEN  b.listing_id END ) as nfs_source_listings,
count(distinct case when listing_status = 'published' and inventory_status = 'not_for_sale' THEN  c.listing_id END ) as nfs_root_listings
from analytics.dw_listings
LEFT JOIN athena_scratch.dw_listings_manual_relist as a on dw_listings.listing_id = a.listing_id
LEFT JOIN athena_scratch.dw_listings_manual_relist as b on dw_listings.listing_id = b.relist_source_listing_id
LEFT JOIN athena_scratch.dw_listings_manual_relist as c on dw_listings.listing_id = c.relist_root_listing_id
LEFT JOIN analytics.dw_users  AS dw_users ON dw_listings.seller_id  = dw_users.user_id
WHERE is_valid_listing is true and deleted_at is null and home_domain in ('us','ca')
AND (NOT (coalesce((datediff(day,(coalesce(dw_users.guest_joined_at, dw_users.joined_at)),(CASE WHEN dw_users.user_status = 'restricted' THEN dw_users.status_updated_at ELSE NULL END)) + 1) <= 30, FALSE) ) OR (coalesce((datediff(day,(coalesce(dw_users.guest_joined_at, dw_users.joined_at)),(CASE WHEN dw_users.user_status = 'restricted' THEN dw_users.status_updated_at ELSE NULL END)) + 1) <= 30, FALSE) ) IS NULL)
group by 1;


-- Available Relists Listings Bucket  base table

drop table if exists analytics_scratch.ashika_delete_relist_analysis_3;
create table analytics_scratch.ashika_delete_relist_analysis_3 as
select COALESCE(c.parent_listing_id, a.relist_root_listing_id) as relist_root_listing_id,
b.seller_id,
c.deleted_at as root_deleted_at,
c.inventory_status as root_listing_inventory_status,
c.listing_status as root_listing_listing_status,
CASE WHEN  c.listing_status = 'published' and c.inventory_status = 'available' THEN 1 ELSE 0 end as is_root_listing_deleted,
Count( a.listing_id) as total_relist_listings,
count(a.relist_source_listing_id) as total_source_listings,
count(DISTINCT CASE when b.listing_status = 'published' and b.inventory_status = 'available'  THEN COALESCE(b.parent_listing_id, b.listing_id) END ) As available_relist_listings

from athena_scratch.dw_listings_manual_relist as a
left join analytics.dw_listings as b on b.listing_id = a.listing_id
left join analytics.dw_listings as c on c.listing_id = a.relist_root_listing_id
group by 1,2,3,4,5,6;



-- Available Relists Listings Bucket

SELECT  CASE
WHEN available_relist_listings + is_root_listing_deleted = 0 THEN 'a. all relist unavailable'
WHEN available_relist_listings + is_root_listing_deleted = 1 THEN 'b. 1 relist'
WHEN available_relist_listings + is_root_listing_deleted BETWEEN 2 AND 5 THEN 'c. 2-5'
WHEN available_relist_listings + is_root_listing_deleted BETWEEN 6 AND 10 THEN 'd. 6-10'
WHEN available_relist_listings + is_root_listing_deleted BETWEEN 11 AND 20 THEN 'e. 11-20'
WHEN available_relist_listings + is_root_listing_deleted BETWEEN 21 AND 50 THEN 'f. 21-50'
WHEN available_relist_listings + is_root_listing_deleted BETWEEN 51 AND 100 THEN 'g. 51-100'
WHEN available_relist_listings + is_root_listing_deleted > 100 THEN 'h. >100'
END AS available_relist_listings_bucket,
count(distinct relist_root_listing_id) as count_unique_root_listing,
count(distinct seller_id) as count_unique_seller,
sum(available_relist_listings) + sum(is_root_listing_deleted) as total_relist_available
from analytics_scratch.ashika_delete_relist_analysis_3
group by 1;


---------------------------------------------------------- Top User List with available relists and the stat ------------------------------------------------------------------------------------




drop table if exists analytics_scratch.ashika_delete_relist_listing1;
create table analytics_scratch.ashika_delete_relist_listing1 as
SELECT COALESCE(a.parent_listing_id, a.listing_id) as parent_listing_id,
       a.parent_first_published_at,
        seller_id,
        b.relist_source_listing_id,
        b.relist_root_listing_id,
        CONCAT(a.listing_title, a.listing_description) AS key_title_description,
        CASE WHEN b.listing_id is not null THEN 1 ELSE 0 END AS is_relist_listing,
        CASE WHEN c.relist_source_listing_id is not null THEN 1 ELSE 0 END AS is_relist_source_listing,
        CASE WHEN d.relist_root_listing_id is not null THEN 1 ELSE 0 END AS is_relist_root_listing
        from analytics.dw_listings as a
        left join athena_scratch.dw_listings_manual_relist as b on b.listing_id = a.listing_id
           left join athena_scratch.dw_listings_manual_relist  as c on c.relist_source_listing_id = a.listing_id
            left join athena_scratch.dw_listings_manual_relist  as d on a.listing_id = d.relist_root_listing_id
        where a.listing_status = 'published' and a.inventory_status = 'available'
        group by 1,2,3,4,5,6,7,8,9;

--select count(distinct parent_listing_id),count( parent_listing_id) from analytics_scratch.ashika_delete_relist_listing1 limit 100;



drop table if exists analytics_scratch.ashika_delete_relist_root_listings;
create table analytics_scratch.ashika_delete_relist_root_listings as
SELECT *,
       ROW_NUMBER()
        OVER (PARTITION BY seller_id, key_title_description ORDER BY parent_first_published_at,parent_listing_id)  AS key_title_description_number
        from analytics_scratch.ashika_delete_relist_listing1 as a

        where is_relist_listing >0 and is_relist_root_listing >0;





drop table if exists analytics_scratch.ashika_delete_relist_root_listings1;
create table analytics_scratch.ashika_delete_relist_root_listings1 as
    SELECT *,
    last_value(relist_root_listing_id_sorted1) ignore nulls over
        (PARTITION BY seller_id, key_title_description ORDER BY parent_first_published_at,parent_listing_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as relist_root_listing_id_sorted

    from

(SELECT *,
       case when key_title_description_number = 1 THEN relist_root_listing_id
        ELSE NULL END AS relist_root_listing_id_sorted1
        from analytics_scratch.ashika_delete_relist_root_listings ) as a;





drop table if exists analytics_scratch.ashika_delete_relist_base_table;
create table analytics_scratch.ashika_delete_relist_base_table as
 SELECT a.*,
        b.key_title_description_number,
        b.relist_root_listing_id_sorted,
        coalesce(b.relist_root_listing_id_sorted,a.relist_root_listing_id) as final_root_listing_id
FROM analytics_scratch.ashika_delete_relist_listing1 a
LEFT JOIN analytics_scratch.ashika_delete_relist_root_listings1  b ON a.parent_listing_id = b.parent_listing_id



drop table if exists analytics_scratch.ashika_delete_relist_base_table1;
create table analytics_scratch.ashika_delete_relist_base_table1 as
 SELECT a.*,
        CASE WHEN b.final_root_listing_id is not null THEN 1 ELSE 0 END AS is_final_relist_root_listing
FROM analytics_scratch.ashika_delete_relist_base_table a
LEFT JOIN analytics_scratch.ashika_delete_relist_base_table  b ON a.parent_listing_id = b.final_root_listing_id




drop table if exists analytics_scratch.ashika_delete_relist_base_table_final;
create table analytics_scratch.ashika_delete_relist_base_table_final as
 SELECT a.*,
        CASE WHEN is_final_relist_root_listing > 0 and is_relist_listing > 0 THEN parent_listing_id
            WHEN is_relist_root_listing > 0 and is_relist_listing = 0 THEN parent_listing_id
            ELSE final_root_listing_id END AS final_relist_root_listing
FROM analytics_scratch.ashika_delete_relist_base_table1 a



drop table if exists analytics_scratch.ashika_delete_relist_analysis_4;
create table analytics_scratch.ashika_delete_relist_analysis_4 as
select seller_id,
       CASE WHEN final_relist_root_listing is not null THEN 'Yes' ELSE 'No' END AS is_relist_listings_including_the_root_listing,
       final_relist_root_listing,
       count( distinct parent_listing_id) as available_listings
       from analytics_scratch.ashika_delete_relist_base_table_final as a
       group by 1,2,3
order by 1
;



-- top available relist user list

SELECT seller_id,username,platform_user_id,home_domain, sum(available_listings) as available_listings,
       sum(case when is_relist_listings_including_the_root_listing = 'Yes' then available_listings end) as relist_listings_including_root_listings,
       sum(case when is_relist_listings_including_the_root_listing = 'Yes' and available_listings > 1 then available_listings end) as available_listing_with_more_than_1_variant,
       count(distinct final_relist_root_listing) as unique_root_listings
from analytics_scratch.ashika_delete_relist_analysis_4
left join analytics.dw_users on dw_users.user_id = seller_id
left join analytics.dw_users_info on dw_users_info.user_id = seller_id
where  home_domain in ('us','ca')
  AND (NOT (coalesce((datediff(day,(coalesce(dw_users.guest_joined_at, dw_users.joined_at)),
(CASE WHEN dw_users.user_status = 'restricted' THEN dw_users.status_updated_at ELSE NULL END)) + 1) <= 30, FALSE) )
        OR (coalesce((datediff(day,(coalesce(dw_users.guest_joined_at, dw_users.joined_at)),
        (CASE WHEN dw_users.user_status = 'restricted' THEN dw_users.status_updated_at ELSE NULL END)) + 1) <= 30, FALSE) ) IS NULL)
group by 1,2,3,4 having available_listing_with_more_than_1_variant > 0
order by available_listing_with_more_than_1_variant desc
limit 100000;



---------------------------------------------------------- First Match of Relist Listings on a given week (09/14 to 09/20 of 2025) ------------------------------------------------------------------------------------



--Databrick code link to create available listings on the given date from snapshot data : https://poshmark-prod.cloud.databricks.com/editor/notebooks/1313218713203374?o=3891659053752709#command/8598555155132630




drop table if exists analytics_scratch.ashika_delete_relist_listing1_fm;
create table analytics_scratch.ashika_delete_relist_listing1_fm as
SELECT a.parent_listing_id,
       a.first_published_at as parent_first_published_at,
        a.seller_id,
        b.relist_source_listing_id,
        b.relist_root_listing_id,
        CONCAT(dw_listings.listing_title, dw_listings.listing_description) AS key_title_description,
        CASE WHEN b.listing_id is not null THEN 1 ELSE 0 END AS is_relist_listing,
        CASE WHEN c.relist_source_listing_id is not null THEN 1 ELSE 0 END AS is_relist_source_listing,
        CASE WHEN d.relist_root_listing_id is not null THEN 1 ELSE 0 END AS is_relist_root_listing
        from analytics_scratch.ashika_dw_listings_available_as_of_2025_09_20 as a
        left join analytics.dw_listings  on a.parent_listing_id = dw_listings.listing_id
        left join athena_scratch.dw_listings_manual_relist as b on b.listing_id = a.parent_listing_id
           left join athena_scratch.dw_listings_manual_relist  as c on c.relist_source_listing_id = a.parent_listing_id
            left join athena_scratch.dw_listings_manual_relist  as d on a.parent_listing_id = d.relist_root_listing_id
        where a.listing_status = 'published' and a.inventory_status = 'available'
        group by 1,2,3,4,5,6,7,8,9;


drop table if exists analytics_scratch.ashika_delete_relist_root_listings_fm;
create table analytics_scratch.ashika_delete_relist_root_listings_fm as
SELECT *,
       ROW_NUMBER()
        OVER (PARTITION BY seller_id, key_title_description ORDER BY parent_first_published_at,parent_listing_id)  AS key_title_description_number
        from analytics_scratch.ashika_delete_relist_listing1_fm as a

        where is_relist_listing >0 and is_relist_root_listing >0;


drop table if exists analytics_scratch.ashika_delete_relist_root_listings1_fm;
create table analytics_scratch.ashika_delete_relist_root_listings1_fm as
    SELECT *,
    last_value(relist_root_listing_id_sorted1) ignore nulls over
        (PARTITION BY seller_id, key_title_description ORDER BY parent_first_published_at,parent_listing_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as relist_root_listing_id_sorted

    from

(SELECT *,
       case when key_title_description_number = 1 THEN relist_root_listing_id
        ELSE NULL END AS relist_root_listing_id_sorted1
        from analytics_scratch.ashika_delete_relist_root_listings_fm ) as a;


drop table if exists analytics_scratch.ashika_delete_relist_base_table_fm;
create table analytics_scratch.ashika_delete_relist_base_table_fm as
 SELECT a.*,
        b.key_title_description_number,
        b.relist_root_listing_id_sorted,
        coalesce(b.relist_root_listing_id_sorted,a.relist_root_listing_id) as final_root_listing_id
FROM analytics_scratch.ashika_delete_relist_listing1_fm a
LEFT JOIN analytics_scratch.ashika_delete_relist_root_listings1_fm  b ON a.parent_listing_id = b.parent_listing_id

drop table if exists analytics_scratch.ashika_delete_relist_base_table1_fm;
create table analytics_scratch.ashika_delete_relist_base_table1_fm as
 SELECT a.*,
        CASE WHEN b.final_root_listing_id is not null THEN 1 ELSE 0 END AS is_final_relist_root_listing
FROM analytics_scratch.ashika_delete_relist_base_table_fm a
LEFT JOIN analytics_scratch.ashika_delete_relist_base_table_fm  b ON a.parent_listing_id = b.final_root_listing_id


drop table if exists analytics_scratch.ashika_delete_relist_base_table_final_fm;
create table analytics_scratch.ashika_delete_relist_base_table_final_fm as
 SELECT a.*,
        CASE WHEN is_final_relist_root_listing > 0 and is_relist_listing > 0 THEN parent_listing_id
            WHEN is_relist_root_listing > 0 and is_relist_listing = 0 THEN parent_listing_id
            ELSE final_root_listing_id END AS final_relist_root_listing
FROM analytics_scratch.ashika_delete_relist_base_table1_fm a



drop table if exists analytics_scratch.ashika_delete_relist_analysis_4_fm;
create table analytics_scratch.ashika_delete_relist_analysis_4_fm as
select seller_id,
       CASE WHEN final_relist_root_listing is not null THEN 'Yes' ELSE 'No' END AS is_relist_listings_including_the_root_listing,
       final_relist_root_listing,
       count( distinct parent_listing_id) as available_listings
       from analytics_scratch.ashika_delete_relist_base_table_final_fm as a
       group by 1,2,3
order by 1
;



drop table if exists analytics_scratch.ashika_delete_relist_fm;
create table analytics_scratch.ashika_delete_relist_fm as
select b.*,user_id,min(shopper_listing_interaction_number) as min_shopper_listing_interaction_number
from analytics_scratch.ashika_delete_relist_base_table_final_fm as b
         left join (select user_id,listing_id,shopper_listing_interaction_number
                    from athena_scratch.highway_traffic_enriched where event_date between '2025-09-14' and '2025-09-20' and actor_type ='user' ) as a
         on b.parent_listing_id = a.listing_id
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15;


------- Summary tables ------

select
       count(distinct parent_listing_id) as available_listings,
       count(distinct case when user_id is not null then parent_listing_id end ) as interacted_listings,
       count(distinct case when user_id is not null and min_shopper_listing_interaction_number=1 then parent_listing_id end ) as fm_listings,

       count(distinct case when user_id is not null then user_id end ) as interacted_listings_viewers,
       count(distinct case when user_id is not null and min_shopper_listing_interaction_number=1 then user_id end ) as fm_listings_viewers,

       count(distinct case when a.final_relist_root_listing is not null then parent_listing_id end ) as relist_listing,
       count(distinct case when a.final_relist_root_listing is not null and user_id is not null then parent_listing_id end ) as relist_listing_interacted,
       count(distinct case when a.final_relist_root_listing is not null and user_id is not null and min_shopper_listing_interaction_number=1  then parent_listing_id end ) as relist_listing_fm,

       count(distinct case when a.final_relist_root_listing is not null and available_listings > 1 then a.parent_listing_id end ) as relist_listing_more_than_1_variant,
       count(distinct case when a.final_relist_root_listing is not null and user_id is not null and available_listings > 1 then a.parent_listing_id end ) as relist_listing_more_than_1_variant_interacted,
       count(distinct case when a.final_relist_root_listing is not null and user_id is not null and available_listings > 1 and min_shopper_listing_interaction_number=1  then a.parent_listing_id end ) as relist_listing_more_than_1_variant_fm,

       count(distinct case when a.final_relist_root_listing is not null then a.final_relist_root_listing end ) as relist_root_listing,
       count(distinct case when a.final_relist_root_listing is not null and user_id is not null then a.final_relist_root_listing end ) as relist_root_listing_interacted,
       count(distinct case when a.final_relist_root_listing is not null and user_id is not null  and min_shopper_listing_interaction_number=1  then a.final_relist_root_listing end ) as relist_root_listing_fm,

       count(distinct case when a.final_relist_root_listing is not null and available_listings > 1 then a.final_relist_root_listing end ) as relist_root_listing_more_than_1_variant,
       count(distinct case when a.final_relist_root_listing is not null and user_id is not null and available_listings > 1 then a.final_relist_root_listing end ) as relist_root_listing_more_than_1_variant_interacted,
       count(distinct case when a.final_relist_root_listing is not null and user_id is not null and available_listings > 1 and min_shopper_listing_interaction_number=1 then a.final_relist_root_listing end ) as relist_root_listing_more_than_1_variant_fm
from analytics_scratch.ashika_delete_relist_fm as a
left join analytics_scratch.ashika_delete_relist_analysis_4_fm as b  on a.final_relist_root_listing = b.final_relist_root_listing
;


---- Relist listings that had FM and interaction

select CASE WHEN b.final_relist_root_listing is not null THEN 'Yes' ELSE 'No' END AS is_relist_listings_including_the_root_listing,
    CASE
        WHEN available_listings = 1 THEN 'b. 1 relist'
        WHEN available_listings  BETWEEN 2 AND 5 THEN 'c. 2-5'
        WHEN available_listings  BETWEEN 6 AND 10 THEN 'd. 6-10'
        WHEN available_listings BETWEEN 11 AND 20 THEN 'e. 11-20'
        WHEN available_listings BETWEEN 21 AND 50 THEN 'f. 21-50'
        WHEN available_listings  BETWEEN 51 AND 100 THEN 'g. 51-100'
        WHEN available_listings  > 100 THEN 'h. >100'
    END AS available_relist_listings_bucket,
       count(distinct parent_listing_id),
       count(distinct a.final_relist_root_listing),
       count(distinct case when user_id is not null and min_shopper_listing_interaction_number=1 then parent_listing_id end ) as fm_listings,
       count(distinct case when user_id is not null and min_shopper_listing_interaction_number=1 then a.final_relist_root_listing end ) as fm_root_listings,
       count(distinct case when user_id is not null and min_shopper_listing_interaction_number=1 then user_id||a.parent_listing_id end ) as fm_listings_viewers,
       count(distinct case when user_id is not null and min_shopper_listing_interaction_number=1 then user_id end ) as fm_unique_viewers,

       count(distinct case when user_id is not null  then parent_listing_id end ) as interacted_listings,
       count(distinct case when user_id is not null then a.final_relist_root_listing end ) as interacted_root_listings,
       count(distinct case when user_id is not null then user_id||a.parent_listing_id end ) as interacted_listings_viewers,
       count(distinct case when user_id is not null then user_id end ) as interacted_unique_viewers
from analytics_scratch.ashika_delete_relist_fm as a
left join analytics_scratch.ashika_delete_relist_analysis_4_fm as b  on a.final_relist_root_listing = b.final_relist_root_listing
group by 1,2
;


----- From shopper view , no of relists FM by the shopper in the given week -----


drop table if exists analytics_scratch.ashika_delete_relist_fm_shopper;
create table analytics_scratch.ashika_delete_relist_fm_shopper as
select user_id,
       final_relist_root_listing as root_listing_id,
       count(distinct case when min_shopper_listing_interaction_number =1 then parent_listing_id end) fm_listing_count,
       count(distinct case when min_shopper_listing_interaction_number >0 then parent_listing_id end) interaction_listing_count
from analytics_scratch.ashika_delete_relist_fm as a
 where final_relist_root_listing is not null
 group by 1,2
 order by 1,2;

select
       CASE
        WHEN fm_listing_count = 1 THEN 'b. 1 FM'
        WHEN fm_listing_count  BETWEEN 2 AND 5 THEN 'c. 2-5'
        WHEN fm_listing_count  BETWEEN 6 AND 10 THEN 'd. 6-10'
        WHEN fm_listing_count BETWEEN 11 AND 20 THEN 'e. 11-20'
        WHEN fm_listing_count BETWEEN 21 AND 50 THEN 'f. 21-50'
        WHEN fm_listing_count  BETWEEN 51 AND 100 THEN 'g. 51-100'
        WHEN fm_listing_count  > 100 THEN 'h. >100'
    END AS relist_listings_fm_bucket,
    count(distinct  root_listing_id ) as count_unique_root_listing,
    count(distinct  user_id ) as unique_shoppers,
    sum(fm_listing_count) as total_listings_with_fm
from analytics_scratch.ashika_delete_relist_fm_shopper
group by 1;



---------------------------------------------------------- Search impression and clicks on Relist Listings on a given week on a daily level (09/14 to 09/20 of 2025) ------------------------------------------------------------------------------------


-- databrick code link to create  analytics_scratch.ashika_search_impression_and_click : https://poshmark-prod.cloud.databricks.com/editor/notebooks/1313218713203374?o=3891659053752709#command/8598555155132630                             


select event_date,
       count(distinct case when true_impression_count >0 then a.listing_id end ) listings_impression,
       count(distinct case when true_impression_count >0  and b.listing_id is not null then a.listing_id end ) as relist_listings,
       count(distinct case when true_impression_count >0  and b.listing_id is not null then coalesce(c.final_relist_root_listing,b.relist_root_listing_id) end ) as relist_root_listings,

       count(distinct case when true_impression_count >0 then a.user_id end ) listings_impression_shoppers,
       count(distinct case when true_impression_count >0  and b.listing_id is not null then a.user_id end ) as relist_listings_shoppers,
       -- count(distinct case when true_impression_count >0  and b.listing_id is not null then coalesce(c.final_relist_root_listing,b.relist_root_listing_id) end ) as relist_root_listings,

       count(distinct case when search_clicks >0 then a.listing_id end ) as listings_click,
       count(distinct case when search_clicks >0  and b.listing_id is not null then a.listing_id end ) as relist_listings_click,
       count(distinct case when search_clicks >0  and b.listing_id is not null then coalesce(c.final_relist_root_listing,b.relist_root_listing_id) end ) as relist_root_listings_click,

      count(distinct case when search_clicks >0 then a.user_id end ) as listings_click_shoppers,
      count(distinct case when search_clicks >0  and b.listing_id is not null then a.user_id end ) as relist_listings_click_shoppers

 from analytics_scratch.ashika_search_impression_and_click as a
left join athena_scratch.dw_listings_manual_relist as b on a.listing_id = b.listing_id
left join analytics_scratch.ashika_delete_relist_analysis_with_buckets as c on a.listing_id = c.parent_listing_id
 left join analytics.dw_listings on a.listing_id = dw_listings.listing_id
 where dw_listings.seller_id <> user_id
group by 1
limit 10;


drop table if exists analytics_scratch.ashika_delete_relist_search_imp;
create table analytics_scratch.ashika_delete_relist_search_imp as
select event_date,user_id,
       CASE WHEN coalesce(c.final_relist_root_listing,b.relist_root_listing_id) is not null THEN 'Yes' ELSE 'No' END AS is_relist,
       coalesce(c.final_relist_root_listing,b.relist_root_listing_id) as root_listing_id,
       count(distinct case when true_impression_count >0 then a.listing_id end) listings_impression,
        count(distinct case when search_clicks >0 then a.listing_id end ) as listings_click
 from analytics_scratch.ashika_search_impression_and_click as a
left join athena_scratch.dw_listings_manual_relist as b on a.listing_id = b.listing_id
left join analytics_scratch.ashika_delete_relist_analysis_with_buckets as c on a.listing_id = c.parent_listing_id
 left join analytics.dw_listings on a.listing_id = dw_listings.listing_id
 where dw_listings.seller_id <> user_id
 group by 1,2,3,4
 order by 1,2;



select event_date,
       is_relist,
       CASE
        WHEN listings_impression = 1 THEN 'b. 1 impression'
        WHEN listings_impression  BETWEEN 2 AND 5 THEN 'c. 2-5'
        WHEN listings_impression  BETWEEN 6 AND 10 THEN 'd. 6-10'
        WHEN listings_impression BETWEEN 11 AND 20 THEN 'e. 11-20'
        WHEN listings_impression BETWEEN 21 AND 50 THEN 'f. 21-50'
        WHEN listings_impression  BETWEEN 51 AND 100 THEN 'g. 51-100'
        WHEN listings_impression  > 100 THEN 'h. >100'
    END AS relist_listings_relist_impression_bucket,
    count(distinct case when listings_impression>0 then root_listing_id end) as count_unique_root_listing,
    count(distinct case when listings_click>0 then root_listing_id end) as count_unique_root_listing_with_click,
    count(distinct case when listings_impression>0 then user_id end) as unique_shoppers_with_impression,
    count(distinct case when listings_click>0 then user_id end) as count_unique_shoppers_with_click,
    sum(listings_impression) as total_listings_with_impression,
    sum(listings_click) as total_listings_with_click
from analytics_scratch.ashika_delete_relist_search_imp
group by 1,2,3;








