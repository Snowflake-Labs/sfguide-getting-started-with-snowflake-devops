/*
-----------------------------------------------------------------
Perform the following changes after switching to the "dev" branch
-----------------------------------------------------------------

-- Add this view definition at the end of steps/03_harmonize_data.sql
create or replace view attractions as select
    city.geo_id,
    city.geo_name,
    count(case when category_main = 'Aquarium' THEN 1 END) aquarium_cnt,
    count(case when category_main = 'Zoo' THEN 1 END) zoo_cnt,
    count(case when category_main = 'Korean Restaurant' THEN 1 END) korean_restaurant_cnt,
from us_points_of_interest__addresses.cybersyn.point_of_interest_index poi
join us_points_of_interest__addresses.cybersyn.point_of_interest_addresses_relationships poi_add on poi_add.poi_id = poi.poi_id
join us_points_of_interest__addresses.cybersyn.us_addresses address on address.address_id = poi_add.address_id
join major_us_cities city on city.geo_id = address.id_city
where true
    and category_main in ('Aquarium', 'Zoo', 'Korean Restaurant')
    and id_country = 'country/USA'
group by city.geo_id, city.geo_name;


-- Append the following column definitions to the column list of the CREATE OR ALTER TABLE in steps/04_orchestrate_jobs.sql
, aquarium_cnt int
, zoo_cnt int
, korean_restaurant_cnt int


-- Add the "attractions" view to the JOIN clause of the "vacation_spots_update" task in steps/04_orchestrate_jobs.sql
join silver.attractions att on att.geo_name = city.geo_name


-- Append the following updates to the WHEN MATCHED clause of the "vacation_spots_update" task in steps/04_orchestrate_jobs.sql
, vacation_spots.aquarium_cnt = harmonized_vacation_spots.aquarium_cnt
, vacation_spots.zoo_cnt = harmonized_vacation_spots.zoo_cnt
, vacation_spots.korean_restaurant_cnt = harmonized_vacation_spots.korean_restaurant_cnt


-- Append the following columns to the WHEN NOT MATCHED clause of the "vacation_spots_update" task in steps/04_orchestrate_jobs.sql
, harmonized_vacation_spots.aquarium_cnt
, harmonized_vacation_spots.zoo_cnt
, harmonized_vacation_spots.korean_restaurant_cnt


-- Add the following filter conditions to the WHERE clause of the "email_notification" task in steps/04_orchestrate_jobs.sql
and korean_restaurant_cnt > 0
and (zoo_cnt > 0 or aquarium_cnt > 0)


---------------------------------------------------------
Commit your changes to the "dev" branch before continuing
---------------------------------------------------------
*/

use role accountadmin;
-- Fetch changes from GitHub
alter git repository quickstart_common.public.quickstart_repo fetch;
-- Deploy the updated data pipeline
execute immediate from @quickstart_common.public.quickstart_repo/branches/main/deploy_pipeline.sql;
