-- Views to transform marketplace data in pipeline
use role accountadmin;
use schema quickstart_prod.silver;

/*
To join the flight and location focused tables 
we need to cross the gap between the airport and cities domains. 
For this we make use of a Snowpark Python UDF. 
What's really cool is that Snowpark allows us to define a vectorized UDF 
making the processing super efficient as we don’t have to invoke the 
function on each row individually!

To compute the mapping between airports and cities, 
we use SnowflakeFile to read a JSON list from the pyairports package. 
The SnowflakeFile class provides dynamic file access, to stream files of any size.
 */
create or replace function get_city_for_airport(iata varchar)
returns varchar
language python
runtime_version = '3.11'
handler = 'get_city_for_airport'
packages = ('snowflake-snowpark-python')
as $$
from snowflake.snowpark.files import SnowflakeFile
from _snowflake import vectorized
import pandas
import json
@vectorized(input=pandas.DataFrame)
def get_city_for_airport(df):
  airport_list = json.loads(SnowflakeFile.open("@bronze.raw/airport_list.json", 'r', require_scoped_url = False).read())
  airports = {airport[3]: airport[1] for airport in airport_list}
  return df[0].apply(lambda iata: airports.get(iata.upper()))
$$;

/*
To mangle the data into a more usable form, 
we make use of views to not materialize the marketplace data 
and avoid the corresponding storage costs. 
 */

-- We are interested in the per seat carbon emissions. 
-- To obtain these, we need to divide the emission data by the number of seats in the airplane.
create or replace view flight_emissions as select departure_airport, arrival_airport, avg(estimated_co2_total_tonnes / seats) * 1000 as co2_emissions_kg_per_person
  from oag_flight_emissions_data_sample.public.estimated_emissions_schedules_sample
  where seats != 0 and estimated_co2_total_tonnes is not null
  group by departure_airport, arrival_airport;

-- To avoid unreliable flight connections, we compute the fraction of flights that arrive 
-- early or on time from the flight status data provided by OAG.
create or replace view flight_punctuality as select departure_iata_airport_code, arrival_iata_airport_code, count(case when arrival_actual_ingate_timeliness IN ('OnTime', 'Early') THEN 1 END) / COUNT(*) * 100 as punctual_pct
  from oag_flight_status_data_sample.public.flight_status_latest_sample
  where arrival_actual_ingate_timeliness is not null
  group by departure_iata_airport_code, arrival_iata_airport_code;

-- When joining the flight emissions with the punctuality view, 
-- we filter for flights starting from the airport closest to where we live. 
-- This information is provided in the tiny JSON file data/home.json which we query directly in the view.
create or replace view flights_from_home as 
  select 
    departure_airport, 
    arrival_airport, 
    get_city_for_airport(arrival_airport) arrival_city,  
    co2_emissions_kg_per_person, 
    punctual_pct,
  from flight_emissions
  join flight_punctuality on departure_airport = departure_iata_airport_code and arrival_airport = arrival_iata_airport_code
  where departure_airport = (select $1:airport from @quickstart_common.public.quickstart_repo/branches/main/data/home.json (FILE_FORMAT => bronze.json_format));

-- Weather Source provides a weather forecast for the upcoming two weeks. 
-- As the free versions of the data sets we use do not cover the entire globe, 
-- we limit our pipeline to zip codes inside the US and compute the average 
-- temperature, humidity, precipitation probability and cloud coverage.
create or replace view weather_forecast as select postal_code, avg(avg_temperature_air_2m_f) avg_temperature_air_f, avg(avg_humidity_relative_2m_pct) avg_relative_humidity_pct, avg(avg_cloud_cover_tot_pct) avg_cloud_cover_pct, avg(probability_of_precipitation_pct) precipitation_probability_pct
  from global_weather__climate_data_for_bi.standard_tile.forecast_day
  where country = 'US'
  group by postal_code;

-- We use the data provided by Cybersyn to limit our pipeline to 
-- US cities with atleast 100k residents to enjoy all the benefits a big city provides during our vacation.
create or replace view major_us_cities as select geo.geo_id, geo.geo_name, max(ts.value) total_population
  from government_essentials.cybersyn.datacommons_timeseries ts
  join government_essentials.cybersyn.geography_index geo on ts.geo_id = geo.geo_id
  join government_essentials.cybersyn.geography_relationships geo_rel on geo_rel.related_geo_id = geo.geo_id
  where true
    and ts.variable_name = 'Total Population, census.gov'
    and date >= '2020-01-01'
    and geo.level = 'City'
    and geo_rel.geo_id = 'country/USA'
    and value > 100000
  group by geo.geo_id, geo.geo_name
  order by total_population desc;

-- Using the geography relationships provided by Cybersyn we collect all the zip codes belonging to a city.
create or replace view zip_codes_in_city as select city.geo_id city_geo_id, city.geo_name city_geo_name, city.related_geo_id zip_geo_id, city.related_geo_name zip_geo_name
  from us_points_of_interest__addresses.cybersyn.geography_relationships country
  join us_points_of_interest__addresses.cybersyn.geography_relationships city on country.related_geo_id = city.geo_id
  where true
    and country.geo_id = 'country/USA'
    and city.level = 'City'
    and city.related_level = 'CensusZipCodeTabulationArea'
  order by city_geo_id;

create or replace view weather_joined_with_major_cities as 
  select 
    city.geo_id, 
    city.geo_name, city.total_population,
    avg(avg_temperature_air_f) avg_temperature_air_f,
    avg(avg_relative_humidity_pct) avg_relative_humidity_pct,
    avg(avg_cloud_cover_pct) avg_cloud_cover_pct,
    avg(precipitation_probability_pct) precipitation_probability_pct
  from major_us_cities city
  join zip_codes_in_city zip on city.geo_id = zip.city_geo_id
  join weather_forecast weather on zip.zip_geo_name = weather.postal_code
  group by city.geo_id, city.geo_name, city.total_population;

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

create or alter TABLE GOLD.VACATION_SPOTS_2 (
	CITY VARCHAR(16777216),
	AIRPORT VARCHAR(16777216),
	CO2_EMISSIONS_KG_PER_PERSON FLOAT,
	PUNCTUAL_PCT FLOAT,
	AVG_TEMPERATURE_AIR_F FLOAT,
	AVG_RELATIVE_HUMIDITY_PCT FLOAT,
	AVG_CLOUD_COVER_PCT FLOAT,
	PRECIPITATION_PROBABILITY_PCT FLOAT,
	AQUARIUM_CNT NUMBER(38,0),
	ZOO_CNT NUMBER(38,0),
	KOREAN_RESTAURANT_CNT NUMBER(38,0),
  DUMMY_CNT NUMBER(38,0)
);