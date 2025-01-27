# Views to transform marketplace data in pipeline

import os

from snowflake.core import Root, CreateMode
from snowflake.snowpark import Session
from snowflake.core.user_defined_function import (
    Argument,
    ReturnDataType,
    PythonFunction,
    UserDefinedFunction,
)
from snowflake.core.view import View, ViewColumn


"""
To join the flight and location focused tables 
we need to cross the gap between the airport and cities domains. 
For this we make use of a Snowpark Python UDF. 
What's really cool is that Snowpark allows us to define a vectorized UDF 
making the processing super efficient as we don’t have to invoke the 
function on each row individually!

To compute the mapping between airports and cities, 
we use SnowflakeFile to read a JSON list from the pyairports package. 
The SnowflakeFile class provides dynamic file access, to stream files of any size.
"""
map_city_to_airport = UserDefinedFunction(
    name="get_city_for_airport",
    arguments=[Argument(name="iata", datatype="VARCHAR")],
    return_type=ReturnDataType(datatype="VARCHAR"),
    language_config=PythonFunction(
        runtime_version="3.11", packages=["snowflake-snowpark-python"], handler="main"
    ),
    body="""
from snowflake.snowpark.files import SnowflakeFile
from _snowflake import vectorized
import pandas
import json

@vectorized(input=pandas.DataFrame)
def main(df):
    airport_list = json.loads(
        SnowflakeFile.open("@bronze.raw/airport_list.json", 'r', require_scoped_url = False).read()
    )
    airports = {airport[3]: airport[1] for airport in airport_list}
    return df[0].apply(lambda iata: airports.get(iata.upper()))
""",
)


"""
To mangle the data into a more usable form, 
we make use of views to not materialize the marketplace data 
and avoid the corresponding storage costs. 
"""

pipeline = [
    # We are interested in the per seat carbon emissions.
    # To obtain these, we need to divide the emission data by the number of seats in the airplane.
    View(
        name="flight_emissions",
        columns=[
            ViewColumn(name="departure_airport"),
            ViewColumn(name="arrival_airport"),
            ViewColumn(name="co2_emissions_kg_per_person"),
        ],
        query="""
        select 
            departure_airport, 
            arrival_airport, 
            avg(estimated_co2_total_tonnes / seats) * 1000 as co2_emissions_kg_per_person
        from oag_flight_emissions_data_sample.public.estimated_emissions_schedules_sample
        where seats != 0 and estimated_co2_total_tonnes is not null
        group by departure_airport, arrival_airport
        """,
    ),
    # To avoid unreliable flight connections, we compute the fraction of flights that arrive
    # early or on time from the flight status data provided by OAG.
    View(
        name="flight_punctuality",
        columns=[
            ViewColumn(name="departure_iata_airport_code"),
            ViewColumn(name="arrival_iata_airport_code"),
            ViewColumn(name="punctual_pct"),
        ],
        query="""
        select 
            departure_iata_airport_code, 
            arrival_iata_airport_code, 
            count(
                case when arrival_actual_ingate_timeliness IN ('OnTime', 'Early') THEN 1 END
            ) / COUNT(*) * 100 as punctual_pct
        from oag_flight_status_data_sample.public.flight_status_latest_sample
        where arrival_actual_ingate_timeliness is not null
        group by departure_iata_airport_code, arrival_iata_airport_code
        """,
    ),
    # When joining the flight emissions with the punctuality view,
    # we filter for flights starting from the airport closest to where we live.
    # This information is provided in the tiny JSON file data/home.json which we query directly in the view.
    View(
        name="flights_from_home",
        columns=[
            ViewColumn(name="departure_airport"),
            ViewColumn(name="arrival_airport"),
            ViewColumn(name="arrival_city"),
            ViewColumn(name="co2_emissions_kg_per_person"),
            ViewColumn(name="punctual_pct"),
        ],
        query="""
        select 
            departure_airport, 
            arrival_airport, 
            get_city_for_airport(arrival_airport) arrival_city,  
            co2_emissions_kg_per_person, 
            punctual_pct,
        from flight_emissions
        join flight_punctuality 
            on departure_airport = departure_iata_airport_code 
            and arrival_airport = arrival_iata_airport_code
        where departure_airport = (
            select $1:airport 
            from @quickstart_common.public.quickstart_repo/branches/main/data/home.json 
                (FILE_FORMAT => bronze.json_format))
        """,
    ),
    # Weather Source provides a weather forecast for the upcoming two weeks.
    # As the free versions of the data sets we use do not cover the entire globe,
    # we limit our pipeline to zip codes inside the US and compute the average
    # temperature, humidity, precipitation probability and cloud coverage.
    View(
        name="weather_forecast",
        columns=[
            ViewColumn(name="postal_code"),
            ViewColumn(name="avg_temperature_air_f"),
            ViewColumn(name="avg_relative_humidity_pct"),
            ViewColumn(name="avg_cloud_cover_pct"),
            ViewColumn(name="precipitation_probability_pct"),
        ],
        query="""
        select 
            postal_code, 
            avg(avg_temperature_air_2m_f) avg_temperature_air_f, 
            avg(avg_humidity_relative_2m_pct) avg_relative_humidity_pct, 
            avg(avg_cloud_cover_tot_pct) avg_cloud_cover_pct, 
            avg(probability_of_precipitation_pct) precipitation_probability_pct
        from global_weather__climate_data_for_bi.standard_tile.forecast_day
        where country = 'US'
        group by postal_code
        """,
    ),
    # We use the data provided by Cybersyn to limit our pipeline to US cities with atleast
    # 100k residents to enjoy all the benefits a big city provides during our vacation.
    View(
        name="major_us_cities",
        columns=[
            ViewColumn(name="geo_id"),
            ViewColumn(name="geo_name"),
            ViewColumn(name="total_population"),
        ],
        query="""
        select 
            geo.geo_id, 
            geo.geo_name, 
            max(ts.value) total_population
        from global_government.cybersyn.datacommons_timeseries ts
        join global_government.cybersyn.geography_index geo 
            on ts.geo_id = geo.geo_id
        join global_government.cybersyn.geography_relationships geo_rel 
            on geo_rel.related_geo_id = geo.geo_id
        where true
            and ts.variable_name = 'Total Population, census.gov'
            and date >= '2020-01-01'
            and geo.level = 'City'
            and geo_rel.geo_id = 'country/USA'
            and value > 100000
        group by geo.geo_id, geo.geo_name
        order by total_population desc
        """,
    ),
    # Using the geography relationships provided by Cybersyn we collect all the
    # zip codes belonging to a city.
    View(
        name="zip_codes_in_city",
        columns=[
            ViewColumn(name="city_geo_id"),
            ViewColumn(name="city_geo_name"),
            ViewColumn(name="zip_geo_id"),
            ViewColumn(name="zip_geo_name"),
        ],
        query="""
        select 
            city.geo_id city_geo_id, 
            city.geo_name city_geo_name, 
            city.related_geo_id zip_geo_id, 
            city.related_geo_name zip_geo_name
        from us_addresses__poi.cybersyn.geography_relationships country
        join us_addresses__poi.cybersyn.geography_relationships city 
            on country.related_geo_id = city.geo_id
        where true
            and country.geo_id = 'country/USA'
            and city.level = 'City'
            and city.related_level = 'CensusZipCodeTabulationArea'
        order by city_geo_id
        """,
    ),
    View(
        name="weather_joined_with_major_cities",
        columns=[
            ViewColumn(name="geo_id"),
            ViewColumn(name="geo_name"),
            ViewColumn(name="total_population"),
            ViewColumn(name="avg_temperature_air_f"),
            ViewColumn(name="avg_relative_humidity_pct"),
            ViewColumn(name="avg_cloud_cover_pct"),
            ViewColumn(name="precipitation_probability_pct"),
        ],
        query="""
        select 
            city.geo_id, 
            city.geo_name, 
            city.total_population,
            avg(avg_temperature_air_f) avg_temperature_air_f,
            avg(avg_relative_humidity_pct) avg_relative_humidity_pct,
            avg(avg_cloud_cover_pct) avg_cloud_cover_pct,
            avg(precipitation_probability_pct) precipitation_probability_pct
        from major_us_cities city
        join zip_codes_in_city zip on city.geo_id = zip.city_geo_id
        join weather_forecast weather on zip.zip_geo_name = weather.postal_code
        group by city.geo_id, city.geo_name, city.total_population
        """,
    ),
    # Placeholder: Add new view definition here
]


# entry point for PythonAPI
root = Root(Session.builder.getOrCreate())

# create views in Snowflake
silver_schema = root.databases[f"quickstart_{os.environ['environment']}"].schemas["silver"]
silver_schema.user_defined_functions.create(
    map_city_to_airport, mode=CreateMode.or_replace
)
for view in pipeline:
    silver_schema.views.create(view, mode=CreateMode.or_replace)

View(
    name="attractions",
    columns=[
        ViewColumn(name="geo_id"),
        ViewColumn(name="geo_name"),
        ViewColumn(name="aquarium_cnt"),
        ViewColumn(name="zoo_cnt"),
        ViewColumn(name="korean_restaurant_cnt"),
    ],
    query="""
    select
        city.geo_id,
        city.geo_name,
        count(case when category_main = 'Aquarium' THEN 1 END) aquarium_cnt,
        count(case when category_main = 'Zoo' THEN 1 END) zoo_cnt,
        count(case when category_main = 'Korean Restaurant' THEN 1 END) korean_restaurant_cnt,
    from us_addresses__poi.cybersyn.point_of_interest_index poi
    join us_addresses__poi.cybersyn.point_of_interest_addresses_relationships poi_add 
        on poi_add.poi_id = poi.poi_id
    join us_addresses__poi.cybersyn.us_addresses address 
        on address.address_id = poi_add.address_id
    join major_us_cities city on city.geo_id = address.id_city
    where true
        and category_main in ('Aquarium', 'Zoo', 'Korean Restaurant')
        and id_country = 'country/USA'
    group by city.geo_id, city.geo_name
    """,
),
