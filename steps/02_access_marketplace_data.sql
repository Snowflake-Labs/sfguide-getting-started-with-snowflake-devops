USE ROLE ACCOUNTADMIN;

-- Verify marketplace data sets are accessible
SELECT * FROM oag_flight_emissions_data_sample.public.estimated_emissions_schedules_sample LIMIT 100;
SELECT * FROM oag_flight_status_data_sample.public.flight_status_latest_sample LIMIT 100;
SELECT * FROM global_weather__climate_data_for_bi.standard_tile.forecast_day LIMIT 100;
SELECT * FROM GLOBAL_GOVERNMENT.cybersyn.datacommons_timeseries LIMIT 100;
SELECT * FROM US_ADDRESSES__POI.cybersyn.point_of_interest_index LIMIT 100;
