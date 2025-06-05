CREATE DATABASE global_warming_db;
USE global_warming_db;

CREATE TABLE energy_consumptions (
  country STRING,
  region STRING,
  hydro_twh DOUBLE,
  solar_twh DOUBLE,
  wind_twh DOUBLE,
  coal_ton DOUBLE,
  gas_m3 DOUBLE,
  oil_m3 DOUBLE,
  nuclear_twh DOUBLE,
  gas_emissions_ton DOUBLE,
  population INT
)
PARTITIONED BY (year INT)
STORED AS PARQUET;