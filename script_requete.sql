--- Il faut harmoniser le données region de température
CREATE OR REPLACE VIEW temperature_h_ext_harm AS
SELECT
  CASE
    WHEN region = 'Australia/South Pacific' THEN 'Oceania'
    WHEN region = 'South/Central America & Carribean' THEN 'South America'
    WHEN region = 'Middle East' THEN 'Asia'
    ELSE region
  END AS region,
  country,
  year,
  avgtemperature
FROM temperature_h_ext;

-- - Il faut catégoriser les mesures prises

SELECT 
    cph.policy_type,
    cph.sector,
    cph.pays AS country,
    cph.year,
    th.avgTemperature,
    COUNT(*) AS policy_count
FROM climate_policies_hive cph
JOIN TEMPERATURE_H_EXT th
    ON cph.pays = th.country AND cph.year = th.year
GROUP BY cph.policy_type, cph.sector, cph.pays, cph.year, th.avgTemperature
ORDER BY cph.year, cph.pays, cph.policy_type, cph.sector;

-- - Liaison entre température et catastrophe
--  	- liaison entre l'augmentation de la température et la gravité/ fréquence des catastrophes naturelles
-- 	- Correlation entre la vitesse de l'augmentation de la température et la gravité des catastrophes naturelles
SELECT
  t1.region,
  t1.year,
  (t1.avgTemperature - t2.avgTemperature) AS temp_increase,
  d.EventName,
  sg.Name AS subgroup,
  dt.Name AS type,
  dst.Name AS subtype,
  COUNT(d.DisasterID) AS disaster_count
FROM
  TEMPERATURE_H_EXT t1
LEFT JOIN TEMPERATURE_H_EXT t2
  ON t1.region = t2.region AND t1.year = t2.year + 1
LEFT JOIN Region r
  ON t1.region = r.Name
LEFT JOIN Disaster d
  ON d.RegionID = r.RegionID AND d.Year = t1.year
LEFT JOIN DisasterSubgroup sg
  ON d.DisasterSubgroupID = sg.DisasterSubgroupID
LEFT JOIN DisasterType dt
  ON d.DisasterTypeID = dt.DisasterTypeID
LEFT JOIN DisasterSubtype dst
  ON d.DisasterSubtypeID = dst.DisasterSubtypeID
WHERE dt.Name is not null 
GROUP BY
  t1.region, t1.year, t1.avgTemperature, t2.avgTemperature, d.EventName, sg.Name, dt.Name, dst.Name
ORDER BY
  t1.region, t1.year
LIMIT 50;



-- - Efficacité des mesures prises
-- 	- gravité des catastrophes avant et après mesures
-- 	- quelles catégories de catastrophes sont moins graves / moins fréquente après les mesures

SELECT
  c.Name AS country,
  CASE
    WHEN d.Year < p.first_policy_year THEN 'before'
    ELSE 'after'
  END AS policy_period,
  sg.Name AS disaster_subgroup,
  dt.Name AS disaster_type,
  COUNT(d.DisasterID) AS disaster_count
FROM
  Disaster d
JOIN Country c ON d.CountryID = c.CountryID
JOIN DisasterSubgroup sg ON d.DisasterSubgroupID = sg.DisasterSubgroupID
JOIN DisasterType dt ON d.DisasterTypeID = dt.DisasterTypeID
JOIN (
    SELECT pays, MIN(year) AS first_policy_year
    FROM climate_policies_hive
    GROUP BY pays
) p ON c.Name = p.pays
GROUP BY
  c.Name,
  CASE WHEN d.Year < p.first_policy_year THEN 'before' ELSE 'after' END,
  sg.Name,
  dt.Name
ORDER BY
  c.Name, policy_period, sg.Name, dt.Name limit 50;


--- - Comment change la consommation d'énergie change par région après des catastrophes => quelles infrastructures de consommation sont les plus touchées
---	  - énergie la plus utilisée , énergie la moins utilisée
with  v_country_energy_consumption as
(select 
       country,
       year,
       region,
       greatest(hydro_twh,solar_twh,wind_twh) as max_renewable,
       least(hydro_twh,solar_twh,wind_twh) as min_renewable,
       greatest(coal_ton,gas_m3,oil_m3) as max_fossil,
       least(coal_ton,gas_m3,oil_m3) as min_fossil
from energy_consumptions)
select
       disasterDetails.* ,
       vec1.year as previous_year,
       vec1.max_renewable as previous_max_renewable,
       vec1.min_renewable as previous_min_renewable,
       vec1.max_fossil as previous_max_fossil,
       vec1.min_fossil as previous_min_fossil,
       vec2.year as next_year,
       vec2.max_renewable as next_max_renewable,
       vec2.min_renewable as next_min_renewable,
       vec2.max_fossil as next_max_fossil,
       vec2.min_fossil as next_min_fossil
from 
(
       select 
              disaster.year,
              region.name as region_name,
              country.name as country_name
       from disaster
       join region on region.regionid = disaster.regionid
       join country on country.countryid = disaster.countryid
group by 
       disaster.year,
       region.name,
       country.name
) as disasterDetails
left join v_country_energy_consumption vec1 on 
       disasterDetails.country_name = vec1.country and
       disasterDetails.year-1 = vec1.year
left join v_country_energy_consumption vec2 on 
       disasterDetails.country_name = vec2.country and
       disasterDetails.year+1 = vec2.year
where vec1.year is not null and 
      vec2.year is not null;

--- - Correlation entre emplacement géographique et mesures prises
--- 	- quelles mesures marchent mieux sur quelle région ?
select disasterDetails.*,
       policies.policy_type,
       policies.policy_count
from
(
       select 
              disaster.year,
              country.name as country_name,
              count(*) as disaster_count
       from disaster
       join region on region.regionid = disaster.regionid
       join country on country.countryid = disaster.countryid
group by 
       disaster.year,
       country.name
) as disasterDetails
left join 
(
  select 
        year,
         pays,
         policy_type,
         count(*) as policy_count
  from climate_policies_hive cp
  group by year,pays,policy_type
) as policies
on policies.pays = disasterDetails.country_name and
   policies.year = disasterDetails.year


--. Quelles types de catastrophes ont tendance à se passer selon un intervalle de température ?--
---	- température moyenne => quelles type de catategorie 
SELECT 
  dt.name AS disaster_type,
  MIN(te.avgTemperature) as min_temp,
  MAX(te.avgTemperature) as max_temp
FROM 
  disaster d
  join country c on d.countryid = c.countryid
JOIN temperature_h_ext te ON c.name = te.country AND d.year = te.year
JOIN disastertype dt ON d.disastertypeid = dt.disastertypeid
WHERE te.avgtemperature IS NOT NULL
GROUP BY dt.name

---Quelles types de mesures agissent le plus vite vs le plus lentement ?
--	- combien d'années faut il attendre avant de voir que les mesures marchent ?
SELECT 
  cp.policy_type,
  AVG(te.year - cp.year) AS avg_delay_years
FROM 
  climate_policies_hive cp
JOIN energy_consumptions te 
  ON cp.pays = te.country 
  AND te.year BETWEEN cp.year AND cp.year + 5
WHERE 
  te.gas_emissions_ton < (
    SELECT MIN(gas_emissions_ton)
    FROM energy_consumptions e2
    WHERE e2.country = cp.pays AND e2.year = cp.year
  )
GROUP BY cp.policy_type
ORDER BY avg_delay_years;


 --Réactivité des organismes internationaux selon les mesures
 --- comparaison entre les mesures et les catastrophes qu'elles sont censées limiter => combien d'années / mois de différence , avant ou après les catastrophes

SELECT 
  dt.name AS disaster_type,
  cp.policy_type,
  AVG(cp.year - d.year) AS avg_delay_years
FROM 
  disaster d
JOIN disastertype dt 
  ON d.disastertypeid = dt.disastertypeid
JOIN country c 
  ON d.countryid = c.countryid
JOIN climate_policies_hive cp 
  ON cp.pays = c.name 
  AND ABS(cp.year - d.year) <= 5
GROUP BY dt.name, cp.policy_type
ORDER BY avg_delay_years;


----Region où une forte augmentation de la consommation d'énergie fossile provoque des catastrophes naturelles
CREATE OR REPLACE VIEW v_energy_vvw_disaster_detailsariation_catastrophe AS
select
    disaster.year,
    region.name as region_name,
    country.name as country_name,
    DisasterType.name as disaster_type,
    DisasterSubtype.name as disaster_subtype,
    disasterSubgroup.name as disaster_subgroup
from
    disaster
    join disasterSubgroup on disaster.disastersubgroupid = disaster.disastersubgroupid
    join DisasterType on DisasterType.disastertypeid = disaster.disastertypeid
    join DisasterSubtype on DisasterSubtype.disastersubtypeid = disaster.disastersubtypeid
    join region on region.regionid = disaster.regionid
    join country on country.countryid = disaster.countryid ;

CREATE OR REPLACE VIEW v_energy_variation_catastrophe AS
WITH fossil_energy_per_year AS (
  SELECT
    CASE
        WHEN region = 'Europe/Asia' THEN 'Europe'
        ELSE region
    END AS region,
    year,
    SUM(coal_ton + gas_m3 + oil_m3) AS total_fossil
FROM energy_consumptions
GROUP BY
    CASE
        WHEN region = 'Europe/Asia' THEN 'Europe'
        ELSE region
    END,
    year
),
disasters_per_region_year AS (
  SELECT
    CASE
        WHEN region_name IN ('Caribbean', 'Central America', 'Northern America') THEN 'North America'
        WHEN region_name IN ('Eastern Asia', 'South-Eastern Asia', 'Southern Asia') THEN 'Asia'
        WHEN region_name IN ('Western Africa') THEN 'Africa'
        WHEN region_name IN ('Western Europe') THEN 'Europe'
        ELSE 'Other'
    END AS region,
    year,
    COUNT(CASE WHEN disaster_subgroup = 'Biological' THEN 1 END) AS biological_disasters,
    COUNT(CASE WHEN disaster_subgroup = 'Climatological' THEN 1 END) AS climatological_disasters,
    COUNT(CASE WHEN disaster_subgroup = 'Geophysical' THEN 1 END) AS geophysical_disasters,
    COUNT(CASE WHEN disaster_subgroup = 'Hydrological' THEN 1 END) AS hydrological_disasters,
    COUNT(CASE WHEN disaster_subgroup = 'Meteorological' THEN 1 END) AS meteorological_disasters,
    COUNT(*) AS total_disasters
  FROM vw_disaster_details
  GROUP BY CASE
        WHEN region_name IN ('Caribbean', 'Central America', 'Northern America') THEN 'North America'
        WHEN region_name IN ('Eastern Asia', 'South-Eastern Asia', 'Southern Asia') THEN 'Asia'
        WHEN region_name IN ('Western Africa') THEN 'Africa'
        WHEN region_name IN ('Western Europe') THEN 'Europe'
        ELSE 'Other' END
    , year
)
SELECT
  f.region as region,
  f.year as year,
  f.total_fossil as total_fossil,
  d.total_disasters as total_disasters,
  d.biological_disasters as biological_disasters,
  d.climatological_disasters as climatological_disasters,
  d.geophysical_disasters as geophysical_disasters,
  d.hydrological_disasters as hydrological_disasters,
  d.meteorological_disasters as meteorological_disasters
FROM fossil_energy_per_year f
JOIN disasters_per_region_year d ON f.region = d.region AND f.year = d.year
ORDER BY f.region, f.year;
