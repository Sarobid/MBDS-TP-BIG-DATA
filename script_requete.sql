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