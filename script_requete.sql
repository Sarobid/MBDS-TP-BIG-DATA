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

