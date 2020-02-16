SELECT * FROM sqlite_master;

-- SELECT * FROM measurement WHERE prcp ISNULL; 

--SELECT * FROM imputed;

-- prototyping queries and joins for sqlalchemy
-- SELECT measurement.id, measurement.station, measurement.date, measurement.prcp, imputed.prcp, measurement.tobs
-- FROM measurement
-- INNER JOIN imputed ON imputed.id =measurement.id
-- WHERE measurement.prcp ISNULL;

-- -- Creating a view to play with so i don't wreck my clean table
-- DROP VIEW IF EXISTS complete_record;
-- CREATE VIEW complete_record AS	
--     SELECT measurement.id,
--            measurement.station,
--            measurement.date,
--            measurement.prcp,
--            imputed.prcp AS imp_prcp, 
--            measurement.tobs
--     FROM measurement LEFT JOIN imputed 
--         ON (imputed.id=measurement.id)
--         ORDER BY measurement.id;

-- --SELECT * FROM complete_record;

-- UPDATE measurement
--     SET prcp = (SELECT imputed.prcp 
--                 FROM imputed
--                 WHERE measurement.id = imputed.id)
--     WHERE measurement.prcp ISNULL;

-- DROP VIEW IF EXISTS final_measurment;
-- CREATE VIEW final_measurement AS	
--     SELECT measurement.id,
--            measurement.station,
--            measurement.date,
--            measurement.prcp,
           
--            measurement.tobs
--     FROM measurement LEFT JOIN imputed 
--         ON (imputed.id=measurement.id)
--         ORDER BY measurement.id;

SELECT * FROM final_measurement;