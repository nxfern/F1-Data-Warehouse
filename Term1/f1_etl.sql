USE ergastf1;

/* ------------------------
   Data Warehouse Procedure
   ------------------------ */
DROP PROCEDURE IF EXISTS CreateDataWarehouse;
DELIMITER //
CREATE PROCEDURE CreateDataWarehouse()
BEGIN
DROP TABLE IF EXISTS data_warehouse;
CREATE TABLE data_warehouse
SELECT
	re.raceId,
	ra.year,
    ra.name AS grand_prix,
    cir.name AS race_name,
    cir.country AS race_country,
	CONCAT(d.surname, ', ', d.forename) AS driver_name, -- Combines names into one column
	d.dob AS driver_dob,
    d.nationality AS driver_nat,
    con.name AS car_brand,
    con.nationality AS brand_nat,
    IF(re.positionOrder = 1, TRUE, FALSE) AS win, -- Creates a boolean column to denote if driver won race
    ROUND(re.points) AS pts_earned, -- Fixes aggregation rounding errors
    re.positionOrder AS fin_pos,
    re.laps AS tot_laps,
    MAX(p.stop) AS tot_pitstops, -- Shows total amount of pitstops made in a race
    DATE_FORMAT(SEC_TO_TIME(SUM(p.milliseconds) / 1000), '%i:%s.%f') AS tot_pit_time, -- Total time spent during race at pitstop MM:SS.mm
    DATE_FORMAT(SEC_TO_TIME((re.milliseconds/re.laps) / 1000), '%i:%s.%f') AS avg_lap_time, -- Calc avg lap time for driver per race MM:SS.mm
    SEC_TO_TIME(re.milliseconds / 1000) AS race_time, -- Calc total race time for driver per race HH:MM:SS
    re.milliseconds AS time_in_milliseconds,
    re.statusId,
    s.status
FROM results re
LEFT JOIN races ra ON re.raceId = ra.raceId
LEFT JOIN circuits cir ON ra.circuitId = cir.circuitId
LEFT JOIN drivers d ON re.driverId = d.driverId
LEFT JOIN constructors con ON re.constructorId = con.constructorId
LEFT JOIN pitstops p ON re.raceId = p.raceId
LEFT JOIN status s ON re.statusId = s.statusId
GROUP BY re.raceId, driver_name, driver_dob, driver_nat, car_brand, brand_nat, win, pts_earned, fin_pos, tot_laps, avg_lap_time, race_time, time_in_milliseconds, statusId
ORDER BY raceId;

END //
DELIMITER ;

/* -------------------
   Creating Data Marts
   ------------------- */
DROP PROCEDURE IF EXISTS CreateDataMarts;
DELIMITER //
CREATE PROCEDURE CreateDataMarts()
BEGIN

/* Data Mart to show whether South American
   drivers score better in races within
   their own continent compared drivers
   of other nationalities */
DROP VIEW IF EXISTS So_American_races_vs_others;
CREATE VIEW So_American_races_vs_others AS
SELECT
	driver_name,
    driver_nat,
    -- Rank displayed for South American races pts for drivers
    RANK() OVER(ORDER BY SUM(CASE WHEN race_country IN ('Argentina', 'Bolivia', 'Brazil', 'Chile', 'Colombia', 'Ecuador', 'Paraguay', 'Peru', 'Uruguay', 'Venezuela') THEN pts_earned ELSE 0 END) DESC) AS SoAm_DriverRank,
    -- Total points for driver amongst all South_American races
    SUM(CASE WHEN race_country IN ('Argentina', 'Bolivia', 'Brazil', 'Chile', 'Colombia', 'Ecuador', 'Paraguay', 'Peru', 'Uruguay', 'Venezuela') THEN pts_earned ELSE 0 END) AS tot_pts_SoAm, 
    -- Rank displayed for non-South American races pts for drivers
    RANK() OVER(ORDER BY SUM(CASE WHEN race_country NOT IN ('Argentina', 'Bolivia', 'Brazil', 'Chile', 'Colombia', 'Ecuador', 'Paraguay', 'Peru', 'Uruguay', 'Venezuela') THEN pts_earned ELSE 0 END) DESC) AS Other_DriverRank,
    -- Total points for drivers amongst all non-South American races
    SUM(CASE WHEN race_country NOT IN ('Argentina', 'Bolivia', 'Brazil', 'Chile', 'Colombia', 'Ecuador', 'Paraguay', 'Peru', 'Uruguay', 'Venezuela') THEN pts_earned ELSE 0 END) AS tot_pts_other    
FROM data_warehouse
GROUP BY driver_name, driver_nat
ORDER BY SoAm_DriverRank;

/* Data Mart to show which constructor(car) 
   brand has performed the best since 2000
   with a minimum 50 races participated*/
DROP VIEW IF EXISTS best_car_brand_since_2000;
CREATE VIEW best_car_brand_since_2000 AS
SELECT
	car_brand,
    COUNT(DISTINCT raceId) AS races_participated,
    SUM(win) AS wins, -- Total wins per brand
    SUM(pts_earned) AS tot_pts_earned, -- Total pts per brand
    ROUND(AVG(fin_pos), 2) AS avg_fin_pos -- Avg finish posiiton per brand rounded to 2 decimals
FROM data_warehouse WHERE year >= 2000
GROUP BY car_brand HAVING races_participated >= 50
ORDER BY avg_fin_pos;

/* Data Mart comparing race statistics for
   driver nationalities which nationality 
   of driver has performed the best since 2000
   where time data is not NULL and race status
   = finished */
DROP VIEW IF EXISTS driver_nat_perf_comp_since2000;
CREATE VIEW driver_nat_perf_comp_since2000 AS
SELECT
    driver_nat,
    SUM(win) AS wins, -- Cumulative wins
    SUM(pts_earned) AS tot_pts, -- Cumulative points
    DATE_FORMAT(SEC_TO_TIME((AVG(time_in_milliseconds)/AVG(tot_laps))/1000), '%i:%s.%f') AS nat_avg_lap_time,
    DATE_FORMAT(SEC_TO_TIME(AVG(time_in_milliseconds)/1000), '%H:%i:%s.%f') AS nat_avg_race_time
FROM data_warehouse WHERE time_in_milliseconds IS NOT NULL AND statusId = 1 AND year >= 2000
GROUP BY driver_nat
ORDER BY tot_pts DESC;

/* Data Mart to show the top point
   earning driver for each year.
   In operational layer the filter
   for rank is applied to display
   correct and necessary information */
DROP VIEW IF EXISTS top_driver_per_year;
CREATE VIEW top_driver_per_year AS
SELECT
	year,
    driver_name,
    SUM(pts_earned) AS tot_pts, -- Cumulative points earned 
    RANK() OVER(PARTITION BY year ORDER BY SUM(pts_earned) DESC) AS pts_rank -- Ranking the drivers by pts_earned
FROM data_warehouse
GROUP BY year, driver_name
ORDER BY year;

END //
DELIMITER ;

/* --------------------
   Creating Message Log
   -------------------- */
DROP TABLE IF EXISTS message_log;
CREATE TABLE message_log (message varchar(100) NOT NULL);

/* -----------------------------
   Creating Data Warehouse Event
   ----------------------------- */
-- Turning on global event scheduler for event
SET GLOBAL event_scheduler = ON;

-- Event
DROP EVENT IF EXISTS CreateEventDW;
DELIMITER $$
CREATE EVENT CreateEventDW
ON SCHEDULE EVERY 1 MINUTE -- Set to update every minute
STARTS CURRENT_TIMESTAMP -- Initializes when run
ENDS CURRENT_TIMESTAMP + INTERVAL 5 MINUTE -- Set to end 5 minutes after run for testing purposes
DO
	BEGIN
        INSERT INTO message_log SELECT CONCAT('event run: ', NOW()); -- Shows time event occurred via message_log
        CALL CreateDataWarehouse();
        CALL CreateDataMarts();
	END $$
DELIMITER ;

/* -----------------------------------------
   Creating triggers to check for insertions
   on the main tables from which the data
   warehouse is constructed
   ----------------------------------------- */
DROP TRIGGER IF EXISTS circuits_insert;
DROP TRIGGER IF EXISTS constructors_instert;
DROP TRIGGER IF EXISTS driver_insert;
DROP TRIGGER IF EXISTS pitstops_insert;
DROP TRIGGER IF EXISTS races_insert;
DROP TRIGGER IF EXISTS results_insert;
DROP TRIGGER IF EXISTS status_insert;
DELIMITER $$

CREATE TRIGGER circuits_insert
AFTER INSERT ON circuits
FOR EACH ROW
BEGIN
	INSERT INTO message_log SELECT CONCAT('circuits insertion: ', NOW()); -- Shows time trigger occurred via message log
END $$

CREATE TRIGGER constructors_insert
AFTER INSERT ON constructors
FOR EACH ROW
BEGIN
	INSERT INTO message_log SELECT CONCAT('constructors insertion: ', NOW()); -- Shows time trigger occurred via message log
END $$

CREATE TRIGGER drivers_insert
AFTER INSERT ON drivers
FOR EACH ROW
BEGIN
	INSERT INTO message_log SELECT CONCAT('drivers insertion: ', NOW()); -- Shows time trigger occurred via message log
END $$

CREATE TRIGGER pitstops_insert
AFTER INSERT ON pitstops
FOR EACH ROW
BEGIN
	INSERT INTO message_log SELECT CONCAT('pitstops insertion: ', NOW()); -- Shows time trigger occurred via message log
END $$

CREATE TRIGGER races_insert
AFTER INSERT ON races
FOR EACH ROW
BEGIN
	INSERT INTO message_log SELECT CONCAT('races insertion: ', NOW()); -- Shows time trigger occurred via message log
END $$

CREATE TRIGGER results_insert
AFTER INSERT ON results
FOR EACH ROW
BEGIN
	INSERT INTO message_log SELECT CONCAT('results insertion: ', NOW()); -- Shows time trigger occurred via message log
END $$

CREATE TRIGGER status_insert
AFTER INSERT ON status
FOR EACH ROW
BEGIN
	INSERT INTO message_log SELECT CONCAT('status insertion: ', NOW()); -- Shows time trigger occurred via message log
END $$

DELIMITER ;