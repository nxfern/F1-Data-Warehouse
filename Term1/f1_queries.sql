-- Displays data_warehouse
SELECT * FROM data_warehouse;

-- Displays view for driver comparison between drivers from South America and the rest of the world based on race location
SELECT * FROM So_American_races_vs_others;

-- Display view for best constructors(brands) since 2000 (including 2000)
SELECT * FROM best_car_brand_since_2000;

-- Displays performance of drivers from nations since 2000 (including 2000)
SELECT * FROM driver_nat_perf_comp_since2000;

-- Display top driver per year, filters view to display only the top driver
SELECT year, driver_name, tot_pts FROM top_driver_per_year WHERE pts_rank = 1;

-- Display event list
SHOW EVENTS;

-- Display trigger list
SHOW TRIGGERS;

-- Trigger test
INSERT INTO status (statusId, status) VALUES(12345, 'madeup');

-- Shows all messages
SELECT * FROM message_log;