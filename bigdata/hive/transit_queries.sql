CREATE TABLE IF NOT EXISTS transit_trips (
  service_date DATE, hour INT, route_id STRING, route_name STRING,
  stop_id STRING, stop_name STRING, passenger_count INT,
  boarding_count INT, alighting_count INT, vehicle_id STRING,
  vehicle_capacity INT, trip_id STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

-- Passenger totals by route
SELECT route_id, SUM(passenger_count) passengers FROM transit_trips GROUP BY route_id ORDER BY passengers DESC;
-- Busiest hours
SELECT hour, SUM(passenger_count) passengers FROM transit_trips GROUP BY hour ORDER BY passengers DESC;
-- Daily passenger statistics
SELECT service_date, SUM(passenger_count) passengers FROM transit_trips GROUP BY service_date ORDER BY service_date;
-- Crowded routes (capacity ratio)
SELECT route_id, AVG(passenger_count / vehicle_capacity) occupancy_ratio FROM transit_trips WHERE vehicle_capacity > 0 GROUP BY route_id ORDER BY occupancy_ratio DESC;
-- Passenger demand by weekday
SELECT dayofweek(service_date), SUM(passenger_count) passengers FROM transit_trips GROUP BY dayofweek(service_date);
