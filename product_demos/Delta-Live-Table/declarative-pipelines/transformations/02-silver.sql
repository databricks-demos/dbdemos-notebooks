-- Let's start cleaning up our raw bronze data into new streaming tables.

-- ==========================================================================
-- == STREAMING TABLE: rides                                               ==
-- ==========================================================================
CREATE OR REFRESH STREAMING TABLE rides (
  -- Streaming tables can infer the schema from the query, however you can specify it explicitly and include column descriptions when defining the table.
  ride_date DATE COMMENT "The date of the bike ride.",
  ride_id STRING COMMENT "Unique identifier for the ride.", 
  start_time TIMESTAMP COMMENT "Timestamp when the ride started.", 
  end_time TIMESTAMP COMMENT "Timestamp when the ride ended.", 
  start_station_id STRING COMMENT "Identifier for the station where the ride started.", 
  end_station_id STRING COMMENT "Identifier for the station where the ride ended.", 
  bike_id STRING COMMENT "Identifier for the bike used in the ride.", 
  user_type STRING COMMENT "Type of user (e.g., member, casual).", 
  ride_revenue DECIMAL(19,4) COMMENT "Calculated revenue for the ride based on duration and user type.",
  CONSTRAINT invalid_ride_duration EXPECT(DATEDIFF(MINUTE, start_time, end_time) > 0) ON VIOLATION DROP ROW
)
COMMENT "Streaming table containing processed ride data from bike shares."
AS SELECT
  DATE(start_time) AS ride_date, ride_id, start_time, end_time, start_station_id, end_station_id, bike_id, user_type,
  -- Calculate ride revenue by taking the ride duration in fractional hours and multiplying it by the ride rate.
  -- Ride rates for members are 10 dollars per hour and 15 dollars per hour for non members.
  CASE WHEN user_type = "member"
    THEN (DATEDIFF(MINUTE, start_time, end_time) / 60.0) * 10.0
    ELSE (DATEDIFF(MINUTE, start_time, end_time) / 60.0) * 15.0
  END :: DECIMAL(19,4) AS ride_revenue
FROM STREAM (rides_raw);


-- ==========================================================================
-- == STREAMING TABLE: maintenance_logs                                    ==
-- ==========================================================================
CREATE OR REFRESH STREAMING TABLE maintenance_logs (
  maintenance_date DATE COMMENT "The date the maintenance was reported.", 
  maintenance_id STRING COMMENT "Unique identifier for the maintenance log entry.", 
  bike_id STRING COMMENT "Identifier for the bike that underwent maintenance.", 
  reported_time TIMESTAMP COMMENT "Timestamp when the maintenance issue was reported.", 
  resolved_time DATE COMMENT "Date when the maintenance issue was resolved.", 
  issue_description STRING COMMENT "Text description of the maintenance issue.", 
  issue_type STRING COMMENT "AI-classified type of the maintenance issue (e.g., brakes, tires).",
  -- We can add constraints to tables to filter out bad data, log it or even fail the pipeline when the data comes in.
  -- Throw out rows with missing issue descriptions
  CONSTRAINT no_maintenance_description EXPECT(issue_description IS NOT NULL) ON VIOLATION DROP ROW,
  -- Log rows with short issue descriptions but don't drop them
  CONSTRAINT short_maintenance_description EXPECT(LEN(issue_description) > 10)
)
COMMENT "Streaming table containing processed maintenance logs for bikes, including AI classified issue types."
AS SELECT
  DATE(reported_time) AS maintenance_date, maintenance_id, bike_id, reported_time, resolved_time, issue_description,
  -- Use AI_CLASSIFY to classify issues into specific categories using the description. Take a look at the exploration notebook for more details on how this function works.
  AI_CLASSIFY(issue_description, ARRAY("brakes", "chains_pedals", "tires", "other")) AS issue_type
FROM STREAM (maintenance_logs_raw);


-- ==========================================================================
-- == STREAMING TABLE: weather                                             ==
-- ==========================================================================
CREATE OR REFRESH STREAMING TABLE weather (
  weather_date DATE COMMENT "The date for which the weather data is recorded.", 
  temperature DOUBLE COMMENT "Temperature in Fahrenheit.", 
  rainfall DOUBLE COMMENT "Rainfall in inches.", 
  wind_speed DOUBLE COMMENT "Wind speed in miles per hour."
)
COMMENT "Streaming table containing processed weather data, converted to standard units."
AS SELECT
  DATE(FROM_UNIXTIME(`timestamp`)) AS weather_date,
  temperature_f AS temperature,
  rainfall_in AS rainfall,
  wind_speed_mph AS wind_speed
FROM STREAM (weather_raw);

-- Next up lets build some aggregations for our dashboard over in 03-gold.sql.