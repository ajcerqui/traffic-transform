-- Begin transaction
BEGIN;

-- Create normalized tables if they don't exist
CREATE TABLE IF NOT EXISTS locations (
    id SERIAL PRIMARY KEY,
    latitude DECIMAL(9,6) NOT NULL,
    longitude DECIMAL(9,6) NOT NULL,
    name VARCHAR(255),
    UNIQUE(latitude, longitude)
);

CREATE TABLE IF NOT EXISTS traffic_flow (
    id SERIAL PRIMARY KEY,
    location_id INTEGER REFERENCES locations(id),
    current_speed DECIMAL(6,2),
    free_flow_speed DECIMAL(6,2),
    current_travel_time INTEGER,
    free_flow_travel_time INTEGER,
    confidence INTEGER,
    road_closure BOOLEAN,
    timestamp TIMESTAMP,
    UNIQUE(location_id, timestamp)
);

-- Extract data from JSON and insert into locations table
WITH raw_data AS (
    SELECT raw_json, created_at, COALESCE(location_name, 'Downtown Seattle') AS location_name 
    FROM raw_traffic_json 
    WHERE raw_json IS NOT NULL
)
INSERT INTO locations (latitude, longitude, name)
SELECT 
    (raw_json->'flowSegmentData'->'coordinates'->'coordinate'->0->>'latitude')::DECIMAL(9,6) AS latitude,
    (raw_json->'flowSegmentData'->'coordinates'->'coordinate'->0->>'longitude')::DECIMAL(9,6) AS longitude,
    location_name AS name
FROM raw_data
ON CONFLICT (latitude, longitude) DO NOTHING;

-- Insert data into traffic_flow table
WITH raw_data AS (
    SELECT raw_json, created_at FROM raw_traffic_json WHERE raw_json IS NOT NULL
)
INSERT INTO traffic_flow (
    location_id, current_speed, free_flow_speed, 
    current_travel_time, free_flow_travel_time, 
    confidence, road_closure, timestamp
)
SELECT 
    l.id AS location_id,
    (r.raw_json->'flowSegmentData'->>'currentSpeed')::DECIMAL(6,2) AS current_speed,
    (r.raw_json->'flowSegmentData'->>'freeFlowSpeed')::DECIMAL(6,2) AS free_flow_speed,
    (r.raw_json->'flowSegmentData'->>'currentTravelTime')::INTEGER AS current_travel_time,
    (r.raw_json->'flowSegmentData'->>'freeFlowTravelTime')::INTEGER AS free_flow_travel_time,
    (r.raw_json->'flowSegmentData'->>'confidence')::INTEGER AS confidence,
    (r.raw_json->'flowSegmentData'->>'roadClosure')::BOOLEAN AS road_closure,
    r.created_at AS timestamp
FROM raw_data r
JOIN locations l ON 
    l.latitude = (r.raw_json->'flowSegmentData'->'coordinates'->'coordinate'->0->>'latitude')::DECIMAL(9,6) AND
    l.longitude = (r.raw_json->'flowSegmentData'->'coordinates'->'coordinate'->0->>'longitude')::DECIMAL(9,6)
ON CONFLICT (location_id, timestamp) DO NOTHING;

-- Clean up raw data after successful transformation
DELETE FROM raw_traffic_json WHERE raw_json IS NOT NULL;

-- Commit the transaction
COMMIT;