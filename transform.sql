BEGIN;
-- Add processed column if missing
ALTER TABLE raw_traffic_json ADD COLUMN IF NOT EXISTS processed BOOLEAN DEFAULT FALSE;

-- Begin transaction with error handling
DO $$
BEGIN


    -- Alter confidence column type if exists
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'traffic_flow' AND column_name = 'confidence') THEN
        ALTER TABLE traffic_flow ALTER COLUMN confidence TYPE DECIMAL(5,2);
    END IF;

    -- Create normalized tables
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
        confidence DECIMAL(5,2),
        road_closure BOOLEAN,
        timestamp TIMESTAMP,
        UNIQUE(location_id, timestamp)
    );

    -- Process only new data
    WITH raw_data AS (
        SELECT raw_json, created_at 
        FROM raw_traffic_json 
        WHERE raw_json IS NOT NULL AND processed = FALSE
        LIMIT 1000
    )
    INSERT INTO locations (latitude, longitude, name)
    SELECT 
        (raw_json->'flowSegmentData'->'coordinates'->'coordinate'->0->>'latitude')::DECIMAL(9,6),
        (raw_json->'flowSegmentData'->'coordinates'->'coordinate'->0->>'longitude')::DECIMAL(9,6),
        'Downtown Seattle'
    FROM raw_data
    ON CONFLICT (latitude, longitude) DO NOTHING;

    WITH raw_data AS (
        SELECT raw_json, created_at 
        FROM raw_traffic_json 
        WHERE raw_json IS NOT NULL AND processed = FALSE
        LIMIT 1000
    )
    INSERT INTO traffic_flow (
        location_id, current_speed, free_flow_speed, 
        current_travel_time, free_flow_travel_time, 
        confidence, road_closure, timestamp
    )
    SELECT 
        l.id,
        (r.raw_json->'flowSegmentData'->>'currentSpeed')::DECIMAL(6,2),
        (r.raw_json->'flowSegmentData'->>'freeFlowSpeed')::DECIMAL(6,2),
        (r.raw_json->'flowSegmentData'->>'currentTravelTime')::INTEGER,
        (r.raw_json->'flowSegmentData'->>'freeFlowTravelTime')::INTEGER,
        (r.raw_json->'flowSegmentData'->>'confidence')::DECIMAL(5,2),
        (r.raw_json->'flowSegmentData'->>'roadClosure')::BOOLEAN,
        r.created_at
    FROM raw_data r
    JOIN locations l ON 
        l.latitude = (r.raw_json->'flowSegmentData'->'coordinates'->'coordinate'->0->>'latitude')::DECIMAL(9,6) AND
        l.longitude = (r.raw_json->'flowSegmentData'->'coordinates'->'coordinate'->0->>'longitude')::DECIMAL(9,6)
    ON CONFLICT (location_id, timestamp) DO NOTHING;

    -- Mark as processed instead of deleting
    UPDATE raw_traffic_json 
    SET processed = TRUE 
    WHERE raw_json IS NOT NULL AND processed = FALSE;

    COMMIT;
    RAISE NOTICE 'Successfully processed % rows', (SELECT COUNT(*) FROM raw_traffic_json WHERE processed = TRUE);
EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'Transformer failed: %', SQLERRM;
    ROLLBACK;
END $$;
