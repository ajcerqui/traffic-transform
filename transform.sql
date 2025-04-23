-- Begin transaction
BEGIN;

-- Add processed column if missing (keep this)
ALTER TABLE raw_traffic_json ADD COLUMN IF NOT EXISTS processed BOOLEAN DEFAULT FALSE;

DO $$
BEGIN
    -- Check if we need to modify the confidence column
    IF EXISTS (SELECT 1 FROM information_schema.columns 
              WHERE table_name = 'traffic_flow' AND column_name = 'confidence'
              AND data_type != 'decimal(5,2)') THEN
              
        -- Drop dependent views temporarily
        DROP VIEW IF EXISTS public.traffic_weather;
        DROP VIEW IF EXISTS api.traffic_current;
        
        -- Then alter the column
        ALTER TABLE traffic_flow ALTER COLUMN confidence TYPE DECIMAL(5,2);
        
        -- Recreate views (replace these with your actual view definitions)
        CREATE OR REPLACE VIEW public.traffic_weather AS
        SELECT tf.*, w.weather_condition 
        FROM traffic_flow tf
        JOIN weather_data w ON tf.timestamp = w.timestamp;
        
        CREATE OR REPLACE VIEW api.traffic_current AS
        SELECT * FROM traffic_flow
        WHERE timestamp > NOW() - INTERVAL '24 hours';
    END IF;

    -- Rest of your transformation logic here
    WITH raw_data AS (
        SELECT raw_json, created_at 
        FROM raw_traffic_json 
        WHERE processed = FALSE
        LIMIT 1000
    )
    INSERT INTO locations (latitude, longitude, name)
    SELECT 
        (raw_json->'flowSegmentData'->'coordinates'->'coordinate'->0->>'latitude')::DECIMAL(9,6),
        (raw_json->'flowSegmentData'->'coordinates'->'coordinate'->0->>'longitude')::DECIMAL(9,6),
        'Downtown Seattle'
    FROM raw_data
    ON CONFLICT (latitude, longitude) DO NOTHING;

    -- Continue with your existing transformation logic
    WITH raw_data AS (
        SELECT raw_json, created_at 
        FROM raw_traffic_json 
        WHERE processed = FALSE
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

    -- Mark as processed
    UPDATE raw_traffic_json 
    SET processed = TRUE 
    WHERE processed = FALSE;

    RAISE NOTICE 'Successfully processed % rows', (SELECT COUNT(*) FROM raw_traffic_json WHERE processed);
EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'Transformer failed: %', SQLERRM;
END
$$ LANGUAGE plpgsql;

COMMIT;
