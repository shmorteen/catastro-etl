CREATE TABLE catastro_buildings (
    id SERIAL PRIMARY KEY,
    parcel_ref TEXT,
    building_type TEXT,
    description TEXT,
    built_area NUMERIC,
    staircase TEXT,
    floor TEXT,
    door TEXT,
    municipality TEXT,
    province TEXT,
    last_update TIMESTAMP
);
