CREATE TABLE catastro_parcels (
id SERIAL PRIMARY KEY,
referencia_catastral TEXT UNIQUE,
municipio TEXT,
codigo_municipio TEXT,
provincia TEXT,
codigo_provincia TEXT,
superficie_parcela NUMERIC,
uso_parcela TEXT,
geometry GEOMETRY(POLYGON, 4326),
last_update DATE
);