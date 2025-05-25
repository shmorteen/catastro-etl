CREATE TABLE catastro_units (
    -- Unique identifiers
    parcel_ref           TEXT,  -- Concatenated pc1+pc2
    unit_ref             TEXT,  -- 'cn' field, if available
    
    -- Use and building data
    use_type             TEXT,  -- debi.luso
    floor_area           NUMERIC, -- debi.sfc
    year_built           INTEGER, -- debi.ant
    participation        NUMERIC, -- debi.cpt

    -- Location / Address
    street_name          TEXT,  -- dt.locs.lous.lourb.dir.nv
    street_type          TEXT,  -- dt.locs.lous.lourb.dir.tv
    street_code          TEXT,  -- dt.locs.lous.lourb.dir.cv
    portal_number        TEXT,  -- dt.locs.lous.lourb.dir.pnp
    portal_suffix        TEXT,  -- dt.locs.lous.lourb.dir.snp
    floor                TEXT,  -- dt.locs.lous.lourb.loint.pt
    door                 TEXT,  -- dt.locs.lous.lourb.loint.pu
    staircase            TEXT,  -- dt.locs.lous.lourb.loint.es
    postal_code          TEXT,  -- dt.locs.lous.lourb.dp
    address_code         TEXT,  -- dt.locs.lous.lourb.dm

    -- Administrative info
    province             TEXT,  -- dt.np
    municipality         TEXT,  -- dt.nm
    municipality_code    TEXT,  -- dt.loine.cm
    province_code        TEXT,  -- dt.loine.cp
    cadastral_municipio  TEXT,  -- dt.cmc

    -- Additional raw fields from API (optional)
    car                  TEXT,  -- rc.car
    cc1                  TEXT,  -- rc.cc1
    cc2                  TEXT,  -- rc.cc2
    raw_address_text     TEXT,  -- bi.ldt

    -- Optional service provider info from bico.lspr
    spr_code             TEXT,  -- lspr[*].cspr
    spr_type_code        TEXT,  -- dspr.ccc
    spr_type_desc        TEXT,  -- dspr.dcc
    spr_url              TEXT,  -- dspr.ip
    spr_entity_name      TEXT,  -- dspr.ssp

    -- Timestamps
    last_update          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Geometry placeholder (optional, if coordinate mapping available)
    geometry                 geometry(Point, 25830)
);
