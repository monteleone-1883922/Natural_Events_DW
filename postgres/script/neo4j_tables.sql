
-- Tabella States
CREATE TABLE IF NOT EXISTS states (
    fips_code INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabella Counties
CREATE TABLE IF NOT EXISTS counties (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    fips_code INTEGER NOT NULL,
    state_fips INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS independent_cities (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    fips_code INTEGER,
    region VARCHAR(100),
    state_fips INTEGER NOT NULL,
    county_fips INTEGER,
    parent_city_fips INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabella Tornadoes
CREATE TABLE tornadoes
(
    id                           INTEGER NOT NULL ,
    unique_id                    INTEGER PRIMARY KEY ,
    year                         INTEGER NOT NULL,
    month                        INTEGER,
    day                          INTEGER,
    date                         DATE,
    time                         VARCHAR(100),
    timeZone                    INTEGER,
    stateNumber                 INTEGER,
    f_scale                      INTEGER,
    injuries                     INTEGER,
    fatalities                   INTEGER,
    millionsDollarsDamage      NUMERIC(15, 2),
    millionDollarsCropsDamage NUMERIC(15, 2),
    latitudeStart               NUMERIC(9, 6),
    longitudeStart              NUMERIC(9, 6),
    latitudeEnd                 NUMERIC(9, 6),
    longitudeEnd                NUMERIC(9, 6),
    length                       INTEGER,
    width                        INTEGER,
    alteredMagnitude            INTEGER,
    state_fips                   INTEGER NOT NULL,
    created_at                   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE tornado_affected_counties
(
    tornado_id  INTEGER NOT NULL,
    county_fips INTEGER NOT NULL,
    order_idx   INTEGER NOT NULL,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (tornado_id, order_idx, county_fips)
);

CREATE TABLE tornado_affected_cities
(
    tornado_id INTEGER NOT NULL,
    city_fips  INTEGER NOT NULL,
    order_idx  INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (tornado_id, order_idx, city_fips)
);

CREATE TABLE traces
(
    id                           INTEGER PRIMARY KEY ,
    year                         INTEGER NOT NULL,
    month                        INTEGER,
    day                          INTEGER,
    date                         DATE,
    time                        VARCHAR(100),
    timeZone                    INTEGER,
    stateNumber                 INTEGER,
    f_scale                      INTEGER,
    injuries                     INTEGER,
    fatalities                   INTEGER,
    millionsDollarsDamage      NUMERIC(15, 2),
    millionDollarsCropsDamage NUMERIC(15, 2),
    latitudeStart               NUMERIC(9, 6),
    longitudeStart              NUMERIC(9, 6),
    latitudeEnd                 NUMERIC(9, 6),
    longitudeEnd                NUMERIC(9, 6),
    length                       INTEGER,
    width                        INTEGER,
    alteredMagnitude            INTEGER,
    state_fips                   INTEGER NOT NULL,
    tornado_id                   INTEGER NOT NULL,
    order_idx                    INTEGER NOT NULL,
    created_at                   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE trace_affected_counties
(
    trace_id    INTEGER NOT NULL,
    county_fips INTEGER NOT NULL,
    order_idx   INTEGER NOT NULL,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (trace_id, order_idx, county_fips)
);

CREATE TABLE trace_affected_cities
(
    trace_id   INTEGER NOT NULL,
    city_fips  INTEGER NOT NULL,
    order_idx  INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (trace_id, order_idx, city_fips)
);