-- Tabella earthquakes_by_country
CREATE TABLE IF NOT EXISTS earthquakes_by_country (
    country VARCHAR(255) NOT NULL,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    hour INTEGER,
    minute INTEGER,
    second DOUBLE PRECISION,
    event_date_txt VARCHAR(255),
    event_date INTEGER,
    event_time VARCHAR(255),
    id INTEGER PRIMARY KEY,
    locationName VARCHAR(255),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    eqMagnitude DOUBLE PRECISION,
    intensity DOUBLE PRECISION,
    deathsAmountOrder INTEGER,
    damageAmountOrder INTEGER,
    regionName VARCHAR(255),
    area VARCHAR(255),
    eqDepth DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Creazione di indici per ottimizzare le query
-- CREATE INDEX idx_earthquakes_country_event_date ON earthquakes_by_country(country, event_date DESC);
-- CREATE INDEX idx_earthquakes_country_event_time ON earthquakes_by_country(country, event_time DESC);
-- CREATE INDEX idx_earthquakes_country_id ON earthquakes_by_country(country, id);

-- Tabella earthquakes_by_magnitude
CREATE TABLE IF NOT EXISTS earthquakes_by_magnitude (
    eqMagnitude FLOAT NOT NULL,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    hour INTEGER,
    minute INTEGER,
    second DOUBLE PRECISION,
    event_date_txt VARCHAR(255),
    event_date INTEGER,
    event_time VARCHAR(255),
    id INTEGER PRIMARY KEY,
    country VARCHAR(255),
    locationName VARCHAR(255),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    intensity DOUBLE PRECISION,
    deathsAmountOrder INTEGER,
    damageAmountOrder INTEGER,
    eqMagMs DOUBLE PRECISION,
    eqMagMl DOUBLE PRECISION,
    eqMagMw DOUBLE PRECISION,
    eqMagMb DOUBLE PRECISION,
    eqMagMfa DOUBLE PRECISION,
    eqMagUnk DOUBLE PRECISION,
    regionName VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indic per earthquakes_by_magnitude
-- CREATE INDEX idx_earthquakes_magnitude_event_date ON earthquakes_by_magnitude(eqMagnitude, event_date DESC);
-- CREATE INDEX idx_earthquakes_magnitude_event_time ON earthquakes_by_magnitude(eqMagnitude, event_time DESC);

-- Tabella earthquakes_by_damage
CREATE TABLE IF NOT EXISTS earthquakes_by_damage (
    damageAmountOrder INTEGER NOT NULL,
    deaths INTEGER,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    hour INTEGER,
    minute INTEGER,
    second DOUBLE PRECISION,
    event_date_txt VARCHAR(255),
    event_date INTEGER,
    event_time VARCHAR(255),
    id INTEGER PRIMARY KEY,
    country VARCHAR(255),
    locationName VARCHAR(255),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    eqMagnitude DOUBLE PRECISION,
    intensity DOUBLE PRECISION,
    missing INTEGER,
    missingAmountOrder INTEGER,
    injuries INTEGER,
    injuriesAmountOrder INTEGER,
    housesDestroyed INTEGER,
    housesDestroyedAmountOrder INTEGER,
    housesDamaged INTEGER,
    housesDamagedAmountOrder INTEGER,
    regionName VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indic per earthquakes_by_damage
-- CREATE INDEX idx_earthquakes_damage_deaths ON earthquakes_by_damage(damageAmountOrder, deaths DESC);
-- CREATE INDEX idx_earthquakes_damage_id ON earthquakes_by_damage(damageAmountOrder, id);

-- Tabella earthquakes_by_total_damage
CREATE TABLE IF NOT EXISTS earthquakes_by_total_damage (
    damageAmountOrderTotal INTEGER NOT NULL,
    deathsTotal INTEGER,
    country VARCHAR(255),
    locationName VARCHAR(255),
    year INTEGER,
    month INTEGER,
    day INTEGER,
    hour INTEGER,
    minute INTEGER,
    second DOUBLE PRECISION,
    event_date_txt VARCHAR(255),
    event_date INTEGER,
    event_time VARCHAR(255),
    id INTEGER PRIMARY KEY,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    eqMagnitude DOUBLE PRECISION,
    intensity DOUBLE PRECISION,
    tsunamiEventId INTEGER,
    volcanoEventId INTEGER,
    deaths INTEGER,
    deathsAmountOrder INTEGER,
    deathsAmountOrderTotal INTEGER,
    missing INTEGER,
    missingAmountOrder INTEGER,
    missingTotal INTEGER,
    missingAmountOrderTotal INTEGER,
    injuries INTEGER,
    injuriesAmountOrder INTEGER,
    injuriesTotal INTEGER,
    housesDestroyed INTEGER,
    housesDestroyedAmountOrder INTEGER,
    housesDestroyedTotal INTEGER,
    housesDestroyedAmountOrderTotal INTEGER,
    housesDamaged INTEGER,
    housesDamagedAmountOrder INTEGER,
    housesDamagedTotal INTEGER,
    housesDamagedAmountOrderTotal INTEGER,
    damageMillionsDollars DOUBLE PRECISION,
    damageMillionsDollarsTotal DOUBLE PRECISION,
    regionName VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indic per earthquakes_by_total_damage
-- CREATE INDEX idx_earthquakes_total_damage_deaths ON earthquakes_by_total_damage(damageAmountOrderTotal, deathsTotal DESC);
-- CREATE INDEX idx_earthquakes_total_damage_id ON earthquakes_by_total_damage(damageAmountOrderTotal, id);


-- Comments on tables for documentation
COMMENT ON TABLE earthquakes_by_country IS 'Table for earthquakes organized by country, sorted by date and time';
COMMENT ON TABLE earthquakes_by_magnitude IS 'Table for earthquakes organized by magnitude, sorted by date and time';
COMMENT ON TABLE earthquakes_by_damage IS 'Table for earthquakes organized by damage level, sorted by casualties';
COMMENT ON TABLE earthquakes_by_total_damage IS 'Table for earthquakes organized by total damage, sorted by total casualties';
