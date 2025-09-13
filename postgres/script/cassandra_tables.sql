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
    tsunamiEventId INTEGER,
    volcanoEventId INTEGER,
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
    eqDepth DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabella earthquakes_by_total_damage
CREATE TABLE IF NOT EXISTS earthquakes_by_damage (
    damageAmountOrderTotal INTEGER NOT NULL,
    damageAmountOrder INTEGER,
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
    damageMillionsDollars DOUBLE PRECISION,
    damageMillionsDollarsTotal DOUBLE PRECISION,
    regionName VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabella earthquakes_by_deaths
CREATE TABLE IF NOT EXISTS earthquakes_by_deaths (
    deathsTotal INTEGER NOT NULL,
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
    deaths INTEGER,
    deathsAmountOrder INTEGER,
    deathsAmountOrderTotal INTEGER,
    regionName VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabella earthquakes_by_missing
CREATE TABLE IF NOT EXISTS earthquakes_by_missing (
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
    missing INTEGER,
    missingAmountOrder INTEGER,
    missingTotal INTEGER,
    missingAmountOrderTotal INTEGER NOT NULL,
    regionName VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabella earthquakes_by_injuries
CREATE TABLE IF NOT EXISTS earthquakes_by_injuries (
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
    injuries INTEGER,
    injuriesAmountOrder INTEGER,
    injuriesTotal INTEGER,
    injuriesAmountOrderTotal INTEGER NOT NULL,
    regionName VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabella earthquakes_by_houses_damages
CREATE TABLE IF NOT EXISTS earthquakes_by_houses_damages (
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
    housesDestroyed INTEGER,
    housesDestroyedAmountOrder INTEGER,
    housesDestroyedTotal INTEGER,
    housesDestroyedAmountOrderTotal INTEGER,
    housesDamaged INTEGER,
    housesDamagedAmountOrder INTEGER,
    housesDamagedTotal INTEGER,
    housesDamagedAmountOrderTotal INTEGER NOT NULL,
    regionName VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

