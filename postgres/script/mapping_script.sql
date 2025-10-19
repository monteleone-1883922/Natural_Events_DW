

CREATE OR REPLACE VIEW integration_db.public.enriched_traces AS
    SELECT traces.id AS id,
           traces.injuries AS injuries,
           traces.fatalities AS deaths,
           traces.millionsdollarsdamage AS damagemillionsdollars,
           traces.latitudestart AS latitude,
           traces.longitudestart AS longitude,
           traces.state_fips AS state_fips,
           concat(date, ' ', time) AS event_timestamp,
           traces.millionsdollarsdamage AS damagemillionsdollarstotal,
           CASE WHEN (traces.millionsdollarsdamage > 25) THEN
               4
           WHEN (traces.millionsdollarsdamage > 5) THEN
               3
           WHEN (traces.millionsdollarsdamage > 1) THEN
               2
           WHEN (traces.millionsdollarsdamage > 0) THEN
               1
           ELSE
               0
           END AS damageamountorder,
           CASE WHEN (traces.millionsdollarsdamage > 25) THEN
               4
           WHEN (traces.millionsdollarsdamage > 5) THEN
               3
           WHEN (traces.millionsdollarsdamage > 1) THEN
               2
           WHEN (traces.millionsdollarsdamage > 0) THEN
               1
           ELSE
               0
           END AS damageamountordertotal,
           traces.injuries AS injuriestotal,
           CASE WHEN (traces.injuries > 1000) THEN
               4
           WHEN (traces.injuries > 100) THEN
               3
           WHEN (traces.injuries > 50) THEN
               2
           WHEN (traces.injuries > 0) THEN
               1
           ELSE
               0
           END AS injuriesamountorder,
           CASE WHEN (traces.injuries > 1000) THEN
               4
           WHEN (traces.injuries > 100) THEN
               3
           WHEN (traces.injuries > 50) THEN
               2
           WHEN (traces.injuries > 0) THEN
               1
           ELSE
               0
           END AS injuriesamountordertotal,
           0 AS missing,
           0 AS missingtotal,
           0 AS missingamountordertotal,
           0 AS missingamountorder,
           0 AS housesdestroyed,
           0 AS housesdestroyedtotal,
           0 AS housesdestroyedamountorder,
           0 AS housesdestroyedamountordertotal,
           traces.fatalities AS deathstotal,
           CASE WHEN (traces.fatalities > 1000) THEN
               4
           WHEN (traces.fatalities > 100) THEN
               3
           WHEN (traces.fatalities > 50) THEN
               2
           WHEN (traces.fatalities > 0) THEN
               1
           ELSE
               0
           END AS deathsamountorder,
           CASE WHEN (traces.fatalities > 1000) THEN
               4
           WHEN (traces.fatalities > 100) THEN
               3
           WHEN (traces.fatalities > 50) THEN
               2
           WHEN (traces.fatalities > 0) THEN
               1
           ELSE
               0
           END AS deathsamountordertotal,
           'USA' AS country,
           'North America and Hawaii' AS regionname,
           0 AS housesdamaged,
           0 AS housesdamagedtotal,
           0 AS housesdamagedamountordertotal,
           0 AS housesdamagedamountorder,
           traces.year AS year,
           traces.month AS month,
           traces.day AS day,
           cast(substring(time FROM 1 FOR 2) AS INTEGER) AS hour,
           cast(substring(time FROM 4 FOR 2) AS INTEGER) AS minute,
           cast(NULL AS DOUBLE PRECISION) AS second,
           traces.milliondollarscropsdamage AS milliondollarscropsdamage
    FROM integration_db.public.traces as traces;

CREATE OR REPLACE VIEW integration_db.public.first_trace_affected_counties AS
    SELECT trace_affected_counties.trace_id AS trace_id,
           trace_affected_counties.county_fips AS county_fips
    FROM integration_db.public.trace_affected_counties as trace_affected_counties
    WHERE order_idx = 1;

CREATE OR REPLACE VIEW integration_db.public.first_trace_affected_cities AS
    SELECT trace_affected_cities.trace_id AS trace_id,
           trace_affected_cities.city_fips AS city_fips
    FROM integration_db.public.trace_affected_cities as trace_affected_cities
    WHERE order_idx = 1;

CREATE OR REPLACE VIEW integration_db.public.enriched_traces_w_counties AS
    SELECT enriched_traces.id AS id,
           enriched_traces.injuries AS injuries,
           enriched_traces.deaths AS deaths,
           enriched_traces.damagemillionsdollars AS damagemillionsdollars,
           enriched_traces.latitude AS latitudestart,
           enriched_traces.longitude AS longitudestart,
           enriched_traces.state_fips AS state_fips,
           enriched_traces.event_timestamp AS event_timestamp,
           enriched_traces.damagemillionsdollarstotal AS damagemillionsdollarstotal,
           enriched_traces.damageamountorder AS damageamountorder,
           enriched_traces.damageamountordertotal AS damageamountordertotal,
           enriched_traces.injuriestotal AS injuriestotal,
           enriched_traces.injuriesamountorder AS injuriesamountorder,
           enriched_traces.injuriesamountordertotal AS injuriesamountordertotal,
           enriched_traces.missing AS missing,
           enriched_traces.missingtotal AS missingtotal,
           enriched_traces.missingamountordertotal AS missingamountordertotal,
           enriched_traces.missingamountorder AS missingamountorder,
           enriched_traces.housesdestroyed AS housesdestroyed,
           enriched_traces.housesdestroyedtotal AS housesdestroyedtotal,
           enriched_traces.housesdestroyedamountorder AS housesdestroyedamountorder,
           enriched_traces.housesdestroyedamountordertotal AS housesdestroyedamountordertotal,
           enriched_traces.deathstotal AS deathstotal,
           enriched_traces.deathsamountorder AS deathsamountorder,
           enriched_traces.deathsamountordertotal AS deathsamountordertotal,
           enriched_traces.country AS country,
           enriched_traces.regionname AS regionname,
           enriched_traces.housesdamaged AS housesdamaged,
           enriched_traces.housesdamagedtotal AS housesdamagedtotal,
           enriched_traces.housesdamagedamountordertotal AS housesdamagedamountordertotal,
           enriched_traces.housesdamagedamountorder AS housesdamagedamountorder,
           enriched_traces.year AS year,
           enriched_traces.hour AS hour,
           enriched_traces.minute AS minute,
           enriched_traces.month AS month,
           enriched_traces.day AS day,
           enriched_traces.second AS second,
           CASE WHEN (counties.name IS NOT NULL) THEN
               counties.name
           ELSE
               NULL
           END AS area,
           enriched_traces.milliondollarscropsdamage AS milliondollarscropsdamage
    FROM (
             integration_db.public.enriched_traces AS enriched_traces
             LEFT OUTER JOIN integration_db.public.first_trace_affected_counties AS first_trace_affected_counties
                 ON enriched_traces.id = first_trace_affected_counties.trace_id
         )
         LEFT OUTER JOIN integration_db.public.counties AS counties
             ON (
                 first_trace_affected_counties.county_fips = counties.fips_code
                 AND enriched_traces.state_fips = counties.state_fips
             );

CREATE OR REPLACE VIEW integration_db.public.enriched_traces_w_area AS
    SELECT first_trace_affected_cities.trace_id AS trace_id,
           first_trace_affected_cities.city_fips AS city_fips,
           enriched_traces_w_counties.id AS id,
           enriched_traces_w_counties.injuries AS injuries,
           enriched_traces_w_counties.deaths AS deaths,
           enriched_traces_w_counties.damagemillionsdollars AS damagemillionsdollars,
           enriched_traces_w_counties.latitudestart AS latitudestart,
           enriched_traces_w_counties.longitudestart AS longitudestart,
           enriched_traces_w_counties.state_fips AS state_fips,
           enriched_traces_w_counties.event_timestamp AS event_timestamp,
           enriched_traces_w_counties.damagemillionsdollarstotal AS damagemillionsdollarstotal,
           enriched_traces_w_counties.damageamountorder AS damageamountorder,
           enriched_traces_w_counties.damageamountordertotal AS damageamountordertotal,
           enriched_traces_w_counties.injuriestotal AS injuriestotal,
           enriched_traces_w_counties.injuriesamountorder AS injuriesamountorder,
           enriched_traces_w_counties.injuriesamountordertotal AS injuriesamountordertotal,
           enriched_traces_w_counties.missing AS missing,
           enriched_traces_w_counties.missingtotal AS missingtotal,
           enriched_traces_w_counties.missingamountordertotal AS missingamountordertotal,
           enriched_traces_w_counties.missingamountorder AS missingamountorder,
           enriched_traces_w_counties.housesdestroyed AS housesdestroyed,
           enriched_traces_w_counties.housesdestroyedtotal AS housesdestroyedtotal,
           enriched_traces_w_counties.housesdestroyedamountorder AS housesdestroyedamountorder,
           enriched_traces_w_counties.housesdestroyedamountordertotal AS housesdestroyedamountordertotal,
           enriched_traces_w_counties.deathstotal AS deathstotal,
           enriched_traces_w_counties.deathsamountorder AS deathsamountorder,
           enriched_traces_w_counties.deathsamountordertotal AS deathsamountordertotal,
           enriched_traces_w_counties.country AS country,
           enriched_traces_w_counties.regionname AS regionname,
           enriched_traces_w_counties.housesdamaged AS housesdamaged,
           enriched_traces_w_counties.housesdamagedtotal AS housesdamagedtotal,
           enriched_traces_w_counties.housesdamagedamountordertotal AS housesdamagedamountordertotal,
           enriched_traces_w_counties.housesdamagedamountorder AS housesdamagedamountorder,
           enriched_traces_w_counties.year AS year,
           enriched_traces_w_counties.hour AS hour,
           enriched_traces_w_counties.minute AS minute,
           enriched_traces_w_counties.month AS month,
           enriched_traces_w_counties.day AS day,
           enriched_traces_w_counties.second AS second,
           CASE WHEN (independent_cities.name IS NOT NULL) THEN
               independent_cities.name
           ELSE
               enriched_traces_w_counties.area
           END AS area,
           enriched_traces_w_counties.milliondollarscropsdamage AS milliondollarscropsdamage
    FROM (
             enriched_traces_w_counties AS enriched_traces_w_counties
             LEFT OUTER JOIN integration_db.public.first_trace_affected_cities AS first_trace_affected_cities
                 ON enriched_traces_w_counties.id = first_trace_affected_cities.trace_id
         )
         LEFT OUTER JOIN integration_db.public.independent_cities AS independent_cities
             ON (
                 first_trace_affected_cities.city_fips = independent_cities.fips_code
                 AND enriched_traces_w_counties.state_fips = independent_cities.state_fips
             );

CREATE OR REPLACE VIEW integration_db.public.link_traces AS
    SELECT ('tornado_' || traces.id) AS trace_id,
           traces.tornado_id AS tornado_id,
           traces.order_idx AS order_idx
    FROM integration_db.public.traces as traces;

CREATE OR REPLACE VIEW integration_db.public.natural_event_tornado AS
    SELECT enriched_traces_w_area.id AS id,
           enriched_traces_w_area.injuries AS injuries,
           enriched_traces_w_area.deaths AS deaths,
           enriched_traces_w_area.damagemillionsdollars AS damagemillionsdollars,
           enriched_traces_w_area.latitudestart AS latitude,
           enriched_traces_w_area.longitudestart AS longitude,
           enriched_traces_w_area.event_timestamp AS event_timestamp,
           enriched_traces_w_area.damagemillionsdollarstotal AS damagemillionsdollarstotal,
           enriched_traces_w_area.damageamountorder AS damageamountorder,
           enriched_traces_w_area.damageamountordertotal AS damageamountordertotal,
           enriched_traces_w_area.injuriestotal AS injuriestotal,
           enriched_traces_w_area.injuriesamountorder AS injuriesamountorder,
           enriched_traces_w_area.injuriesamountordertotal AS injuriesamountordertotal,
           enriched_traces_w_area.missing AS missing,
           enriched_traces_w_area.missingtotal AS missingtotal,
           enriched_traces_w_area.missingamountordertotal AS missingamountordertotal,
           enriched_traces_w_area.missingamountorder AS missingamountorder,
           enriched_traces_w_area.housesdestroyed AS housesdestroyed,
           enriched_traces_w_area.housesdestroyedtotal AS housesdestroyedtotal,
           enriched_traces_w_area.housesdestroyedamountorder AS housesdestroyedamountorder,
           enriched_traces_w_area.housesdestroyedamountordertotal AS housesdestroyedamountordertotal,
           enriched_traces_w_area.deathstotal AS deathstotal,
           enriched_traces_w_area.deathsamountorder AS deathsamountorder,
           enriched_traces_w_area.deathsamountordertotal AS deathsamountordertotal,
           enriched_traces_w_area.area AS area,
           enriched_traces_w_area.country AS country,
           enriched_traces_w_area.regionname AS region,
           states.name AS event_location,
           enriched_traces_w_area.housesdamaged AS housesdamaged,
           enriched_traces_w_area.housesdamagedtotal AS housesdamagedtotal,
           enriched_traces_w_area.housesdamagedamountordertotal AS housesdamagedamountordertotal,
           enriched_traces_w_area.housesdamagedamountorder AS housesdamagedamountorder,
           'tornado' AS event_type,
           enriched_traces_w_area.year AS event_year,
           enriched_traces_w_area.hour AS event_hour,
           enriched_traces_w_area.minute AS event_minute,
           enriched_traces_w_area.month AS event_month,
           enriched_traces_w_area.day AS event_day,
           enriched_traces_w_area.second AS event_second,
           enriched_traces_w_area.milliondollarscropsdamage AS milliondollarscropsdamage
    FROM integration_db.public.enriched_traces_w_area AS enriched_traces_w_area
         INNER JOIN integration_db.public.states AS states
             ON enriched_traces_w_area.state_fips = states.fips_code;


CREATE OR REPLACE VIEW integration_db.public.natural_event_tsunami AS
    SELECT tsunami.id AS id,
           tsunami.eventdate AS event_timestamp,
           tsunami.country AS country,
           tsunami.locationname AS event_location,
           tsunami.latitude AS latitude,
           tsunami.longitude AS longitude,
           tsunami.deathsamountorder AS deathsamountorder,
           tsunami.damageamountorder AS damageamountorder,
           tsunami.deathsamountordertotal AS deathsamountordertotal,
           tsunami.damageamountordertotal AS damageamountordertotal,
           tsunami.housesdestroyedamountorder AS housesdestroyedamountorder,
           tsunami.deathstotal AS deathstotal,
           tsunami.housesdestroyedamountordertotal AS housesdestroyedamountordertotal,
           tsunami.deaths AS deaths,
           tsunami.housesdamagedamountorder AS housesdamagedamountorder,
           tsunami.housesdamagedamountordertotal AS housesdamagedamountordertotal,
           tsunami.housesdestroyed AS housesdestroyed,
           tsunami.housesdestroyedtotal AS housesdestroyedtotal,
           tsunami.area AS area,
           tsunami.injuries AS injuries,
           tsunami.injuriesamountorder AS injuriesamountorder,
           tsunami.injuriestotal AS injuriestotal,
           tsunami.injuriesamountordertotal AS injuriesamountordertotal,
           tsunami.housesdamaged AS housesdamaged,
           tsunami.housesdamagedtotal AS housesdamagedtotal,
           tsunami.missingtotal AS missingtotal,
           tsunami.missingamountordertotal AS missingamountordertotal,
           tsunami.damagemillionsdollarstotal AS damagemillionsdollarstotal,
           tsunami.damagemillionsdollars AS damagemillionsdollars,
           tsunami.missing AS missing,
           tsunami.missingamountorder AS missingamountorder,
           tsunami.regionname AS region,
           'tsunami' AS event_type,
           year AS event_year,
           month AS event_month,
           day AS event_day,
           hour AS event_hour,
           minute AS event_minute,
           second AS event_second,
           cast(NULL AS DECIMAL) AS milliondollarscropsdamage
    FROM integration_db.public.tsunami;

CREATE OR REPLACE VIEW integration_db.public.union_traces AS
    SELECT link_traces.trace_id AS event1_id,
           link_traces1.trace_id AS event2_id
    FROM integration_db.public.link_traces AS link_traces
         INNER JOIN integration_db.public.link_traces AS link_traces1
             ON link_traces.tornado_id = link_traces1.tornado_id
    WHERE link_traces.order_idx > link_traces1.order_idx;

CREATE EXTENSION IF NOT EXISTS dblink;

INSERT INTO reconciled_data_layer.public.natural_event (country,
id,
event_location,
latitude,
longitude,
region,
area,
event_timestamp,
damageamountordertotal,
damageamountorder,
damagemillionsdollars,
damagemillionsdollarstotal,
deathstotal,
deaths,
deathsamountorder,
deathsamountordertotal,
housesdestroyed,
housesdestroyedamountorder,
housesdestroyedtotal,
housesdestroyedamountordertotal,
housesdamaged,
housesdamagedamountorder,
housesdamagedtotal,
housesdamagedamountordertotal,
injuries,
injuriesamountorder,
injuriestotal,
injuriesamountordertotal,
missing,
missingamountorder,
missingtotal,
missingamountordertotal,
event_type,
event_year,
event_month,
event_day,
event_hour,
event_minute,
event_second,
milliondollarscropsdamage
)
SELECT country,
id,
event_location,
latitude,
longitude,
region,
area,
event_timestamp,
damageamountordertotal,
damageamountorder,
damagemillionsdollars,
damagemillionsdollarstotal,
deathstotal,
deaths,
deathsamountorder,
deathsamountordertotal,
housesdestroyed,
housesdestroyedamountorder,
housesdestroyedtotal,
housesdestroyedamountordertotal,
housesdamaged,
housesdamagedamountorder,
housesdamagedtotal,
housesdamagedamountordertotal,
injuries,
injuriesamountorder,
injuriestotal,
injuriesamountordertotal,
missing,
missingamountorder,
missingtotal,
missingamountordertotal,
event_type,
event_year,
event_month,
event_day,
event_hour,
event_minute,
event_second,
milliondollarscropsdamage
FROM dblink('host=localhost port=5432 dbname=integration_db user=user password=password1234',
    'SELECT country,
id,
event_location,
latitude,
longitude,
region,
area,
event_timestamp,
damageamountordertotal,
damageamountorder,
damagemillionsdollars,
damagemillionsdollarstotal,
deathstotal,
deaths,
deathsamountorder,
deathsamountordertotal,
housesdestroyed,
housesdestroyedamountorder,
housesdestroyedtotal,
housesdestroyedamountordertotal,
housesdamaged,
housesdamagedamountorder,
housesdamagedtotal,
housesdamagedamountordertotal,
injuries,
injuriesamountorder,
injuriestotal,
injuriesamountordertotal,
missing,
missingamountorder,
missingtotal,
missingamountordertotal,
event_type,
event_year,
event_month,
event_day,
event_hour,
event_minute,
event_second,
milliondollarscropsdamage FROM integration_db.public.natural_event_tornado')
AS t(country TEXT, id TEXT, event_location TEXT, latitude NUMERIC,
     longitude NUMERIC, region TEXT, area TEXT, event_timestamp TEXT,
     damageamountordertotal INTEGER, damageamountorder INTEGER,
     damagemillionsdollars NUMERIC, damagemillionsdollarstotal NUMERIC,
     deathstotal INTEGER, deaths INTEGER, deathsamountorder INTEGER,
     deathsamountordertotal INTEGER, housesdestroyed INTEGER,
     housesdestroyedamountorder INTEGER, housesdestroyedtotal INTEGER,
     housesdestroyedamountordertotal INTEGER, housesdamaged INTEGER,
     housesdamagedamountorder INTEGER, housesdamagedtotal INTEGER,
     housesdamagedamountordertotal INTEGER, injuries INTEGER,
     injuriesamountorder INTEGER, injuriestotal INTEGER,
     injuriesamountordertotal INTEGER, missing INTEGER,
     missingamountorder INTEGER, missingtotal INTEGER,
     missingamountordertotal INTEGER, event_type TEXT, event_year INTEGER,
     event_month INTEGER, event_day INTEGER, event_hour INTEGER,
     event_minute INTEGER, event_second NUMERIC, milliondollarscropsdamage NUMERIC)

ON CONFLICT DO NOTHING;


INSERT INTO reconciled_data_layer.public.natural_event (country,
id,
event_location,
latitude,
longitude,
region,
area,
event_timestamp,
damageamountordertotal,
damageamountorder,
damagemillionsdollars,
damagemillionsdollarstotal,
deathstotal,
deaths,
deathsamountorder,
deathsamountordertotal,
housesdestroyed,
housesdestroyedamountorder,
housesdestroyedtotal,
housesdestroyedamountordertotal,
housesdamaged,
housesdamagedamountorder,
housesdamagedtotal,
housesdamagedamountordertotal,
injuries,
injuriesamountorder,
injuriestotal,
injuriesamountordertotal,
missing,
missingamountorder,
missingtotal,
missingamountordertotal,
event_type,
event_year,
event_month,
event_day,
event_hour,
event_minute,
event_second,
milliondollarscropsdamage
)
SELECT country,
id,
event_location,
latitude,
longitude,
region,
area,
event_timestamp,
damageamountordertotal,
damageamountorder,
damagemillionsdollars,
damagemillionsdollarstotal,
deathstotal,
deaths,
deathsamountorder,
deathsamountordertotal,
housesdestroyed,
housesdestroyedamountorder,
housesdestroyedtotal,
housesdestroyedamountordertotal,
housesdamaged,
housesdamagedamountorder,
housesdamagedtotal,
housesdamagedamountordertotal,
injuries,
injuriesamountorder,
injuriestotal,
injuriesamountordertotal,
missing,
missingamountorder,
missingtotal,
missingamountordertotal,
event_type,
event_year,
event_month,
event_day,
event_hour,
event_minute,
event_second,
milliondollarscropsdamage
FROM dblink('host=localhost port=5432 dbname=integration_db user=user password=password1234',
            'SELECT country,
id,
event_location,
latitude,
longitude,
region,
area,
event_timestamp,
damageamountordertotal,
damageamountorder,
damagemillionsdollars,
damagemillionsdollarstotal,
deathstotal,
deaths,
deathsamountorder,
deathsamountordertotal,
housesdestroyed,
housesdestroyedamountorder,
housesdestroyedtotal,
housesdestroyedamountordertotal,
housesdamaged,
housesdamagedamountorder,
housesdamagedtotal,
housesdamagedamountordertotal,
injuries,
injuriesamountorder,
injuriestotal,
injuriesamountordertotal,
missing,
missingamountorder,
missingtotal,
missingamountordertotal,
event_type,
event_year,
event_month,
event_day,
event_hour,
event_minute,
event_second,
milliondollarscropsdamage FROM integration_db.public.natural_event_tsunami')
AS t(country TEXT, id TEXT, event_location TEXT, latitude NUMERIC,
     longitude NUMERIC, region TEXT, area TEXT, event_timestamp TEXT,
     damageamountordertotal INTEGER, damageamountorder INTEGER,
     damagemillionsdollars NUMERIC, damagemillionsdollarstotal NUMERIC,
     deathstotal INTEGER, deaths INTEGER, deathsamountorder INTEGER,
     deathsamountordertotal INTEGER, housesdestroyed INTEGER,
     housesdestroyedamountorder INTEGER, housesdestroyedtotal INTEGER,
     housesdestroyedamountordertotal INTEGER, housesdamaged INTEGER,
     housesdamagedamountorder INTEGER, housesdamagedtotal INTEGER,
     housesdamagedamountordertotal INTEGER, injuries INTEGER,
     injuriesamountorder INTEGER, injuriestotal INTEGER,
     injuriesamountordertotal INTEGER, missing INTEGER,
     missingamountorder INTEGER, missingtotal INTEGER,
     missingamountordertotal INTEGER, event_type TEXT, event_year INTEGER,
     event_month INTEGER, event_day INTEGER, event_hour INTEGER,
     event_minute INTEGER, event_second NUMERIC, milliondollarscropsdamage NUMERIC)
ON CONFLICT DO NOTHING;


INSERT INTO reconciled_data_layer.public.related_event (event1_id, event2_id)
SELECT event1_id, event2_id FROM
dblink('host=localhost port=5432 dbname=integration_db user=user password=password1234',
       'SELECT event1_id, event2_id FROM integration_db.public.union_traces')
                            AS t(event1_id TEXT, event2_id TEXT)
                            ON CONFLICT DO NOTHING;

DROP TABLE IF EXISTS reconciled_data_layer.public.tornado_trace;

CREATE TABLE reconciled_data_layer.public.tornado_trace AS
    SELECT
           id,
           f_scale,
           latitudeend,
           longitudeend,
           trace_length,
           width,
           alteredmagnitude,
           order_idx,
           concat('tornado_', cast(id AS TEXT)) AS natural_event_id
    FROM
        dblink('host=localhost port=5432 dbname=integration_db user=user password=password1234',
               'SELECT traces.id AS id,
           traces.f_scale AS f_scale,
           traces.latitudeend AS latitudeend,
           traces.longitudeend AS longitudeend,
           traces.length AS trace_length,
           traces.width AS width,
           traces.alteredmagnitude AS alteredmagnitude,
           traces.order_idx AS order_idx
         FROM integration_db.public.traces as traces')
            AS t(id INT, f_scale VARCHAR(6), latitudeend NUMERIC,
                 longitudeend NUMERIC, trace_length NUMERIC,
                 width NUMERIC, alteredmagnitude BOOLEAN,
                 order_idx INTEGER);

ALTER TABLE reconciled_data_layer.public.tornado_trace
ADD PRIMARY KEY (id);
