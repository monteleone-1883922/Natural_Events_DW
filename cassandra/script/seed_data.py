import os
import logging
import  polars as pl
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from decimal import Decimal


def print_progress_bar(percentuale, logger, lunghezza_barra=20):
    blocchi_compilati = int(lunghezza_barra * percentuale)
    barra = "[" + "=" * (blocchi_compilati - 1) + ">" + " " * (lunghezza_barra - blocchi_compilati) + "]"
    logger.info(f"\r{barra} {percentuale * 100:.2f}% completo")


class CassandraLoader():
    def __init__(self, host, port, user, pwd, logger):
        self.host = host
        self.port = port
        self.user = user
        self.pwd = pwd
        self.logger = logger
        # connect to db
        self.auth_provider = PlainTextAuthProvider(username=user, password=pwd)
        self.cluster = Cluster([host], port=port, auth_provider=self.auth_provider)
        self.session = self.cluster.connect()

    def create_keyspace(self, keyspace_name):
        self.session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace_name +
                             " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};")
        self.session.set_keyspace(keyspace_name)
        self.logger.info("Keyspace created")

    def close(self):
        try:
            if self.session:
                self.session.shutdown()
                self.logger.debug("Session Cassandra closed")

            if self.cluster:
                self.cluster.shutdown()
                self.logger.debug("Cluster Cassandra closed")

            self.logger.info("Cassandra connection closed successfully")

        except Exception as e:
            self.logger.error(f"error during connection closing: {e}")

    def create_indexes(self):
        """
        Crea gli indici secondari per migliorare le performance delle query
        """
        try:
            # Indici per earthquakes_by_country
            self.logger.info("Creating indexes for earthquakes_by_country...")

            # Indice su year per query temporali
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_country_year
                                     ON earthquakes_by_country (year);
                                 """)
            self.logger.info("Created index idx_country_year")

            # Indice su locationName per ricerche geografiche
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_country_location
                                     ON earthquakes_by_country (locationName);
                                 """)
            self.logger.info("Created index idx_country_location")

            # Indice su eqMagnitude per filtrare per magnitudo
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_country_magnitude
                                     ON earthquakes_by_country (eqMagnitude);
                                 """)
            self.logger.info("Created index idx_country_magnitude")

            # Indice su regionName per query regionali
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_country_region
                                     ON earthquakes_by_country (regionName);
                                 """)
            self.logger.info("Created index idx_country_region")

            # Indici per earthquakes_by_magnitude
            self.logger.info("Creating indexes for earthquakes_by_magnitude...")

            # Indice su country per filtrare per paese
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_magnitude_country
                                     ON earthquakes_by_magnitude (country);
                                 """)
            self.logger.info("Created index idx_magnitude_country")

            # Indice su year per query temporali
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_magnitude_year
                                     ON earthquakes_by_magnitude (year);
                                 """)
            self.logger.info("Created index idx_magnitude_year")

            # Indice su intensity per correlazioni magnitudo-intensità
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_magnitude_intensity
                                     ON earthquakes_by_magnitude (intensity);
                                 """)
            self.logger.info("Created index idx_magnitude_intensity")

            # Indici per earthquakes_by_damage
            self.logger.info("Creating indexes for earthquakes_by_damage...")

            # Indice su country per filtrare per paese
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_damage_country
                                     ON earthquakes_by_damage (country);
                                 """)
            self.logger.info("Created index idx_damage_country")

            # Indice su year per query temporali
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_damage_year
                                     ON earthquakes_by_damage (year);
                                 """)
            self.logger.info("Created index idx_damage_year")

            # Indice su eqMagnitude per correlazioni danno-magnitudo
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_damage_magnitude
                                     ON earthquakes_by_damage (eqMagnitude);
                                 """)
            self.logger.info("Created index idx_damage_magnitude")

            # Indice su damageMillionsDollars per query sui danni economici
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_damage_amount
                                     ON earthquakes_by_damage (damageMillionsDollars);
                                 """)
            self.logger.info("Created index idx_damage_amount")

            # Indice su regionName per query regionali
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_damage_region
                                     ON earthquakes_by_damage (regionName);
                                 """)
            self.logger.info("Created index idx_damage_region")

            # Indici per earthquakes_by_deaths
            self.logger.info("Creating indexes for earthquakes_by_deaths...")

            # Indice su country per filtrare per paese
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_deaths_country
                                     ON earthquakes_by_deaths (country);
                                 """)
            self.logger.info("Created index idx_deaths_country")

            # Indice su year per query temporali
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_deaths_year
                                     ON earthquakes_by_deaths (year);
                                 """)
            self.logger.info("Created index idx_deaths_year")

            # Indice su eqMagnitude per correlazioni vittime-magnitudo
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_deaths_magnitude
                                     ON earthquakes_by_deaths (eqMagnitude);
                                 """)
            self.logger.info("Created index idx_deaths_magnitude")

            # Indice su deaths per query sul numero di vittime
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_deaths_count
                                     ON earthquakes_by_deaths (deaths);
                                 """)
            self.logger.info("Created index idx_deaths_count")

            # Indice su regionName per query regionali
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_deaths_region
                                     ON earthquakes_by_deaths (regionName);
                                 """)
            self.logger.info("Created index idx_deaths_region")

            # Indici per earthquakes_by_missing
            self.logger.info("Creating indexes for earthquakes_by_missing...")

            # Indice su country per filtrare per paese
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_missing_country
                                     ON earthquakes_by_missing (country);
                                 """)
            self.logger.info("Created index idx_missing_country")

            # Indice su year per query temporali
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_missing_year
                                     ON earthquakes_by_missing (year);
                                 """)
            self.logger.info("Created index idx_missing_year")

            # Indice su eqMagnitude per correlazioni dispersi-magnitudo
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_missing_magnitude
                                     ON earthquakes_by_missing (eqMagnitude);
                                 """)
            self.logger.info("Created index idx_missing_magnitude")

            # Indice su missing per query sul numero di dispersi
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_missing_count
                                     ON earthquakes_by_missing (missing);
                                 """)
            self.logger.info("Created index idx_missing_count")

            # Indice su regionName per query regionali
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_missing_region
                                     ON earthquakes_by_missing (regionName);
                                 """)
            self.logger.info("Created index idx_missing_region")

            # Indici per earthquakes_by_injuries
            self.logger.info("Creating indexes for earthquakes_by_injuries...")

            # Indice su country per filtrare per paese
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_injuries_country
                                     ON earthquakes_by_injuries (country);
                                 """)
            self.logger.info("Created index idx_injuries_country")

            # Indice su year per query temporali
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_injuries_year
                                     ON earthquakes_by_injuries (year);
                                 """)
            self.logger.info("Created index idx_injuries_year")

            # Indice su eqMagnitude per correlazioni feriti-magnitudo
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_injuries_magnitude
                                     ON earthquakes_by_injuries (eqMagnitude);
                                 """)
            self.logger.info("Created index idx_injuries_magnitude")

            # Indice su injuries per query sul numero di feriti
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_injuries_count
                                     ON earthquakes_by_injuries (injuries);
                                 """)
            self.logger.info("Created index idx_injuries_count")

            # Indice su regionName per query regionali
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_injuries_region
                                     ON earthquakes_by_injuries (regionName);
                                 """)
            self.logger.info("Created index idx_injuries_region")

            # Indici per earthquakes_by_houses_damages
            self.logger.info("Creating indexes for earthquakes_by_houses_damages...")

            # Indice su country per filtrare per paese
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_houses_country
                                     ON earthquakes_by_houses_damages (country);
                                 """)
            self.logger.info("Created index idx_houses_country")

            # Indice su year per query temporali
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_houses_year
                                     ON earthquakes_by_houses_damages (year);
                                 """)
            self.logger.info("Created index idx_houses_year")

            # Indice su eqMagnitude per correlazioni danni abitazioni-magnitudo
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_houses_magnitude
                                     ON earthquakes_by_houses_damages (eqMagnitude);
                                 """)
            self.logger.info("Created index idx_houses_magnitude")

            # Indice su housesDestroyed per query su case distrutte
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_houses_destroyed
                                     ON earthquakes_by_houses_damages (housesDestroyed);
                                 """)
            self.logger.info("Created index idx_houses_destroyed")

            # Indice su housesDamaged per query su case danneggiate
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_houses_damaged
                                     ON earthquakes_by_houses_damages (housesDamaged);
                                 """)
            self.logger.info("Created index idx_houses_damaged")

            # Indice su regionName per query regionali
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_houses_region
                                     ON earthquakes_by_houses_damages (regionName);
                                 """)
            self.logger.info("Created index idx_houses_region")

            self.logger.info("All indexes created successfully!")

        except Exception as e:
            self.logger.error(f"Error creating indexes: {e}")
            raise

    def insert_data(self, input_file):

        df = pl.read_csv(input_file) \
            .with_columns([
                pl.col("month").cast(pl.Int32).fill_null(1),
                pl.col("day").cast(pl.Int32).fill_null(1),
                pl.col("hour").cast(pl.Int32).fill_null(0),
                pl.col("minute").cast(pl.Int32).fill_null(0),
                pl.col("second").cast(pl.Float32).fill_null(0),
                pl.col("injuries").cast(pl.Int32),
                pl.col("injuriesAmountOrder").cast(pl.Int32),
                pl.col("injuriesTotal").cast(pl.Int32),
                pl.col("injuriesAmountOrderTotal").cast(pl.Int32),
                pl.col("housesDamagedAmountOrderTotal").cast(pl.Int32),
                pl.col("eqMagMw").cast(pl.Float64),
                pl.col("housesDestroyed").cast(pl.Int32),
                pl.col("housesDamaged").cast(pl.Int32),
                pl.col("housesDestroyedTotal").cast(pl.Int32),
                pl.col("housesDamagedTotal").cast(pl.Int32),
                pl.col("eqMagMfa").cast(pl.Float64),
                pl.col("damageMillionsDollars").cast(pl.Float32),
                pl.col("missing").cast(pl.Int32),
                pl.col("missingAmountOrder").cast(pl.Int32),
                pl.col("missingTotal").cast(pl.Int32),
                pl.col("missingAmountOrderTotal").cast(pl.Int32),
                pl.col("damageMillionsDollarsTotal").cast(pl.Float32),
                pl.col("eqMagMb").cast(pl.Float64),
            ])
        self.logger.info(f"Schema is {df.schema}")

        insert_earthquakes_by_country = self.session.prepare("""
                 INSERT INTO earthquakes_by_country (country, year, month,
                                                     day, hour, minute,
                                                     second, event_date_txt, event_date, event_time, id,
                                                     locationName, latitude,
                                                     longitude,
                                                     eqMagnitude, intensity,
                                                     deathsAmountOrder,
                                                     damageAmountOrder,
                                                     regionName, area,
                                                     eqDepth,
                                                     tsunamiEventId,
                                                     volcanoEventId)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ;""")


        insert_earthquakes_by_magnitude = self.session.prepare("""
               INSERT INTO earthquakes_by_magnitude (eqMagnitude, year,
                                                     month,
                                                     day, hour, minute,
                                                     second, event_date_txt, event_date, event_time,
                                                     id,
                                                     country,
                                                     locationName,
                                                     latitude,
                                                     longitude,
                                                     intensity,
                                                     deathsAmountOrder,
                                                     damageAmountOrder,
                                                     eqMagMs, eqMagMl,
                                                     eqMagMw, eqMagMb,
                                                     eqMagMfa, eqMagUnk,
                                                     regionName, eqDepth)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ;""")

        # Tabella earthquakes_by_damage
        insert_earthquakes_by_damage = self.session.prepare("""
                                                            INSERT INTO earthquakes_by_damage (damageAmountOrderTotal,
                                                                                               damageAmountOrder,
                                                                                               country,
                                                                                               locationName, year,
                                                                                               month, day, hour, minute,
                                                                                               second,
                                                                                               event_date_txt,
                                                                                               event_date, event_time,
                                                                                               id, latitude,
                                                                                               longitude, eqMagnitude,
                                                                                               intensity,
                                                                                               damageMillionsDollars,
                                                                                               damageMillionsDollarsTotal,
                                                                                               regionName)
                                                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                                                                    ?, ?, ?, ?)
                                                            ;""")

        # Tabella earthquakes_by_deaths
        insert_earthquakes_by_deaths = self.session.prepare("""
                                                            INSERT INTO earthquakes_by_deaths (deathsTotal, country,
                                                                                               locationName, year,
                                                                                               month, day,
                                                                                               hour, minute, second,
                                                                                               event_date_txt,
                                                                                               event_date,
                                                                                               event_time, id, latitude,
                                                                                               longitude, eqMagnitude,
                                                                                               intensity, deaths,
                                                                                               deathsAmountOrder,
                                                                                               deathsAmountOrderTotal,
                                                                                               regionName)
                                                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                                                                    ?, ?, ?, ?)
                                                            ;""")

        # Tabella earthquakes_by_missing
        insert_earthquakes_by_missing = self.session.prepare("""
                                                             INSERT INTO earthquakes_by_missing (country, locationName,
                                                                                                 year, month, day, hour,
                                                                                                 minute,
                                                                                                 second, event_date_txt,
                                                                                                 event_date, event_time,
                                                                                                 id,
                                                                                                 latitude, longitude,
                                                                                                 eqMagnitude, intensity,
                                                                                                 missing,
                                                                                                 missingAmountOrder,
                                                                                                 missingTotal,
                                                                                                 missingAmountOrderTotal,
                                                                                                 regionName)
                                                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                                                                     ?, ?, ?, ?)
                                                             ;""")

        # Tabella earthquakes_by_injuries
        insert_earthquakes_by_injuries = self.session.prepare("""
                                                              INSERT INTO earthquakes_by_injuries (country,
                                                                                                   locationName, year,
                                                                                                   month, day, hour,
                                                                                                   minute,
                                                                                                   second,
                                                                                                   event_date_txt,
                                                                                                   event_date,
                                                                                                   event_time, id,
                                                                                                   latitude, longitude,
                                                                                                   eqMagnitude,
                                                                                                   intensity, injuries,
                                                                                                   injuriesAmountOrder,
                                                                                                   injuriesTotal,
                                                                                                   injuriesAmountOrderTotal,
                                                                                                   regionName)
                                                              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                                                                      ?, ?, ?, ?)
                                                              ;""")

        # Tabella earthquakes_by_houses_damages
        insert_earthquakes_by_houses_damages = self.session.prepare("""
                                                                    INSERT INTO earthquakes_by_houses_damages (country,
                                                                                                               locationName,
                                                                                                               year,
                                                                                                               month,
                                                                                                               day,
                                                                                                               hour,
                                                                                                               minute,
                                                                                                               second,
                                                                                                               event_date_txt,
                                                                                                               event_date,
                                                                                                               event_time,
                                                                                                               id,
                                                                                                               latitude,
                                                                                                               longitude,
                                                                                                               eqMagnitude,
                                                                                                               intensity,
                                                                                                               housesDestroyed,
                                                                                                               housesDestroyedAmountOrder,
                                                                                                               housesDestroyedTotal,
                                                                                                               housesDestroyedAmountOrderTotal,
                                                                                                               housesDamaged,
                                                                                                               housesDamagedAmountOrder,
                                                                                                               housesDamagedTotal,
                                                                                                               housesDamagedAmountOrderTotal,
                                                                                                               regionName)
                                                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                                                                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                                                    ;""")
        for index, row in enumerate(df.iter_rows(named=True)):
            if index % 50 == 0:
                print_progress_bar(index/df.height, self.logger)

            year = row["year"]
            month = row["month"]
            day = row["day"]
            hour = row["hour"]
            minute = row["minute"]
            second = row["second"]


            event_date_txt = f"{year}-{month:02d}-{day:02d}"
            event_date = year * 10000 + month * 100 + day
            sec_int = int(second)
            sec_frac = Decimal(str(second - sec_int)).quantize(Decimal("0.001"))

            event_time = f"{hour:02d}:{minute:02d}:{sec_int:02d}{str(sec_frac)[1:]}"

            self.session.execute(insert_earthquakes_by_country, (
                row["country"], year, month, day, hour,
                minute, second, event_date_txt, event_date, event_time, row["id"],
                row["locationName"], row["latitude"], row["longitude"],
                row["eqMagnitude"], row["intensity"], row["deathsAmountOrder"],
                row["damageAmountOrder"], row["regionName"], row.get("area"),
                row["eqDepth"], row["tsunamiEventId"], row["volcanoEventId"]
            ))


            if row["eqMagnitude"] is not None:
                self.session.execute(insert_earthquakes_by_magnitude, (
                    row["eqMagnitude"], year, month, day, hour,
                    minute, second, event_date_txt, event_date, event_time, row["id"],
                    row["country"], row["locationName"], row["latitude"], row["longitude"],
                    row["intensity"], row["deathsAmountOrder"], row["damageAmountOrder"],
                    row.get("eqMagMs"), row.get("eqMagMl"),
                    row.get("eqMagMw"), row.get("eqMagMb"),
                    row.get("eqMagMfa"), row.get("eqMagUnk"), row["regionName"], row["eqDepth"]
                ))


            if row["damageAmountOrderTotal"] is not None or row["damageAmountOrder"] is not None or \
                    row["damageMillionsDollars"] is not None or row["damageMillionsDollarsTotal"] is not None:
                damageAmountOrderTotal = row["damageAmountOrderTotal"] if row["damageAmountOrderTotal"] is not None else 0
                damageAmountOrder = row["damageAmountOrder"] if row["damageAmountOrder"] is not None else 0
                damageMillionsDollars = row["damageMillionsDollars"] if row["damageMillionsDollars"] is not None else 0
                damageMillionsDollarsTotal = row["damageMillionsDollarsTotal"] if row["damageMillionsDollarsTotal"] is not None else 0
                self.session.execute(insert_earthquakes_by_damage, (
                    damageAmountOrderTotal, damageAmountOrder,
                      row["country"], row["locationName"], year,
                      month, day, hour, minute,
                      second, event_date_txt, event_date, event_time, row["id"],
                      row["latitude"], row["longitude"], row["eqMagnitude"],
                      row["intensity"],
                      damageMillionsDollars,
                      damageMillionsDollarsTotal,
                      row["regionName"]
                ))

            # Per earthquakes_by_deaths
            if row["deathsTotal"] is not None or row["deaths"] is not None or \
                    row["deathsAmountOrder"] is not None or row["deathsAmountOrderTotal"] is not None:
                deathsTotal = row["deathsTotal"] if row["deathsTotal"] is not None else 0
                deaths = row["deaths"] if row["deaths"] is not None else 0
                deathsAmountOrder = row["deathsAmountOrder"] if row["deathsAmountOrder"] is not None else 0
                deathsAmountOrderTotal = row["deathsAmountOrderTotal"] if row[
                                                                              "deathsAmountOrderTotal"] is not None else 0
                self.session.execute(insert_earthquakes_by_deaths, (
                    deathsTotal,
                    row["country"], row["locationName"], year,
                    month, day, hour, minute,
                    second, event_date_txt, event_date, event_time, row["id"],
                    row["latitude"], row["longitude"], row["eqMagnitude"],
                    row["intensity"],
                    deaths,
                    deathsAmountOrder,
                    deathsAmountOrderTotal,
                    row["regionName"]
                ))

            # Per earthquakes_by_missing
            if row["missingTotal"] is not None or row["missing"] is not None or \
                    row["missingAmountOrder"] is not None or row["missingAmountOrderTotal"] is not None:
                missingTotal = row["missingTotal"] if row["missingTotal"] is not None else 0
                missing = row["missing"] if row["missing"] is not None else 0
                missingAmountOrder = row["missingAmountOrder"] if row["missingAmountOrder"] is not None else 0
                missingAmountOrderTotal = row["missingAmountOrderTotal"] if row[
                                                                                "missingAmountOrderTotal"] is not None else 0
                self.session.execute(insert_earthquakes_by_missing, (
                    row["country"], row["locationName"], year,
                    month, day, hour, minute,
                    second, event_date_txt, event_date, event_time, row["id"],
                    row["latitude"], row["longitude"], row["eqMagnitude"],
                    row["intensity"],
                    missing,
                    missingAmountOrder,
                    missingTotal,
                    missingAmountOrderTotal,
                    row["regionName"]
                ))

            # Per earthquakes_by_injuries
            if row["injuriesTotal"] is not None or row["injuries"] is not None or \
                    row["injuriesAmountOrder"] is not None or row["injuriesAmountOrderTotal"] is not None:
                injuriesTotal = row["injuriesTotal"] if row["injuriesTotal"] is not None else 0
                injuries = row["injuries"] if row["injuries"] is not None else 0
                injuriesAmountOrder = row["injuriesAmountOrder"] if row["injuriesAmountOrder"] is not None else 0
                injuriesAmountOrderTotal = row["injuriesAmountOrderTotal"] if row["injuriesAmountOrderTotal"] is not None else 0
                self.session.execute(insert_earthquakes_by_injuries, (
                    row["country"], row["locationName"], year,
                    month, day, hour, minute,
                    second, event_date_txt, event_date, event_time, row["id"],
                    row["latitude"], row["longitude"], row["eqMagnitude"],
                    row["intensity"],
                    injuries,
                    injuriesAmountOrder,
                    injuriesTotal,
                    injuriesAmountOrderTotal,
                    row["regionName"]
                ))

            # Per earthquakes_by_houses_damages
            if row["housesDestroyedTotal"] is not None or row["housesDestroyed"] is not None or \
                    row["housesDestroyedAmountOrder"] is not None or row[
                "housesDestroyedAmountOrderTotal"] is not None or \
                    row["housesDamagedTotal"] is not None or row["housesDamaged"] is not None or \
                    row["housesDamagedAmountOrder"] is not None or row["housesDamagedAmountOrderTotal"] is not None:
                housesDestroyedTotal = row["housesDestroyedTotal"] if row["housesDestroyedTotal"] is not None else 0
                housesDestroyed = row["housesDestroyed"] if row["housesDestroyed"] is not None else 0
                housesDestroyedAmountOrder = row["housesDestroyedAmountOrder"] if row["housesDestroyedAmountOrder"] is not None else 0
                housesDestroyedAmountOrderTotal = row["housesDestroyedAmountOrderTotal"] if row["housesDestroyedAmountOrderTotal"] is not None else 0
                housesDamagedTotal = row["housesDamagedTotal"] if row["housesDamagedTotal"] is not None else 0
                housesDamaged = row["housesDamaged"] if row["housesDamaged"] is not None else 0
                housesDamagedAmountOrder = row["housesDamagedAmountOrder"] if row["housesDamagedAmountOrder"] is not None else 0
                housesDamagedAmountOrderTotal = row["housesDamagedAmountOrderTotal"] if row["housesDamagedAmountOrderTotal"] is not None else 0

                self.session.execute(insert_earthquakes_by_houses_damages, (
                    row["country"], row["locationName"], year,
                    month, day, hour, minute,
                    second, event_date_txt, event_date, event_time, row["id"],
                    row["latitude"], row["longitude"], row["eqMagnitude"],
                    row["intensity"],
                    housesDestroyed,
                    housesDestroyedAmountOrder,
                    housesDestroyedTotal,
                    housesDestroyedAmountOrderTotal,
                    housesDamaged,
                    housesDamagedAmountOrder,
                    housesDamagedTotal,
                    housesDamagedAmountOrderTotal,
                    row["regionName"]
                ))


    def create_tables(self):
        # earthquakes_by_country
        self.session.execute("""
            CREATE TABLE earthquakes_by_country (
                country text,
                year bigint,
                month bigint,
                day bigint,
                hour bigint,
                minute bigint,
                second double,
                event_date_txt text,
                event_date bigint,
                event_time time, 
                id bigint,
                locationName text,
                latitude double,
                longitude double,
                eqMagnitude double,
                intensity double,
                deathsAmountOrder bigint,
                damageAmountOrder bigint,
                regionName text,
                area text,
                eqDepth double,
                tsunamiEventId bigint,
                volcanoEventId bigint,
                PRIMARY KEY ((country), event_date, event_time, id)
            ) WITH CLUSTERING ORDER BY (event_date DESC, event_time DESC);
        ;""")
        self.logger.info("Created table earthquakes_by_country")
        #earthquakes_by_magnitude
        self.session.execute("""
            CREATE TABLE earthquakes_by_magnitude (
                eqMagnitude double,
                year bigint,
                month bigint,
                day bigint,
                hour bigint,
                minute bigint,
                second double,
                event_date_txt text,
                event_date bigint,
                event_time time,
                id bigint,
                country text,
                locationName text,
                latitude double,
                longitude double,
                intensity double,
                deathsAmountOrder bigint,
                damageAmountOrder bigint,
                eqMagMs double,
                eqMagMl double,
                eqMagMw double,
                eqMagMb double,
                eqMagMfa double,
                eqMagUnk double,
                regionName text,
                eqDepth double,
                PRIMARY KEY ((eqMagnitude), event_date, event_time, id)
            ) WITH CLUSTERING ORDER BY (event_date DESC, event_time DESC);
        ;""")
        self.logger.info("Created table earthquakes_by_magnitude")
        #earthquakes_by_damage


        self.session.execute("""
            CREATE TABLE earthquakes_by_damage (
                damageAmountOrderTotal bigint,
                damageAmountOrder bigint,
                country text,
                locationName text,
                year bigint,
                month bigint,
                day bigint,
                hour bigint,
                minute bigint,
                second double,
                event_date_txt text,
                event_date bigint,
                event_time time,
                id bigint,
                latitude double,
                longitude double,
                eqMagnitude double,
                intensity double,
                damageMillionsDollars double,
                damageMillionsDollarsTotal double,
                regionName text,
                PRIMARY KEY ((damageAmountOrderTotal), damageAmountOrder, id)
            ) WITH CLUSTERING ORDER BY (damageAmountOrder DESC);
        ;""")
        self.logger.info("Created table earthquakes_by_damage")

        self.session.execute("""
                             CREATE TABLE earthquakes_by_deaths
                             (
                                 deathsTotal                     bigint,
                                 country                         text,
                                 locationName                    text,
                                 year                            bigint,
                                 month                           bigint,
                                 day                             bigint,
                                 hour                            bigint,
                                 minute                          bigint,
                                 second                          double,
                                 event_date_txt                  text,
                                 event_date                      bigint,
                                 event_time                      time,
                                 id                              bigint,
                                 latitude                        double,
                                 longitude                       double,
                                 eqMagnitude                     double,
                                 intensity                       double,
                                 deaths                          bigint,
                                 deathsAmountOrder               bigint,
                                 deathsAmountOrderTotal          bigint,
                                 regionName                      text,
                                 PRIMARY KEY ((deathsAmountOrderTotal), deathsTotal, id)
                             ) WITH CLUSTERING ORDER BY (deathsTotal DESC);
                             ;""")
        self.logger.info("Created table earthquakes_by_deaths")

        self.session.execute("""
                             CREATE TABLE earthquakes_by_missing
                             (
                                 country                         text,
                                 locationName                    text,
                                 year                            bigint,
                                 month                           bigint,
                                 day                             bigint,
                                 hour                            bigint,
                                 minute                          bigint,
                                 second                          double,
                                 event_date_txt                  text,
                                 event_date                      bigint,
                                 event_time                      time,
                                 id                              bigint,
                                 latitude                        double,
                                 longitude                       double,
                                 eqMagnitude                     double,
                                 intensity                       double,
                                 missing                         bigint,
                                 missingAmountOrder              bigint,
                                 missingTotal                    bigint,
                                 missingAmountOrderTotal         bigint,
                                 regionName                      text,
                                 PRIMARY KEY ((missingAmountOrderTotal), missingTotal, id)
                             ) WITH CLUSTERING ORDER BY (missingTotal DESC);
                             ;""")
        self.logger.info("Created table earthquakes_by_missing")

        self.session.execute("""
                             CREATE TABLE earthquakes_by_injuries
                             (
                                 country                         text,
                                 locationName                    text,
                                 year                            bigint,
                                 month                           bigint,
                                 day                             bigint,
                                 hour                            bigint,
                                 minute                          bigint,
                                 second                          double,
                                 event_date_txt                  text,
                                 event_date                      bigint,
                                 event_time                      time,
                                 id                              bigint,
                                 latitude                        double,
                                 longitude                       double,
                                 eqMagnitude                     double,
                                 intensity                       double,
                                 injuries                        bigint,
                                 injuriesAmountOrder             bigint,
                                 injuriesTotal                   bigint,
                                 injuriesAmountOrderTotal        bigint,
                                 regionName                      text,
                                 PRIMARY KEY ((injuriesAmountOrderTotal), injuriesTotal, id)
                             ) WITH CLUSTERING ORDER BY (injuriesTotal DESC);
                             ;""")
        self.logger.info("Created table earthquakes_by_injuries")

        self.session.execute("""
                             CREATE TABLE earthquakes_by_houses_damages
                             (
                                 country                         text,
                                 locationName                    text,
                                 year                            bigint,
                                 month                           bigint,
                                 day                             bigint,
                                 hour                            bigint,
                                 minute                          bigint,
                                 second                          double,
                                 event_date_txt                  text,
                                 event_date                      bigint,
                                 event_time                      time,
                                 id                              bigint,
                                 latitude                        double,
                                 longitude                       double,
                                 eqMagnitude                     double,
                                 intensity                       double,
                                 housesDestroyed                 bigint,
                                 housesDestroyedAmountOrder      bigint,
                                 housesDestroyedTotal            bigint,
                                 housesDestroyedAmountOrderTotal bigint,
                                 housesDamaged                   bigint,
                                 housesDamagedAmountOrder        bigint,
                                 housesDamagedTotal              bigint,
                                 housesDamagedAmountOrderTotal   bigint,
                                 regionName                      text,
                                 PRIMARY KEY ((housesDamagedAmountOrderTotal), housesDamagedTotal, id)
                             ) WITH CLUSTERING ORDER BY (housesDamagedTotal DESC);
                             ;""")
        self.logger.info("Created table earthquakes_by_total_damage")

    def load_data(self, keyspace, input_file):
        self.create_keyspace(keyspace)
        self.create_tables()
        self.insert_data(input_file)
        self.create_indexes()
        self.close()
        self.logger.info("Finished loading")





if __name__ == "__main__":
    host = os.getenv("CASSANDRA_HOST", "localhost")
    port = int(os.getenv("CASSANDRA_PORT", "9042"))
    user = os.getenv("CASSANDRA_USERNAME", "cassandra")
    pwd = os.getenv("CASSANDRA_PASSWORD", "cassandra")
    keyspace = os.getenv("KEYSPACE_NAME", "earthquake")
    input_file = os.getenv("INPUT_FILE", "./data/earthquakes.csv")

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("Earthquake Loader")
    loader = CassandraLoader(host, port, user, pwd, logger)
    loader.load_data(keyspace, input_file)
