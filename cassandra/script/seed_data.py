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

            # Indice su country per analisi danni per paese
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_damage_country
                                     ON earthquakes_by_damage (country);
                                 """)
            self.logger.info("Created index idx_damage_country")

            # Indice su year per analisi temporali dei danni
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_damage_year
                                     ON earthquakes_by_damage (year);
                                 """)
            self.logger.info("Created index idx_damage_year")

            # Indice su eqMagnitude per correlazioni magnitudo-danni
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_damage_magnitude
                                     ON earthquakes_by_damage (eqMagnitude);
                                 """)
            self.logger.info("Created index idx_damage_magnitude")

            # Indici per earthquakes_by_total_damage
            self.logger.info("Creating indexes for earthquakes_by_total_damage...")

            # Indice su country per analisi danni totali per paese
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_total_damage_country
                                     ON earthquakes_by_total_damage (country);
                                 """)
            self.logger.info("Created index idx_total_damage_country")

            # Indice su year per analisi temporali danni totali
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_total_damage_year
                                     ON earthquakes_by_total_damage (year);
                                 """)
            self.logger.info("Created index idx_total_damage_year")

            # Indice su tsunamiEventId per eventi correlati a tsunami
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_total_damage_tsunami
                                     ON earthquakes_by_total_damage (tsunamiEventId);
                                 """)
            self.logger.info("Created index idx_total_damage_tsunami")

            # Indice su volcanoEventId per eventi correlati a vulcani
            self.session.execute("""
                                 CREATE INDEX IF NOT EXISTS idx_total_damage_volcano
                                     ON earthquakes_by_total_damage (volcanoEventId);
                                 """)
            self.logger.info("Created index idx_total_damage_volcano")

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
        self.logger.info("Schema is ", df.schema)

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
                                                     eqDepth)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                                                     regionName)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ;""")

        insert_earthquakes_by_damage = self.session.prepare("""
                INSERT INTO earthquakes_by_damage (damageAmountOrder, deaths,
                                                   year, month, day, hour,
                                                   minute, second, event_date_txt, event_date, event_time,
                                                   id,
                                                   country,
                                                   locationName,
                                                   latitude,
                                                   longitude,
                                                   eqMagnitude,
                                                   intensity,
                                                   missing,
                                                   missingAmountOrder,
                                                   injuries,
                                                   injuriesAmountOrder,
                                                   housesDestroyed,
                                                   housesDestroyedAmountOrder,
                                                   housesDamaged,
                                                   housesDamagedAmountOrder,
                                                   regionName)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ;""")

        insert_earthquakes_by_total_damage = self.session.prepare("""
              INSERT INTO earthquakes_by_total_damage (damageAmountOrderTotal, deathsTotal,
                                                       country,
                                                       locationName,
                                                       year, month, day, hour,
                                                       minute, second, event_date_txt, event_date, event_time,
                                                       id,
                                                       latitude, longitude,
                                                       eqMagnitude, intensity,
                                                       tsunamiEventId,
                                                       volcanoEventId,
                                                       deaths, deathsAmountOrder,
                                                       deathsAmountOrderTotal,
                                                       missing, missingAmountOrder,
                                                       missingTotal, missingAmountOrderTotal,
                                                       injuries, injuriesAmountOrder,
                                                       injuriesTotal, housesDestroyed,
                                                       housesDestroyedAmountOrder,
                                                       housesDestroyedTotal, housesDestroyedAmountOrderTotal,
                                                       housesDamaged,
                                                       housesDamagedAmountOrder,
                                                       housesDamagedTotal,
                                                       housesDamagedAmountOrderTotal,
                                                       damageMillionsDollars,
                                                       damageMillionsDollarsTotal,
                                                       regionName)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                      ?, ?, ?, ?, ?, ?, ?, ?)
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
                row["eqDepth"]
            ))


            if row["eqMagnitude"] is not None:
                self.session.execute(insert_earthquakes_by_magnitude, (
                    row["eqMagnitude"], year, month, day, hour,
                    minute, second, event_date_txt, event_date, event_time, row["id"],
                    row["country"], row["locationName"], row["latitude"], row["longitude"],
                    row["intensity"], row["deathsAmountOrder"], row["damageAmountOrder"],
                    row.get("eqMagMs"), row.get("eqMagMl"),
                    row.get("eqMagMw"), row.get("eqMagMb"),
                    row.get("eqMagMfa"), row.get("eqMagUnk"), row["regionName"]
                ))

            if row["damageAmountOrder"] is not None:
                deaths = row["deaths"] if (row["deaths"] is not None and row["deaths"] != "") else 0
                self.session.execute(insert_earthquakes_by_damage, (
                    row["damageAmountOrder"], deaths, year, month,
                    day, hour, minute, second, event_date_txt, event_date, event_time,
                    row["id"], row["country"], row["locationName"],
                    row["latitude"], row["longitude"], row["eqMagnitude"],
                    row["intensity"],
                    row["missing"], row["missingAmountOrder"],
                    row["injuries"], row["injuriesAmountOrder"],
                    row["housesDestroyed"], row["housesDestroyedAmountOrder"],
                    row["housesDamaged"], row["housesDamagedAmountOrder"],
                    row["regionName"]
                ))

            if row["damageAmountOrderTotal"] is not None and (
                    row["tsunamiEventId"] is not None or
                    row["volcanoEventId"] is not None
            ):
                deathsTotal = row["deaths"] if (row["deaths"] is not None and row["deaths"] != "") else 0
                self.session.execute(insert_earthquakes_by_total_damage, (
                      row["damageAmountOrderTotal"], deathsTotal,
                      row["country"], row["locationName"], year,
                      month, day, hour, minute,
                      second, event_date_txt, event_date, event_time, row["id"],
                      row["latitude"], row["longitude"], row["eqMagnitude"],
                      row["intensity"], row["tsunamiEventId"], row["volcanoEventId"],
                      row["deaths"], row["deathsAmountOrder"], row["deathsAmountOrderTotal"],
                      row["missing"], row["missingAmountOrder"],
                      row["missingTotal"], row["missingAmountOrderTotal"],
                      row["injuries"], row["injuriesAmountOrder"],
                      row["injuriesTotal"], row["housesDestroyed"],
                      row["housesDestroyedAmountOrder"],
                      row["housesDestroyedTotal"], row["housesDestroyedAmountOrderTotal"],
                      row["housesDamaged"], row["housesDamagedAmountOrder"],
                      row["housesDamagedTotal"], row["housesDamagedAmountOrderTotal"],
                      row["damageMillionsDollars"],
                      row["damageMillionsDollarsTotal"],
                      row["regionName"]
                ))


    def create_tables(self):
        # earthquakes_by_country
        self.session.execute("""
            CREATE TABLE earthquakes_by_country (
                country text,
                year int,
                month int,
                day int,
                hour int,
                minute int,
                second float,
                event_date_txt text,
                event_date int,
                event_time time, 
                id int,
                locationName text,
                latitude float,
                longitude float,
                eqMagnitude float,
                intensity float,
                deathsAmountOrder int,
                damageAmountOrder int,
                regionName text,
                area text,
                eqDepth float,
                PRIMARY KEY ((country), event_date, event_time, id)
            ) WITH CLUSTERING ORDER BY (event_date DESC, event_time DESC);
        ;""")
        self.logger.info("Created table earthquakes_by_country")
        #earthquakes_by_magnitude
        self.session.execute("""
            CREATE TABLE earthquakes_by_magnitude (
                eqMagnitude float,
                year int,
                month int,
                day int,
                hour int,
                minute int,
                second float,
                event_date_txt text,
                event_date int,
                event_time time,
                id int,
                country text,
                locationName text,
                latitude float,
                longitude float,
                intensity float,
                deathsAmountOrder int,
                damageAmountOrder int,
                eqMagMs float,
                eqMagMl float,
                eqMagMw float,
                eqMagMb float,
                eqMagMfa float,
                eqMagUnk float,
                regionName text,
                PRIMARY KEY ((eqMagnitude), event_date, event_time, id)
            ) WITH CLUSTERING ORDER BY (event_date DESC, event_time DESC);
        ;""")
        self.logger.info("Created table earthquakes_by_magnitude")
        #earthquakes_by_damage
        self.session.execute("""
            CREATE TABLE earthquakes_by_damage (
                damageAmountOrder int,
                deaths int,
                year int,
                month int,
                day int,
                hour int,
                minute int,
                second float,
                event_date_txt text,
                event_date int,
                event_time time,
                id int,
                country text,
                locationName text,
                latitude float,
                longitude float,
                eqMagnitude float,
                intensity float,
                missing int,
                missingAmountOrder int,
                injuries int,
                injuriesAmountOrder int,
                housesDestroyed int,
                housesDestroyedAmountOrder int,
                housesDamaged int,
                housesDamagedAmountOrder int,
                regionName text,
                PRIMARY KEY ((damageAmountOrder), deaths, id)
            ) WITH CLUSTERING ORDER BY (deaths DESC);
        ;""")
        self.logger.info("Created table earthquakes_by_damage")

        self.session.execute("""
            CREATE TABLE earthquakes_by_total_damage (
                damageAmountOrderTotal int,
                deathsTotal int,
                country text,
                locationName text,
                year int,
                month int,
                day int,
                hour int,
                minute int,
                second float,
                event_date_txt text,
                event_date int,
                event_time time,
                id int,
                latitude float,
                longitude float,
                eqMagnitude float,
                intensity float,
                tsunamiEventId int,
                volcanoEventId int,
                deaths int,
                deathsAmountOrder int,
                deathsAmountOrderTotal int,
                missing int,
                missingAmountOrder int,
                missingTotal int,
                missingAmountOrderTotal int,
                injuries int,
                injuriesAmountOrder int,
                injuriesTotal int,
                housesDestroyed int,
                housesDestroyedAmountOrder int,
                housesDestroyedTotal int,
                housesDestroyedAmountOrderTotal int,
                housesDamaged int,
                housesDamagedAmountOrder int,
                housesDamagedTotal int,
                housesDamagedAmountOrderTotal int,
                damageMillionsDollars float,
                damageMillionsDollarsTotal float,
                regionName text,
                PRIMARY KEY ((damageAmountOrderTotal), deathsTotal, id)
            ) WITH CLUSTERING ORDER BY (deathsTotal DESC);
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
