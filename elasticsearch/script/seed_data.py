import polars as pl
from elasticsearch import Elasticsearch, helpers
import calendar
import os
import logging


MAPPING = {
    "mappings": {
        "properties": {
            "id": {"type": "integer"},
            "eventDate": {"type": "date"},
            "eventValidity": {"type": "keyword"},
            "causeCode": {"type": "keyword"},
            "earthquakeEventId": {"type": "keyword"},
            "numDeposits": {"type": "integer"},
            "country": {"type": "keyword"},
            "locationName": {
                "type": "text",
                "fields": {"raw": {"type": "keyword"}}
            },
            "regionCode": {"type": "keyword"},
            "geoLocation": {"type": "geo_point"},
            "numRunups": {"type": "integer"},
            "tsIntensity": {"type": "float"},
            "deathsAmountOrder": {"type": "integer"},
            "damageAmountOrder": {"type": "integer"},
            "deathsAmountOrderTotal": {"type": "integer"},
            "damageAmountOrderTotal": {"type": "integer"},
            "publish": {"type": "boolean"},
            "oceanicTsunami": {"type": "boolean"},
            "volcanoEventId": {"type": "keyword"},
            "maxWaterHeight": {"type": "float"},
            "eqMagnitude": {"type": "float"},
            "housesDestroyedAmountOrder": {"type": "integer"},
            "deathsTotal": {"type": "integer"},
            "housesDestroyedAmountOrderTotal": {"type": "integer"},
            "tsMtIi": {"type": "float"},
            "deaths": {"type": "integer"},
            "eqDepth": {"type": "float"},
            "housesDamagedAmountOrder": {"type": "integer"},
            "housesDamagedAmountOrderTotal": {"type": "integer"},
            "housesDestroyed": {"type": "integer"},
            "housesDestroyedTotal": {"type": "integer"},
            "area": {"type": "keyword"},
            "injuries": {"type": "integer"},
            "injuriesAmountOrder": {"type": "integer"},
            "injuriesTotal": {"type": "integer"},
            "injuriesAmountOrderTotal": {"type": "integer"},
            "housesDamaged": {"type": "integer"},
            "housesDamagedTotal": {"type": "integer"},
            "missingTotal": {"type": "integer"},
            "missingAmountOrderTotal": {"type": "integer"},
            "damageMillionsDollarsTotal": {"type": "float"},
            "tsMtAbe": {"type": "float"},
            "damageMillionsDollars": {"type": "float"},
            "warningStatusId": {"type": "keyword"},
            "missing": {"type": "integer"},
            "missingAmountOrder": {"type": "integer"},
            "regionName": {"type": "text"},
            "cause": {"type": "text"},
            "validity": {"type": "text"},
            "warningStatus": {"type": "text"}
        }
    }
}

def row_to_doc(row):

    year = row["year"]
    month = row.get("month", 1)
    day = row.get("day", 1)
    hour = row.get("hour", 0)
    minute = row.get("minute", 0)
    sec = row.get("second", 0)
    max_day = calendar.monthrange(year if year > 0 else 1, month)[1]
    if day > max_day:
        day = max_day

    if year < 0:
        year_str = f"-{abs(year):04d}"
    else:
        year_str = f"{year:04d}"

    sec_int = int(sec)
    sec_frac = sec - sec_int

    if sec_frac > 0:
        # arrotonda a millisecondi
        ms = int(round(sec_frac * 1000))
        second_str = f"{sec_int:02d}.{ms:03d}"
    else:
        second_str = f"{sec_int:02d}"

    event_date = (f"{year_str}-{month:02d}-{day:02d}T" +
                  f"{hour:02d}:{minute:02d}:{second_str}Z")


    return {
        "id": row["id"],
        "eventValidity": row.get("eventValidity"),
        "causeCode": row.get("causeCode"),
        "earthquakeEventId": row.get("earthquakeEventId"),
        "numDeposits": row.get("numDeposits"),  # AGGIUNTO
        "country": row.get("country"),
        "locationName": row.get("locationName"),
        "regionCode": row.get("regionCode"),
        "numRunups": row.get("numRunups"),
        "tsIntensity": row.get("tsIntensity"),
        "deathsAmountOrder": row.get("deathsAmountOrder"),  # AGGIUNTO
        "damageAmountOrder": row.get("damageAmountOrder"),  # AGGIUNTO
        "deathsAmountOrderTotal": row.get("deathsAmountOrderTotal"),  # AGGIUNTO
        "damageAmountOrderTotal": row.get("damageAmountOrderTotal"),  # AGGIUNTO
        "publish": row.get("publish", False),
        "oceanicTsunami": row.get("oceanicTsunami", False),
        "volcanoEventId": row.get("volcanoEventId"),
        "maxWaterHeight": row.get("maxWaterHeight"),
        "eqMagnitude": row.get("eqMagnitude"),
        "housesDestroyedAmountOrder": row.get("housesDestroyedAmountOrder"),  # AGGIUNTO
        "deathsTotal": row.get("deathsTotal"),
        "housesDestroyedAmountOrderTotal": row.get("housesDestroyedAmountOrderTotal"),  # AGGIUNTO
        "tsMtIi": row.get("tsMtIi"),
        "deaths": row.get("deaths"),
        "eqDepth": row.get("eqDepth"),
        "housesDamagedAmountOrder": row.get("housesDamagedAmountOrder"),  # AGGIUNTO
        "housesDamagedAmountOrderTotal": row.get("housesDamagedAmountOrderTotal"),  # AGGIUNTO
        "housesDestroyed": row.get("housesDestroyed"),
        "housesDestroyedTotal": row.get("housesDestroyedTotal"),
        "area": row.get("area"),
        "injuries": row.get("injuries"),
        "injuriesAmountOrder": row.get("injuriesAmountOrder"),  # AGGIUNTO
        "injuriesTotal": row.get("injuriesTotal"),
        "injuriesAmountOrderTotal": row.get("injuriesAmountOrderTotal"),  # AGGIUNTO
        "housesDamaged": row.get("housesDamaged"),
        "housesDamagedTotal": row.get("housesDamagedTotal"),
        "missingTotal": row.get("missingTotal"),
        "missingAmountOrderTotal": row.get("missingAmountOrderTotal"),  # AGGIUNTO
        "damageMillionsDollarsTotal": row.get("damageMillionsDollarsTotal"),
        "tsMtAbe": row.get("tsMtAbe"),
        "damageMillionsDollars": row.get("damageMillionsDollars"),
        "warningStatusId": row.get("warningStatusId"),
        "missing": row.get("missing"),
        "missingAmountOrder": row.get("missingAmountOrder"),  # AGGIUNTO
        "regionName": row.get("regionName"),
        "cause": row.get("cause"),
        "validity": row.get("validity"),
        "warningStatus": row.get("warningStatus"),
        "eventDate": event_date,
        "geoLocation": {
            "lat": row["latitude"],
            "lon": row["longitude"]
        } if row["latitude"] is not None and row["longitude"] is not None else None
}


class ElasticSearchLoader:

    def __init__(self, url, logger):
        self.index_name = None
        self.es = Elasticsearch(url)
        self.logger = logger

    def create_index(self, index_name):
        if self.es.indices.exists(index=index_name):
            self.es.indices.delete(index=index_name, ignore_unavailable=True)
        self.es.indices.create(index=index_name, body=MAPPING)
        self.index_name = index_name


    def load_data(self, filename):
        df = pl.read_csv(filename).with_columns([
            pl.col("month").cast(pl.Int32).fill_null(1),
            pl.col("day").cast(pl.Int32).fill_null(1),
            pl.col("hour").cast(pl.Int32).fill_null(0),
            pl.col("minute").cast(pl.Int32).fill_null(0),
            pl.col("second").cast(pl.Float32).fill_null(0)
        ])
        self.logger.info(f"Schema is {df.schema}")
        self.logger.info(f"Storing {df.height} records")
        actions = [
            {
                "_index": self.index_name,
                "_id": str(r["id"]),
                "_source": row_to_doc(r)
            }
            for r in df.iter_rows(named=True)
        ]

        try:
            helpers.bulk(self.es, actions)
            self.logger.info(f"inserted {len(actions)} documents in {self.index_name}")
        except helpers.BulkIndexError as e:
            self.logger.error(f"BulkIndexError: {len(e.errors)} document(s) failed to index.")
            # Log dettagliato dei primi errori
            for i, error in enumerate(e.errors[:10]):  # Primi 10 errori
                self.logger.error(f"Error {i + 1}:")
                self.logger.error(f"  Document ID: {error.get('index', {}).get('_id', 'unknown')}")
                self.logger.error(f"  Error type: {error.get('index', {}).get('error', {}).get('type', 'unknown')}")
                self.logger.error(f"  Error reason: {error.get('index', {}).get('error', {}).get('reason', 'unknown')}")
                if 'caused_by' in error.get('index', {}).get('error', {}):
                    caused_by = error['index']['error']['caused_by']
                    self.logger.error(
                        f"  Caused by: {caused_by.get('type', 'unknown')} - {caused_by.get('reason', 'unknown')}")

            # Non rilanciare l'errore, così puoi vedere almeno quelli che sono andati a buon fine
            successful_docs = len(actions) - len(e.errors)
            self.logger.info(f"Successfully indexed {successful_docs} out of {len(actions)} documents")

    def close(self):
        self.es.close()

    def load(self, filename, index_name):
        self.create_index(index_name)
        self.load_data(filename)
        self.close()
        self.logger.info("closed connection to elasticsearch")




if __name__ == "__main__":
    INDEX_NAME = os.getenv("INDEX_NAME", "tsunami_events")
    URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
    FILE_PATH = os.getenv("DATA_SOURCE_PATH", "./data/tsunamis.csv")


    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("ElasticsearchLoader")
    loader = ElasticSearchLoader(URL, logger)
    loader.load(FILE_PATH, INDEX_NAME)

