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
            "volcanoEventId": {"type": "keyword"},
            "country": {"type": "keyword"},
            "locationName": {
                "type": "text",
                "fields": {"raw": {"type": "keyword"}}
            },
            "regionCode": {"type": "keyword"},
            "geoLocation": {"type": "geo_point"},
            "eqMagnitude": {"type": "float"},
            "eqDepth": {"type": "float"},
            "maxWaterHeight": {"type": "float"},
            "tsIntensity": {"type": "float"},
            "tsMtIi": {"type": "float"},
            "tsMtAbe": {"type": "float"},
            "deaths": {"type": "integer"},
            "deathsTotal": {"type": "integer"},
            "injuries": {"type": "integer"},
            "injuriesTotal": {"type": "integer"},
            "missing": {"type": "integer"},
            "missingTotal": {"type": "integer"},
            "housesDestroyed": {"type": "integer"},
            "housesDestroyedTotal": {"type": "integer"},
            "housesDamaged": {"type": "integer"},
            "housesDamagedTotal": {"type": "integer"},
            "damageMillionsDollars": {"type": "float"},
            "damageMillionsDollarsTotal": {"type": "float"},
            "numRunups": {"type": "integer"},
            "numDeposits": {"type": "integer"},
            "area": {"type": "keyword"},
            "publish": {"type": "boolean"},
            "oceanicTsunami": {"type": "boolean"},
            "warningStatusId": {"type": "keyword"},
            "regionName": {"type": "text"},
            "cause": {"type": "text"},
            "validity": {"type": "text"},
            "warningStatus": {"type": "text"},
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
        "eventDate": event_date,
        "eventValidity": row.get("eventValidity"),
        "causeCode": row.get("causeCode"),
        "earthquakeEventId": row.get("earthquakeEventId"),
        "volcanoEventId": row.get("volcanoEventId"),
        "country": row.get("country"),
        "locationName": row.get("locationName"),
        "regionCode": row.get("regionCode"),
        "geoLocation": {
            "lat": row["latitude"],
            "lon": row["longitude"]
        } if row["latitude"] is not None and row["longitude"] is not None else None,
        "eqMagnitude": row.get("eqMagnitude"),
        "eqDepth": row.get("eqDepth"),
        "maxWaterHeight": row.get("maxWaterHeight"),
        "tsIntensity": row.get("tsIntensity"),
        "tsMtIi": row.get("tsMtIi"),
        "tsMtAbe": row.get("tsMtAbe"),
        "deaths": row.get("deaths"),
        "deathsTotal": row.get("deathsTotal"),
        "injuries": row.get("injuries"),
        "injuriesTotal": row.get("injuriesTotal"),
        "damageMillionsDollars": row.get("damageMillionsDollars"),
        "damageMillionsDollarsTotal": row.get("damageMillionsDollarsTotal"),
        "missing": row.get("missing"),
        "missingTotal": row.get("missingTotal"),
        "housesDestroyed": row.get("housesDestroyed"),
        "housesDestroyedTotal": row.get("housesDestroyedTotal"),
        "housesDamaged": row.get("housesDamaged"),
        "housesDamagedTotal": row.get("housesDamagedTotal"),
        "numRunups": row.get("numRunups"),
        "numDeposits": row.get("numDeposits"),
        "area": row.get("area"),
        "warningStatusId": row.get("warningStatusId"),
        "regionName": row.get("regionName"),
        "cause": row.get("cause"),
        "validity": row.get("validity"),
        "warningStatus": row.get("warningStatus"),
        "publish": row.get("publish", False),
        "oceanicTsunami": row.get("oceanicTsunami", False)
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

