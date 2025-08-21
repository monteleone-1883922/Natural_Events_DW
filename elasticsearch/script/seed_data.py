import polars as pl
from elasticsearch import Elasticsearch, helpers
import datetime
import os
import logging


MAPPING = {
    "mappings": {
        "properties": {
            "id": {"type": "integer"},
            "eventDate": {"type": "date"},
            "eventValidity": {"type": "boolean"},
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
            "area": {"type": "float"},
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

    event_date = datetime.datetime(
        int(row["year"]),
        int(row.get("month", 1) or 1),
        int(row.get("day", 1) or 1),
        int(row.get("hour", 0) or 0),
        int(row.get("minute", 0) or 0),
        int(row.get("second", 0) or 0)
    ).isoformat()


    return {
        "id": row["id"],
        "eventDate": event_date,
        "eventValidity": row.get("eventValidity", 0),
        "causeCode": row.get("causeCode"),
        "earthquakeEventId": row.get("earthquakeEventId"),
        "volcanoEventId": row.get("volcanoEventId"),
        "country": row.get("country"),
        "locationName": row.get("locationName"),
        "regionCode": row.get("regionCode"),
        "geoLocation": {
            "lat": row["latitude"],
            "lon": row["longitude"]
        },
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
        "publish": row.get("publish", 0),
        "oceanicTsunami": row.get("oceanicTsunami", 0)
    }


class ElasticSearchLoader:

    def __init__(self, url, logger):
        self.es = Elasticsearch(url)
        self.logger = logger

    def create_index(self, index_name):
        if not self.es.indices.exists(index=index_name):
            self.es.indices.create(index=index_name, body=MAPPING)
        self.index_name = index_name


    def load_data(self, filename):
        df = pl.read_csv(filename)
        actions = [
            {
                "_index": self.index_name,
                "_id": str(r["id"]),
                "_source": row_to_doc(r)
            }
            for r in df.to_dicts()
        ]

        helpers.bulk(self.es, actions)
        self.logger.info(f"inserted {len(actions)} documents in {self.index_name}")

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
    logger = logging.getLogger(__name__)
    loader = ElasticSearchLoader(URL, logger)
    loader.load(FILE_PATH, INDEX_NAME)

