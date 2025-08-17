import polars as pl
from pymongo import MongoClient
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MongoCSVLoader:
    def __init__(self,username, password, mongo_host="mongodb", mongo_port=27017, db_name="volcanoes"):
        self.client = MongoClient(f"mongodb://{username}:{password}@{mongo_host}:{mongo_port}/{db_name}?authSource=admin")
        self.db = self.client[db_name]
        logger.info(f"Connected to MongoDB: {mongo_host}:{mongo_port}/{db_name}")

    def create_indexes(self):
        logger.info("Creating database indexes...")

        try:
            # INDICI PER COLLECTION VOLCANOES
            volcanoes_collection = self.db['volcanoes']

            # Indice univoco su id (primary key naturale)
            volcanoes_collection.create_index("id", unique=True)
            logger.info("✅ Created unique index on volcanoes.id")

            # Indice geografico per query spaziali
            volcanoes_collection.create_index([("position", "2dsphere")])
            logger.info("✅ Created 2dsphere index on volcanoes.position")

            # Indici per filtri comuni
            volcanoes_collection.create_index("country")
            volcanoes_collection.create_index("region")
            volcanoes_collection.create_index("status")
            volcanoes_collection.create_index("morphology")
            logger.info("✅ Created indexes on volcanoes filter fields")

            # Indice composto per query geografiche per paese/regione
            volcanoes_collection.create_index([("country", 1), ("region", 1)])
            logger.info("✅ Created compound index on volcanoes.country+region")

            # INDICI PER COLLECTION ERUPTIONS
            eruptions_collection = self.db['eruptions']

            # Indice univoco su id
            eruptions_collection.create_index("id", unique=True)
            logger.info("✅ Created unique index on eruptions.id")

            # INDICE FONDAMENTALE: volcano_id per i lookup
            eruptions_collection.create_index("volcano_id")
            logger.info("✅ Created index on eruptions.volcano_id (for lookups)")

            # Indici per range query temporali
            eruptions_collection.create_index("date")
            logger.info("✅ Created indexes on eruptions temporal fields")

            # Indice per VEI (Volcanic Explosivity Index)
            eruptions_collection.create_index("vei")
            logger.info("✅ Created index on eruptions.vei")

            # Indici per query su eventi significativi
            eruptions_collection.create_index("significant")
            logger.info("✅ Created indexes on eruptions significance fields")

            # Indice geografico per eruzioni
            eruptions_collection.create_index([("position", "2dsphere")])
            logger.info("✅ Created 2dsphere index on eruptions.position")

            # Indici composti per query complesse comuni
            eruptions_collection.create_index([("volcano_id", 1), ("vei", -1)])  # eruzioni più forti per vulcano
            logger.info("✅ Created compound indexes on eruptions")

            # INDICI PER COLLECTION VOLCANOES_AGGREGATED
            volcanoes_agg_collection = self.db['volcanoes_aggregated']

            # Indice su volcano_id
            volcanoes_agg_collection.create_index("volcano_id", unique=True)
            logger.info("✅ Created unique index on volcanoes_aggregated.volcano_id")

            # Indici sulle statistiche per ordinamenti e filtri
            volcanoes_agg_collection.create_index("eruption_stats.total_eruptions")
            volcanoes_agg_collection.create_index("eruption_stats.last_eruption_date")
            volcanoes_agg_collection.create_index("eruption_stats.max_vei")
            volcanoes_agg_collection.create_index("eruption_stats.total_deaths")
            logger.info("✅ Created indexes on volcanoes_aggregated statistics")

            # Indice geografico
            volcanoes_agg_collection.create_index([("position", "2dsphere")])
            logger.info("✅ Created 2dsphere index on volcanoes_aggregated.position")

            # Indici per filtri comuni
            volcanoes_agg_collection.create_index("country")
            volcanoes_agg_collection.create_index("region")
            volcanoes_agg_collection.create_index("status")
            logger.info("✅ Created indexes on volcanoes_aggregated filter fields")

            # INDICI PER COLLECTION REGIONS
            regions_collection = self.db['regions']
            regions_collection.create_index("id")
            logger.info("✅ Created index on regions.id")


            # INDICI PER COLLECTION EPOCHS
            epochs_collection = self.db['epochs']
            epochs_collection.create_index("type")

            logger.info("🎯 All database indexes created successfully!")

        except Exception as e:
            logger.error(f"❌ Error creating indexes: {e}")
            raise

    def load_regions(self, csv_path: str):

        logger.info("Loading regions...")
        df_regions = pl.read_csv(csv_path)
        collection = self.db['regions']
        collection.drop()
        documents = df_regions.to_dicts()

        if documents:
            result = collection.insert_many(documents)
            logger.info(f"inserted {len(result.inserted_ids)} regions")


    def load_epochs(self, csv_path: str):

        logger.info("Loading epochs...")
        df_epoch = pl.read_csv(csv_path)
        collection = self.db['epochs']
        collection.drop()
        documents = df_epoch.to_dicts()
        if documents:
            result = collection.insert_many(documents)
            logger.info(f"Inserted {len(result.inserted_ids)} epochs")


    def load_volcanoes(self, csv_path: str):

        logger.info("Loading volcanoes...")
        df_volcanoes = pl.read_csv(csv_path).with_columns(
                pl.struct([
                    pl.lit("Point").alias("type"),
                    pl.concat_list([pl.col("longitude"), pl.col("latitude")]).alias("coordinates")
                ]).alias("position")
            )


        collection = self.db['volcanoes']
        collection.drop()
        documents = df_volcanoes.to_dicts()
        if documents:
            result = collection.insert_many(documents)
            logger.info(f"Inserted {len(result.inserted_ids)} volcanoes")



    def load_eruptions(self, csv_path: str):

        logger.info("Loading eruptions...")
        df_eruptions = pl.read_csv(csv_path).with_columns(
                pl.datetime(pl.col('year'), pl.col('month'), pl.col('day')).alias('date'),
                # Converti longitudine e latitudine a float
                pl.col('longitude').cast(pl.Float64),
                pl.col('latitude').cast(pl.Float64)
            ).with_columns(
                pl.struct([
                    pl.lit("Point").alias("type"),
                    pl.concat_list([pl.col("longitude"), pl.col("latitude")]).alias("coordinates")
                ]).alias("position")
            )

        collection = self.db['eruptions']
        collection.drop()
        documents = df_eruptions.to_dicts()

        if documents:
            result = collection.insert_many(documents)
            logger.info(f"Inserted {len(result.inserted_ids)} eruptions")

    def create_aggregated_volcanoes(self):
        logger.info("Creating aggregated volcanoes collection...")

        pipeline = [
            {
                "$lookup": {
                    "from": "eruptions",
                    "localField": "id",
                    "foreignField": "volcano_id",
                    "as": "eruptions"
                }
            },
            {
                "$project": {
                    "volcano_id": "$id",
                    "volcano_num": "$num",
                    "volcano_name": "$name",
                    "country": 1,
                    "region": 1,
                    "timeErupt": 1,
                    "location": 1,
                    "elevation": 1,
                    "morphology": 1,
                    "status": 1,
                    "position": 1,

                    "eruption_stats": {
                        "total_eruptions": {"$size": "$eruptions"},
                        "last_eruption_date": {"$max": "$eruptions.date"},
                        "max_vei": {"$max": "$eruptions.vei"},
                        "total_deaths": {"$sum": "$eruptions.deaths.amount"},
                        "total_damage": {"$sum": "$eruptions.damage.houses_destroyed"},
                        "total_injuries": {"$sum": "$eruptions.injuries.amount"},
                        "total_missing": {"$sum": "$eruptions.missing.amount"}
                    },

                    "eruptions": {
                        "$map": {
                            "input": {
                                "$sortArray": {
                                    "input": "$eruptions",
                                    "sortBy": { "date": 1 }
                                }
                            },
                            "as": "eruption",
                            "in": {
                                "id": "$$eruption.id",
                                "year": "$$eruption.year",
                                "vei": "$$eruption.vei",
                                "date": "$$eruption.date",
                                "significant": "$$eruption.significant",
                                "publish": "$$eruption.publish",
                                "eruption": "$$eruption.eruption",
                                "agents": "$$eruption.agent",
                                "timeErupt": "$$eruption.timeErupt",
                                "related_events": {
                                    "tsunami": "$$eruption.tsunamiEventId",
                                    "earthquake": "$$eruption.earthquakeEventId"
                                },
                                "deaths": {
                                    "amount": "$$eruption.deaths",
                                    "order": "$$eruption.deathsAmountOrder"
                                },
                                "damage": {
                                    "order": "$$eruption.damageAmountOrder",
                                    "houses_destroyed": "$$eruption.housesDestroyed"
                                },
                                "injuries": {
                                    "amount": "$$eruption.injuries",
                                    "order": "$$eruption.injuriesAmountOrder"
                                },
                                "missing": {
                                    "amount": "$$eruption.missing",
                                    "order": "$$eruption.missingAmountOrder"
                                }
                            }
                        }
                    }
                }
            }
        ]


        collection_aggregated = self.db['volcanoes_aggregated']
        collection_aggregated.drop()


        results = list(self.db['volcanoes'].aggregate(pipeline, allowDiskUse=True))

        if results:
            collection_aggregated.insert_many(results)
            logger.info(f"✅ Created {len(results)} aggregated volcano documents")
        else:
            logger.warning("⚠️ No aggregated documents created")


    def load_all_data(self, csv_dir: str):
        try:
            self.load_regions(os.path.join(csv_dir, "volcano_regions.csv"))
            self.load_epochs(os.path.join(csv_dir, "eruption_times.csv"))

            self.load_volcanoes(os.path.join(csv_dir, "volcanoes.csv"))

            self.load_eruptions(os.path.join(csv_dir, "eruptions.csv"))
            self.create_aggregated_volcanoes()
            self.create_indexes()



            logger.info("✅ All successfully loaded!")

        except Exception as e:
            logger.error(f"❌ Error during loading: {e}")
            raise

    def close(self):
        self.client.close()
        logger.info("closed connection to mongodb")

def make_point(lon, lat):
    return {
        "type": "Point",
        "coordinates": [float(lon), float(lat)]
    }




if __name__ == "__main__":
    CSV_DIRECTORY = "/app/data"
    MONGO_HOST = "mongodb"
    MONGO_USERNAME = os.getenv("MONGO_USERNAME", "mongo")
    MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "password1234")
    DB_NAME = os.getenv("MONGO_DATABASE", "volcanoes")

    loader = MongoCSVLoader(
        mongo_host=MONGO_HOST,
        db_name=DB_NAME,
        username=MONGO_USERNAME,
        password=MONGO_PASSWORD
    )

    try:
        loader.load_all_data(CSV_DIRECTORY)
    finally:
        loader.close()