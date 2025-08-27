import polars as pl
from pymongo import MongoClient
import os
import logging




class MongoCSVLoader:
    def __init__(self,username, password, logger, mongo_host="mongodb", mongo_port=27017, db_name="volcanoes"):
        self.client = MongoClient(f"mongodb://{username}:{password}@{mongo_host}:{mongo_port}/{db_name}?authSource=admin")
        self.db = self.client[db_name]
        self.logger = logger
        self.logger.info(f"Connected to MongoDB: {mongo_host}:{mongo_port}/{db_name}")

    def create_indexes(self):
        self.logger.info("Creating database indexes...")

        try:
            # INDICI PER COLLECTION VOLCANOES
            volcanoes_collection = self.db['volcanoes']

            # Indice univoco su id (primary key naturale)
            volcanoes_collection.create_index("id", unique=True)
            self.logger.info("✅ Created unique index on volcanoes.id")

            # Indice geografico per query spaziali
            volcanoes_collection.create_index([("position", "2dsphere")])
            self.logger.info("✅ Created 2dsphere index on volcanoes.position")

            # Indici per filtri comuni
            volcanoes_collection.create_index("country")
            volcanoes_collection.create_index("region")
            volcanoes_collection.create_index("status")
            volcanoes_collection.create_index("morphology")
            self.logger.info("✅ Created indexes on volcanoes filter fields")

            # Indice composto per query geografiche per paese/regione
            volcanoes_collection.create_index([("country", 1), ("region", 1)])
            self.logger.info("✅ Created compound index on volcanoes.country+region")

            # INDICI PER COLLECTION ERUPTIONS
            eruptions_collection = self.db['eruptions']

            # Indice univoco su id
            eruptions_collection.create_index("id", unique=True)
            self.logger.info("✅ Created unique index on eruptions.id")

            # INDICE FONDAMENTALE: volcano_id per i lookup
            eruptions_collection.create_index("volcano_id")
            self.logger.info("✅ Created index on eruptions.volcano_id (for lookups)")

            # Indici per range query temporali
            eruptions_collection.create_index("date")
            self.logger.info("✅ Created indexes on eruptions temporal fields")

            # Indice per VEI (Volcanic Explosivity Index)
            eruptions_collection.create_index("vei")
            self.logger.info("✅ Created index on eruptions.vei")

            # Indici per query su eventi significativi
            eruptions_collection.create_index("significant")
            self.logger.info("✅ Created indexes on eruptions significance fields")

            # Indice geografico per eruzioni
            eruptions_collection.create_index([("position", "2dsphere")])
            self.logger.info("✅ Created 2dsphere index on eruptions.position")

            # Indici composti per query complesse comuni
            eruptions_collection.create_index([("volcano_id", 1), ("vei", -1)])  # eruzioni più forti per vulcano
            self.logger.info("✅ Created compound indexes on eruptions")

            # INDICI PER COLLECTION VOLCANOES_AGGREGATED
            volcanoes_agg_collection = self.db['volcanoes_aggregated']

            # Indice su volcano_id
            volcanoes_agg_collection.create_index("volcano_id", unique=True)
            self.logger.info("✅ Created unique index on volcanoes_aggregated.volcano_id")

            # Indici sulle statistiche per ordinamenti e filtri
            volcanoes_agg_collection.create_index("eruption_stats.total_eruptions")
            volcanoes_agg_collection.create_index("eruption_stats.last_eruption_date")
            volcanoes_agg_collection.create_index("eruption_stats.max_vei")
            volcanoes_agg_collection.create_index("eruption_stats.total_deaths")
            self.logger.info("✅ Created indexes on volcanoes_aggregated statistics")

            # Indice geografico
            volcanoes_agg_collection.create_index([("position", "2dsphere")])
            self.logger.info("✅ Created 2dsphere index on volcanoes_aggregated.position")

            # Indici per filtri comuni
            volcanoes_agg_collection.create_index("country")
            volcanoes_agg_collection.create_index("region")
            volcanoes_agg_collection.create_index("status")
            self.logger.info("✅ Created indexes on volcanoes_aggregated filter fields")

            self.logger.info("🎯 All database indexes created successfully!")

        except Exception as e:
            self.logger.error(f"❌ Error creating indexes: {e}")
            raise


    def load_volcanoes(self, csv_path: str, regions_df: pl.DataFrame, epoch_df: pl.DataFrame):

        self.logger.info("Loading volcanoes...")
        df_volcanoes = pl.read_csv(csv_path).with_columns(
                pl.struct([
                    pl.lit("Point").alias("type"),
                    pl.concat_list([pl.col("longitude"), pl.col("latitude")]).alias("coordinates")
                ]).alias("position")
            ).join(
                epoch_df, left_on='timeErupt', right_on='type', how='left'
            ).drop(
                ["timeErupt"]
            ).rename(
                {"description": "timeErupt"}
            ).join(
                regions_df, left_on='region', right_on='id', how='left'
            ).drop(
                ["region"]
            ).rename(
                {"description": "region"}
            )
        self.logger.info(f"Schema is {df_volcanoes.schema}")


        collection = self.db['volcanoes']
        collection.drop()
        documents = df_volcanoes.to_dicts()
        if documents:
            result = collection.insert_many(documents)
            self.logger.info(f"Inserted {len(result.inserted_ids)} volcanoes")



    def load_eruptions(self, csv_path: str, epoch_df: pl.DataFrame):

        self.logger.info("Loading eruptions...")

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
            ).join(
                epoch_df, left_on='timeErupt', right_on='type', how='left'
            ).drop(
                ["timeErupt"]
            ).rename(
                {"description": "timeErupt"}
            )
        self.logger.info(f"Schema is {df_eruptions.schema}")
        collection = self.db['eruptions']
        collection.drop()
        documents = df_eruptions.to_dicts()

        if documents:
            result = collection.insert_many(documents)
            self.logger.info(f"Inserted {len(result.inserted_ids)} eruptions")

    def create_aggregated_volcanoes(self):
        self.logger.info("Creating aggregated volcanoes collection...")

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
            self.logger.info(f"✅ Created {len(results)} aggregated volcano documents")
        else:
            self.logger.warning("⚠️ No aggregated documents created")


    def load_all_data(self, csv_dir: str):
        try:
            regions = pl.read_csv(os.path.join(csv_dir, "volcano_regions.csv"))
            eruption_times = pl.read_csv(os.path.join(csv_dir, "eruption_times.csv"))
            self.load_volcanoes(os.path.join(csv_dir, "volcanoes.csv"), regions, eruption_times)

            self.load_eruptions(os.path.join(csv_dir, "eruptions.csv"), eruption_times)
            self.create_aggregated_volcanoes()
            self.create_indexes()



            self.logger.info("✅ All successfully loaded!")

        except Exception as e:
            self.logger.error(f"❌ Error during loading: {e}")
            raise

    def close(self):
        self.client.close()
        self.logger.info("closed connection to mongodb")

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

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("MongoLoader")

    loader = MongoCSVLoader(
        mongo_host=MONGO_HOST,
        db_name=DB_NAME,
        logger=logger,
        username=MONGO_USERNAME,
        password=MONGO_PASSWORD
    )

    try:
        loader.load_all_data(CSV_DIRECTORY)
    finally:
        loader.close()