from retrieve_form_rest_api import setup
from pathlib import Path

DIR_TO_CREATE = "../{database}/data/"
DATABASES = ["neo4j", "cassandra", "elasticsearch", "mongodb", "postgres", "metabase"]

def create_directories():
    for database in DATABASES:
        Path(DIR_TO_CREATE.format(database=database)).mkdir()
    print("Data directories created")

if __name__ == '__main__':
    create_directories()
    setup('tornado')
    setup('county')
    setup('volcano')
    setup('earthquake')
    setup('tsunami')

