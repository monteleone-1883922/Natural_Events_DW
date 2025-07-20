import sys

def print_progress_bar(percentuale, lunghezza_barra=20):
    blocchi_compilati = int(lunghezza_barra * percentuale)
    barra = "[" + "=" * (blocchi_compilati - 1) + ">" + " " * (lunghezza_barra - blocchi_compilati) + "]"
    sys.stdout.write(f"\r{barra} {percentuale * 100:.2f}% completo")
    sys.stdout.flush()

# 1. URL dell'API REST
URL_VOLCANO = 'https://www.ngdc.noaa.gov/hazel/hazard-service/api/v1/volcanoes/{id}/info'
SETUP_DATA = {
    'tsunami': {
        'url': 'https://www.ngdc.noaa.gov/hazel/hazard-service/api/v1/tsunamis/events',
        'output_file': 'tsunami.csv'
    },
    'earthquake': {
        'url': 'https://www.ngdc.noaa.gov/hazel/hazard-service/api/v1/earthquakes',
        'output_file': 'earthquake.csv'
    },
    'volcano': {
        'url': 'https://volcano.si.edu/database/list_volcano_holocene_excel.cfm',
        'output_file': 'tsunami.csv'
    },
    'eruption': {
        'url': 'https://volcano.si.edu/database/GVP_Eruption_Search_Result.xlsx',
        'output_file': 'volcano.csv'
    },
    'tornado': {
        'url': 'https://www.spc.noaa.gov/wcm/data/1950-2024_all_tornadoes.csv',
        'tmp_file': 'tmp_tornadoes.csv',
        'tornado': 'tornadoes.csv',
        'trace': 'traces.csv',
        'link_county': 'link_counties.csv',
        'database': 'neo4j'
    },
    'county': {
        'url': 'https://www.spc.noaa.gov/wcm/stnindex_all.txt',
        'tmp_file': 'counties.txt',
        'county': 'counties.csv',
        'independent_city': 'independent_cities.csv',
        'big_independent_city': 'big_independent_cities.csv',
        'database': 'neo4j'
    }
}
BASE_DATA_PATH = '../{database}/data/'
HEADERS_COUNTIES = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

def get_filename_from_setup(setup, file):
    return BASE_DATA_PATH.format(database=SETUP_DATA[setup]['database']) + SETUP_DATA[setup][file]
def get_url_from_setup(setup):
    return SETUP_DATA[setup]['url']

def get_tmp_filename_from_setup(setup):
    return get_filename_from_setup(setup, 'tmp_file')