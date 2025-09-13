import sys

def print_progress_bar(percentuale, lunghezza_barra=20):
    blocchi_compilati = int(lunghezza_barra * percentuale)
    barra = "[" + "=" * (blocchi_compilati - 1) + ">" + " " * (lunghezza_barra - blocchi_compilati) + "]"
    sys.stdout.write(f"\r{barra} {percentuale * 100:.2f}% completo")
    sys.stdout.flush()
    if percentuale == 1:
        print("")

# 1. URL dell'API REST
SETUP_DATA = {
    'tsunami': {
        'url': 'https://www.ngdc.noaa.gov/hazel/hazard-service/api/v1/tsunamis/events',
        'url-causes': 'https://www.ngdc.noaa.gov/hazel/hazard-service/api/v1/descriptors/tsunami/causes',
        'url-validity': 'https://www.ngdc.noaa.gov/hazel/hazard-service/api/v1/descriptors/tsunami/validity',
        'url-regions': 'https://www.ngdc.noaa.gov/hazel/hazard-service/api/v1/descriptors/tsunami/regions',
        'url-warning-status': 'https://www.ngdc.noaa.gov/hazel/hazard-service/api/v1/descriptors/tsunami/warning-statuses',
        'output_file': 'tsunami.csv',
        'database': 'elasticsearch'
    },
    'earthquake': {
        'url': 'https://www.ngdc.noaa.gov/hazel/hazard-service/api/v1/earthquakes',
        'url-regions': 'https://www.ngdc.noaa.gov/hazel/hazard-service/api/v1/descriptors/earthquake/regions',
        'output_file': 'earthquake.csv',
        'database': 'cassandra'
    },
    'volcano': {
        'url': 'https://www.ngdc.noaa.gov/hazel/hazard-service/api/v1/volcanolocs',
        'output_file': 'volcanoes.csv',
        'database': 'mongodb'
    },
    'eruption': {
        'url': 'https://www.ngdc.noaa.gov/hazel/hazard-service/api/v1/volcanoes',
        'output_file': 'eruptions.csv',
        'database': 'mongodb'
    },
    'volcano-region': {
        'url': 'https://www.ngdc.noaa.gov/hazel/hazard-service/api/v1/descriptors/volcano/regions',
        'output_file': 'volcano_regions.csv',
        'database': 'mongodb'
    },
    'eruption-times': {
        'output_file': 'eruption_times.csv',
        'database': 'mongodb',
        'data': [
            {
                "type": "D1",
                "description": "Last known eruption 1964 or later"
             },
            {
                "type": "D2",
                "description": "Last known eruption 1900-1963"
            },
            {
                "type": "D3",
                "description": "Last known eruption 1800-1899"
            },
            {
                "type": "D4",
                "description": "Last known eruption 1700-1799"
            },
            {
                "type": "D5",
                "description": "Last known eruption 1500-1699"
            },
            {
                "type": "D6",
                "description": "Last known eruption A.D. 1-1499"
            },
            {
                "type": "D7",
                "description": "Last known eruption B.C. (Holocene)"
            },
            {
                "type": "U",
                "description": "Undated, but probable Holocene eruption"
            },
            {
                "type": "Q",
                "description": "Quaternary eruption(s) with the only known Holocene activity being hydrothermal"
            },
            {
                "type": "?",
                "description": "Uncertain Holocene eruption"
            }
        ]
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

def get_output_filename_from_setup(setup):
    return get_filename_from_setup(setup, 'output_file')