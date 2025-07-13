import sys

import requests
import polars as pl

def print_progress_bar(percentuale, lunghezza_barra=20):
    blocchi_compilati = int(lunghezza_barra * percentuale)
    barra = "[" + "=" * (blocchi_compilati - 1) + ">" + " " * (lunghezza_barra - blocchi_compilati) + "]"
    sys.stdout.write(f"\r{barra} {percentuale * 100:.2f}% completo")
    sys.stdout.flush()

# 1. URL dell'API REST
URL_VOLCANO = 'https://www.ngdc.noaa.gov/hazel/hazard-service/api/v1/volcanoes/{id}/info'
URL_COUNTIES = 'https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/georef-united-states-of-america-county/records'
URLS = {'tsunami': 'https://www.ngdc.noaa.gov/hazel/hazard-service/api/v1/tsunamis/events',
        'earthquake': 'https://www.ngdc.noaa.gov/hazel/hazard-service/api/v1/earthquakes',
        'volcano': 'https://volcano.si.edu/database/list_volcano_holocene_excel.cfm',
        'eruption': 'https://volcano.si.edu/database/GVP_Eruption_Search_Result.xlsx',
        'tornado': 'https://www.spc.noaa.gov/wcm/data/1950-2024_all_tornadoes.csv',
        }
BASE_DATA_PATH = "../data/"
COUNTIES_FILE = BASE_DATA_PATH + "counties.csv"
# Parametri della query

def build_county_json(county_raw):
    if len(county_raw['ste_code']) != 1 or len(county_raw['coty_code']) != 1 or len(county_raw['coty_name_long']) != 1 \
            or len(county_raw['coty_name']) != 1 or len(county_raw['ste_name']) != 1:
        raise Exception('invalid county format')
    return {
        "state_FIPS": int(county_raw['ste_code'][0]),
        "state_name": county_raw['ste_name'][0],
        "county_code": int(county_raw['coty_code'][0]),
        "county_name": county_raw['coty_name'][0],
        "area": county_raw['coty_area_code'],
        "county_type": county_raw['coty_type'],
        "county_FIPS": int(county_raw['coty_fp_code']),
        "county_gnis_code": int(county_raw['coty_gnis_code']),
        "county_full_name": county_raw['coty_name_long'][0]
    }

def retrieve_counties():
    num_counties = 100000
    i = 0
    counties = []
    while i < num_counties:
        print_progress_bar(i/num_counties)
        params = {
            'limit': 100,
            'offset': i
        }
        response = requests.get(URL_COUNTIES, params=params)
        data = response.json()
        num_counties = data['total_count']
        counties += [build_county_json(county_raw) for county_raw in data['results']]
        i += 100
    print("\ncounties: ", len(counties))
    df = pl.DataFrame(counties)
    del data, response, counties
    df.write_csv(COUNTIES_FILE)

def retrieve_data(url, output_file):
    num_pages = 100000
    i=1
    items = []
    print("retrieving data")
    while i <= num_pages:
        print_progress_bar(i / num_pages)
        params = {
            'page': i  # Puoi cambiare questo valore a piacere
        }

        # 2. Effettua la richiesta con i parametri
        response = requests.get(url, params=params)
        data = response.json()  # supponiamo sia una lista di dizionari
        items += data['items']
        num_pages = data['totalPages']
        i += 1

    del data, response

    # 4. Converte in DataFrame e mostra un'anteprima
    df = pl.DataFrame(items)
    del items
    #df = df[df['volcanoEventId'].notna()]
    df = df.with_columns(
        pl.lit(None).alias("related_volcano_number"),  # Colonna con valori None
        pl.lit(None).alias("related_eruption_date")
    )

    volcano_ids = df.filter(pl.col("volcanoEventId").is_not_null())
    volcano_ids = volcano_ids.select(["id", "volcanoEventId"])
    unfound_volcanoes = 0
    print("\nRetrieving volcano info")
    for i,row in enumerate(volcano_ids.iter_rows(named=True)):
        print_progress_bar(i/len(volcano_ids))
        response = requests.get(URL_VOLCANO.format(id=str(row["volcanoEventId"])))
        if response.status_code != 200:
            print("error response volcano event")
            exit(1)
        data = response.json()
        if "newNum" in data:
            volcano_num = data["newNum"]
            df = df.with_columns([
                # Modifica primo campo
                pl.when(pl.col("id") == row['id'])
                .then(volcano_num)
                .otherwise(pl.col("related_volcano_number"))
                .alias("related_volcano_number"),

                # Modifica secondo campo
                pl.when(pl.col("id") == row['id'])
                .then(pl.col("year"))
                .otherwise(pl.col("related_eruption_date"))
                .alias("related_eruption_date")
            ])
        else:
            unfound_volcanoes += 1
    print("\nUnfound volcanoes: ", unfound_volcanoes)
    df.write_csv(output_file)

def retrieve_data_simple(url, filename):

    # Scarica il file
    response = requests.get(url, stream=True)
    response.raise_for_status()  # Verifica errori

    with open(filename, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    print(f"File saved: {filename}")
    if url_idx == 'tornado':
        fix_tornado_ids(filename)
        retrieve_counties()


def fix_tornado_ids(filename):
    df = pl.read_csv(filename)


    df = df.with_columns(
        (pl.col("yr").cast(pl.Utf8) + pl.col("om").cast(pl.Utf8)).cast(pl.Int64).alias("id")
    )
    df.write_csv(filename)
    print(f"added  id column to file: {filename}")



if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python script_name.py <url> <output_file>")
        sys.exit(1)
    url_idx = sys.argv[1]
    if url_idx not in URLS.keys():
        print("Invalid URL index.")
        print("Valid indices:", list(URLS.keys()))
        sys.exit(1)
    filename = BASE_DATA_PATH + sys.argv[2]
    if url_idx in ['tsunami', 'earthquake']:
        retrieve_data(URLS[url_idx], filename)
    else:
        retrieve_data_simple(URLS[url_idx], filename)


