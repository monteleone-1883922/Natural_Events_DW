import os
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
URL_COUNTIES = 'https://www.spc.noaa.gov/wcm/stnindex_all.txt'
URLS = {'tsunami': 'https://www.ngdc.noaa.gov/hazel/hazard-service/api/v1/tsunamis/events',
        'earthquake': 'https://www.ngdc.noaa.gov/hazel/hazard-service/api/v1/earthquakes',
        'volcano': 'https://volcano.si.edu/database/list_volcano_holocene_excel.cfm',
        'eruption': 'https://volcano.si.edu/database/GVP_Eruption_Search_Result.xlsx',
        'tornado': 'https://www.spc.noaa.gov/wcm/data/1950-2024_all_tornadoes.csv',
        }
BASE_DATA_PATH = "../data/"
COUNTIES_FILE_TXT = BASE_DATA_PATH + "counties.txt"
COUNTIES_FILE_CSV = BASE_DATA_PATH + "counties.csv"
HEADERS_COUNTIES = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
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

    response = requests.get(URL_COUNTIES, headers=HEADERS_COUNTIES)
    response.raise_for_status()  # Controlla ancora gli errori

    with open(COUNTIES_FILE_TXT, "wb") as f:
        f.write(response.content)
    dataframe ={
        "county_name": [],
        "county_fips": [],
        "state_name": [],
        "state_fips": [],
        "city_name": [],
        "city_fips": [],
        "city_region": []
    }
    actual_state = ""
    actual_taste_fips = -1
    ignore = False
    independent_cities = False
    with open(COUNTIES_FILE_TXT) as f:
        for line in f:
            line = line.strip()
            if independent_cities:
                independent_cities, ignore = handle_independent_cities(dataframe, line, actual_state, actual_taste_fips)
            elif line.find("-") != -1 and actual_state == line.split('-')[0].strip():
                ignore = False
            elif line.find("-") != -1:
                if line.split('-')[0].strip() != "VIRGINIA CITIES":
                    actual_state = line.split('-')[0].strip()
                    actual_taste_fips = int(line.split('-')[1].strip()[-2:])
                    ignore = False
                else:
                    independent_cities = True
            elif not ignore and (line == "" or line.find('COUNTIES') != -1):
                ignore = True
            elif not ignore:
                line_split = split_line(line)
                for i, item in enumerate(line_split):
                    if i % 2 == 0:
                        dataframe["county_name"].append(item.strip())
                        dataframe["state_name"].append(actual_state)
                        dataframe["state_fips"].append(actual_taste_fips)
                        dataframe["city_fips"].append(None)
                        dataframe["city_region"].append(None)
                        dataframe["city_name"].append(None)
                    elif item.strip() != "":
                        dataframe["county_fips"].append(int(item.strip()))


    df = pl.DataFrame(dataframe)
    df.write_csv(COUNTIES_FILE_CSV)
    os.remove(COUNTIES_FILE_TXT)
    print("Counties retrieved and converted to csv")

def handle_independent_cities(df: dict, line: str, state: str, state_fips: int):
    if line == "":
        return False, True
    else:
        elements = []
        for el in line.split('\t'):
            if el.strip() != "":
                elements += [it.strip() for it in el.split(' ') if it.strip() != ""]
        city_name = ""
        for i, el in enumerate(elements):
            if el.isdigit():
                df["city_name"].append(city_name.strip())
                df["county_name"].append(None)
                df["state_name"].append(state)
                df["state_fips"].append(state_fips)
                df["city_fips"].append(int(el))
                if len(elements) > i+1:
                    df["county_fips"].append(int(elements[i+1]))
                else:
                    df["county_fips"].append(None)
                if len(elements) > i+2:
                    df["city_region"].append(elements[i+2])
                else:
                    df["city_region"].append(None)
                return True, False
            else:
                city_name += el + " "
        raise Exception("no digit!")










def strip_deep(string: str):
    return string.strip().strip('*').strip('*').strip('*').strip('#').strip('#').strip('o').strip('O')



def split_line(line: str):
    line_split = []
    elements = []
    for el in line.split('\t'):
        if el.strip() != "":
            elements += [it.strip() for it in el.split(' ') if it.strip() != ""]
    county_name = ""
    for el in elements:
        if strip_deep(el).isdigit():
            line_split.append(county_name.strip())
            line_split.append(strip_deep(el))
        else:
            county_name += el + " "

    return line_split




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


