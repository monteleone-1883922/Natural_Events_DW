import os

import requests
import polars as pl
from utils import *



# Parametri della query



def excel_to_csv(input_path, output_path, sheet_name=None):
    """
    Converte un file Excel (.xls o .xlsx) in CSV

    Args:
        input_path (str): Percorso del file Excel di input
        output_path (str): Percorso del file CSV di output
        sheet_name (str, optional): Nome del foglio da convertire. Se None, usa il primo foglio.
    """
    # Legge il file Excel
    df = pl.read_excel(input_path)

    # Scrive il DataFrame in formato CSV
    df.write_csv(output_path)
    os.remove(input_path)
    print(f"File successfully converted: {input_path} -> {output_path}")

def retrieve_counties():
    url = get_url_from_setup('county')
    response = requests.get(url, headers=HEADERS_COUNTIES)
    response.raise_for_status()  # Controlla ancora gli errori
    tmp_file = get_tmp_filename_from_setup('county')

    with open(tmp_file, "wb") as f:
        f.write(response.content)
    df_counties ={
        "county_name": [],
        "county_fips": [],
        "state_name": [],
        "state_fips": []
    }
    df_independent_cities = {
        "state_fips": [],
        "city_name": [],
        "city_fips": [],
        "county_fips": [],
        "city_region": []
    }
    df_big_independent_cities = {
        "state_fips": [],
        "city_name": [],
        "city_fips": []
    }
    actual_state = ""
    actual_taste_fips = -1
    ignore = False
    independent_cities = False
    with open(tmp_file) as f:
        for line in f:
            line = line.strip()
            if independent_cities:
                independent_cities, ignore = handle_independent_cities(df_independent_cities, df_big_independent_cities, line, actual_state, actual_taste_fips)
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
                        df_counties["county_name"].append(item.strip())
                        df_counties["state_name"].append(actual_state)
                        df_counties["state_fips"].append(actual_taste_fips)
                    elif item.strip() != "":
                        df_counties["county_fips"].append(int(item.strip()))

    df = pl.DataFrame(df_counties)
    df.write_csv(get_filename_from_setup('county', 'county'))
    os.remove(tmp_file)
    print("Counties retrieved and converted to csv")

def handle_independent_cities(df_independent_cities: dict, df_big_independent_cities: dict, line: str, state: str, state_fips: int):
    if line == "":
        df1 = pl.DataFrame(df_independent_cities)
        df1.write_csv(get_filename_from_setup('county', 'independent_city'))
        df2 = pl.DataFrame(df_big_independent_cities)
        df2.write_csv(get_filename_from_setup('county', 'big_independent_city'))
        print("Finished preparing independent cities")
        return False, True
    else:
        elements = []
        for el in line.split('\t'):
            if el.strip() != "":
                elements += [it.strip() for it in el.split(' ') if it.strip() != ""]
        city_name = ""
        for i, el in enumerate(elements):
            if el.isdigit():
                if len(elements) > i+2:
                    df_independent_cities["city_name"].append(city_name.strip())
                    df_independent_cities["state_fips"].append(state_fips)
                    df_independent_cities["city_fips"].append(int(el))
                    df_independent_cities["city_region"].append(elements[i+2])
                    df_independent_cities["county_fips"].append(int(elements[i + 1]))
                else:
                    df_big_independent_cities["city_name"].append(city_name.strip())
                    df_big_independent_cities["state_fips"].append(state_fips)
                    df_big_independent_cities["city_fips"].append(int(el))
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

def setup_tornadoes():
    tmp_filename = get_tmp_filename_from_setup('tornado')
    retrieve_data_simple(get_url_from_setup('tornado'), tmp_filename)
    df = pl.read_csv(tmp_filename)
    df = fix_tornado_ids(df)
    tornado_df = df.filter(
        ((pl.col("ns") == 1) & (pl.col("sn") == 1) & (pl.col("sg") == 1)) |
        ((pl.col("ns") != 1) & (pl.col("sn") == 0) & (pl.col("sg") == 1))
    )
    traces_df = df.filter((pl.col("ns") != 1) & (pl.col("sn") == 1) & (pl.col("sg") == 2))
    link_counties_df = df.filter((pl.col("sg") == -9))

    tornado_df.write_csv(get_filename_from_setup('tornado', 'tornado'))
    traces_df.write_csv(get_filename_from_setup('tornado', 'trace'))
    link_counties_df.write_csv(get_filename_from_setup('tornado', 'link_county'))
    os.remove(tmp_filename)
    print('Finished setup Tornadoes')

def fix_tornado_ids(df):
    df = df.with_columns(
        (pl.col("yr").cast(pl.Utf8) + pl.col("om").cast(pl.Utf8)).cast(pl.Int64).alias("id")
    )
    return df


def setup(url_idx):
    if url_idx not in SETUP_DATA.keys():
        print("Invalid URL index.")
        print("Valid indices:", list(SETUP_DATA.keys()))
        sys.exit(1)
    if url_idx in ['tsunami', 'earthquake']:
        pass #retrieve_data(URLS[url_idx], filename)
    elif url_idx == 'county':
        retrieve_counties()
    elif url_idx in ['eruption', 'volcano']:
        pass
    elif url_idx == 'tornado':
        setup_tornadoes()



if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python script_name.py <url> <output_file>")
        sys.exit(1)
    url_idx = sys.argv[1]
    setup(url_idx)


