import requests
import json
import csv
import pandas as pd
import datetime

access_token = 'YOUR-TOKEN'
url_pattern = "https://graph.facebook.com/{}/ads_archive?ad_type={}&fields={}&ad_reached_countries={}&search_page_ids={}&ad_active_status={}&limit={}"
api_version = 'v13.0'
ad_type = 'POLITICAL_AND_ISSUE_ADS'
countries = ['PE']
page_limit = 50
num_chunks = 10 #AdLibrary acepta máximo 10 páginas para consulta de anuncios
fields = ['ad_creation_time,'
          'ad_creative_bodies,'
          'ad_delivery_start_time,'
          'ad_delivery_stop_time,'
          'bylines,'
          'currency,'
          'spend,'
          'estimated_audience_size,'
          'impressions,'
          'page_id,'
          'page_name,'
          'publisher_platforms']
request_headers = {"Authorization": "Bearer {}".format(access_token)}
file_header = ['id'] + fields[0].split(',')


def get_ads(cargo, page_ids):
    for page_group_ids in page_ids:
        print(page_group_ids)
        url = url_pattern.format(api_version, ad_type, fields, countries, page_group_ids, 'ALL', page_limit)
        get(cargo, url)


def get(cargo, request_url):
    response = requests.get(request_url, headers=request_headers)
    data = response.json()
    try:
        for ad in data['data']:
            save_ad(cargo, ad)
    except KeyError:
        print("URL sin datos de publicidad: "+request_url)
    try:
        next_url = data['paging']['next']
        get(cargo, next_url)
    except KeyError:
        pass


def save_ad(cargo, ad):
    with open(cargo+".csv", mode='a', encoding='utf-8') as ad_file:
        ad_writer = csv.DictWriter(ad_file, delimiter=',', fieldnames=file_header, quotechar='"', quoting=csv.QUOTE_MINIMAL)
        cleaned_ad = {}
        for x in file_header:
            try:
                cleaned_ad[x] = ad[x]
            except KeyError:
                cleaned_ad[x] = "no disponible"
        ad_writer.writerow(cleaned_ad)


def read_ids_file(cargo):
    data = pd.read_csv("ids_paginas_elecciones.csv", dtype=str)
    pages_ids = data[data['cargo'] == cargo].page_id.values.tolist()
    chunks = int(len(pages_ids)/num_chunks)+1
    return [pages_ids[i*num_chunks:(i+1)*num_chunks] for i in range(chunks)]


def transform_json(row, field):
    row = row.replace("\'", "\"")
    res = json.loads(row)
    try:
        return int(res[field])
    except KeyError:
        pass


def periodo(start, stop):
    inicio_analisis = '2020-07-09'  # 09 de julio 2020
    fin_analisis = '2021-08-10'  # 10 de agosto 2021
    if inicio_analisis <= start <= stop <= fin_analisis:
        return True
    elif start < inicio_analisis <= stop <= fin_analisis:
        return True
    elif inicio_analisis < start <= fin_analisis <= stop:
        return True
    elif start < inicio_analisis < fin_analisis < stop:
        return True
    else:
        return False


def silencio(start, stop):
    print(start+"-->"+stop)
    if start == 'no disponible' or stop == 'no disponible':
        return False
    fechas = pd.date_range(start, stop).tolist()
    silencio_primera = [datetime.datetime.strptime('2021-04-10', "%Y-%m-%d"),
                        datetime.datetime.strptime('2021-04-11', "%Y-%m-%d"),
                        datetime.datetime.strptime('2021-06-05', "%Y-%m-%d"),
                        datetime.datetime.strptime('2021-06-06', "%Y-%m-%d")]
    for x in silencio_primera:
        if x in fechas:
            return True
    return False


def create_ads_file(cargo):
    with open(cargo+".csv", mode='w') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=file_header)
        writer.writeheader()
    page_ids = read_ids_file(cargo)
    get_ads(cargo, page_ids)


def filter_ads(cargo):
    anuncios = pd.read_csv(cargo+'.csv')
    anuncios['lower_bound_spend'] = anuncios['spend'].apply(lambda x: transform_json(x, 'lower_bound'))
    anuncios['upper_bound_spend'] = anuncios['spend'].apply(lambda x: transform_json(x, 'upper_bound'))
    anuncios['lower_bound_impressions'] = anuncios['impressions'].apply(lambda x: transform_json(x, 'lower_bound'))
    anuncios['upper_bound_impressions'] = anuncios['impressions'].apply(lambda x: transform_json(x, 'upper_bound'))

    anuncios['electorales'] = anuncios.apply(lambda x: periodo(x['ad_delivery_start_time'], x['ad_delivery_stop_time']), axis=1)
    anuncios['silencio'] = anuncios.apply(lambda x: silencio(x['ad_delivery_start_time'], x['ad_delivery_stop_time']), axis=1)
    anuncios[anuncios['silencio'] == True].to_csv(cargo+'/anuncios_prohibidos.csv')
    anuncios[anuncios['electorales'] == True].to_csv(cargo+'/anuncios_electorales.csv')


if __name__ == '__main__':
    create_ads_file('congresales')
    filter_ads('congresales')







