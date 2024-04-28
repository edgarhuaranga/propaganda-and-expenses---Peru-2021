import requests
import json
import csv
import pandas as pd
import datetime


access_token = 'YOUR-TOKEN'
url_pattern = "https://graph.facebook.com/{}/ads_archive?ad_type={}&fields={}&ad_reached_countries={}&search_page_ids={}"\
              "&ad_active_status={}&limit={}"

api_version = 'v13.0'
ad_type = 'POLITICAL_AND_ISSUE_ADS'
fields = ['ad_creation_time, '
          'ad_creative_bodies, '
          'ad_creative_link_captions, '
          'ad_creative_link_descriptions, '
          'ad_creative_link_titles, '
          'ad_delivery_start_time, '
          'ad_delivery_stop_time, '
          'ad_snapshot_url, '
          'bylines, '
          'currency, '
          'spend, '
          'delivery_by_region, '
          'demographic_distribution, '
          'estimated_audience_size, '
          'impressions, '
          'languages, '
          'page_id, '
          'page_name, '
          'publisher_platforms']

filename = 'congresales/total_anuncios.csv'
countries = ['PE']
page_limit = 50
headers = {"Authorization": "Bearer {}".format(access_token)}
employee_info = ['id',
                 'ad_creation_time',
                 'ad_creative_bodies',
                 'ad_snapshot_url',
                 'ad_delivery_start_time',
                 'ad_delivery_stop_time',
                 'bylines',
                 'currency',
                 'spend',
                 'delivery_by_region',
                 'demographic_distribution',
                 'impressions',
                 'page_id',
                 'page_name',
                 'publisher_platforms']


def get(url):
    response = requests.get(url, headers=headers)
    data = response.json()
    try:
        for ad in data['data']:
            save_ad(ad)
        #print('===========')
    except KeyError:
        print(url)
        print(data)
        print("URL sin datos de publicidad")
    try:
        next_url = data['paging']['next']
        get(next_url)
    except KeyError:
        pass #print("Cerrando páginas")


def save_ad(ad):
    with open(filename, mode='a', encoding='utf-8') as ad_file:
        ad_writer = csv.DictWriter(ad_file, delimiter=',', fieldnames=employee_info, quotechar='"', quoting=csv.QUOTE_MINIMAL)
        cleaned_ad = {}
        for x in employee_info:
            try:
                cleaned_ad[x] = ad[x]
            except KeyError:
                cleaned_ad[x] = "no disponible"
        ad_writer.writerow(cleaned_ad)


def read_id_file():
    data = pd.read_csv("ids_congreso.csv", dtype=str)
    pages_ids = data[~data['page_id'].isnull()].page_id.values.tolist()
    return pages_ids


def transform_spend(row, field):
    row = row.replace("\'", "\"")
    res = json.loads(row)
    return int(res[field])


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
    inicio_analisis = '2020-04-10'  # 09 de julio 2020
    fin_analisis = '2020-04-11'  # 10 de agosto 2021
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


def silencio2(start, stop):
    print(start)
    print(stop)
    if start == 'no disponible' or stop == 'no disponible':
        return False

    fechas = pd.date_range(start, stop).tolist()
    silencio_primera = [datetime.datetime.strptime('2021-04-10', "%Y-%m-%d"),
                        datetime.datetime.strptime('2021-04-11', "%Y-%m-%d")] #
    silencio_segunda = [datetime.datetime.strptime('2021-06-05', "%Y-%m-%d"),
                        datetime.datetime.strptime('2021-06-06', "%Y-%m-%d")]  #
    for x in silencio_primera:
        if x in fechas:
            return True

    for x in silencio_segunda:
        if x in fechas:
            return True

    return False


if __name__ == '__main__':
    #creación de los headers del file
    #with open(filename, mode='w') as csv_file:
    #    writer = csv.DictWriter(csv_file, fieldnames=employee_info)
    #    writer.writeheader()
    #page_ids = read_id_file()

    #for page_group_ids in page_ids:
    #    print(page_group_ids)
    #    url = url_pattern.format(api_version, ad_type, fields, countries, page_group_ids, 'ALL', page_limit)
    #    get(url)

    anuncios = pd.read_csv(filename)
    #anuncios['elecciones'] = ((anuncios['ad_creation_time'] > '2020-07-08') & (anuncios['ad_creation_time'] < '2021-08-11'))
    #anuncios['electorales'] = anuncios.apply(lambda x: periodo(x['ad_delivery_start_time'], x['ad_delivery_stop_time']), axis=1)
    #anuncios['lower_bound'] = anuncios['spend'].apply(lambda x: transform_spend(x, 'lower_bound'))
    #anuncios['upper_bound'] = anuncios['spend'].apply(lambda x: transform_spend(x, 'upper_bound'))

    #anuncios[anuncios['electorales'] == True].to_csv('congresales/activos_elecciones.csv')

    #anuncios_activos = pd.read_csv('congresales/activos_elecciones.csv')
    #anuncios_q = anuncios_activos[['id', 'page_id']].groupby(['page_id']).count().unstack()
    #anuncios_q.to_csv('congresales/conteo_anuncios.csv')

    #anuncios_spend = anuncios_activos[['page_id', 'currency', 'lower_bound', 'upper_bound']].groupby(['page_id', 'currency']).sum().unstack()
    #anuncios_spend.to_csv('congresales/gasto_anuncios.csv')
    #anuncios['silencio1'] = anuncios.apply(lambda x: silencio(x['ad_delivery_start_time'], x['ad_delivery_stop_time']), axis=1)
    #anuncios['silencio2'] = anuncios.apply(lambda x: silencio2(x['ad_delivery_start_time'], x['ad_delivery_stop_time']), axis=1)
    #anuncios[anuncios['silencio1'] == True].to_csv('congresales/anuncios_silencio.csv')
    #anuncios[anuncios['silencio2'] == True].to_csv('congresales/anuncios_silencio2.csv')

    anuncios['silencio2'] = anuncios.apply(lambda x: silencio2(x['ad_delivery_start_time'], x['ad_delivery_stop_time']),
                                           axis=1)
    anuncios_prohibidos = anuncios[anuncios['silencio2'] == True]
    anuncios_prohibidos[
        ['id', 'ad_creation_time', 'ad_delivery_start_time', 'ad_delivery_stop_time', 'page_id', 'page_name',
         'ad_creative_bodies']].to_csv('congresales/anuncios_silencio_electoral.csv')
