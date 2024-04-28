import requests
import json
import csv
import pandas as pd


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

filename = 'anomalias/anuncios_total.csv'
countries = ['PE']
page_limit = 50
headers = {"Authorization": "Bearer {}".format(access_token)}
employee_info = ['id',
                 'ad_creation_time',
                 'ad_creative_bodies',
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
        #print(cleaned_ad)
        ad_writer.writerow(cleaned_ad)


def read_id_file():
    data = pd.read_csv("anomalias/acumulado.csv", dtype=str)
    pages_ids = data[~data['page_id'].isnull()].page_id.values.tolist()
    chunks = int(len(pages_ids)/10)+1
    #return pages_ids
    return [pages_ids[i*10:(i+1)*10] for i in range(chunks)]


if __name__ == '__main__':
    with open(filename, mode='w') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=employee_info)
        writer.writeheader()
    page_ids = read_id_file()

    for page_group_ids in page_ids:
        print(page_group_ids)
        url = url_pattern.format(api_version, ad_type, fields, countries, page_group_ids, 'ALL', page_limit)
        get(url)

