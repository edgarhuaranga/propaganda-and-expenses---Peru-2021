import requests
import json
import csv
import pandas as pd
import numpy as np

minimum_amount = 4600


def transform_spend(row, field):
    row = row.replace("\'", "\"")
    res = json.loads(row)
    return int(res[field])


def read_id_file():
    data = pd.read_csv("FacebookAdLibraryReport_2022-07-23_PE_lifelong_advertisers.csv", dtype=str).astype({"page_id": 'int'})
    presidenciales = pd.read_csv('ids_presidenciales.csv').dropna()
    congresales = pd.read_csv('ids_congreso.csv').dropna()
    paginas = pd.concat([presidenciales, congresales]).astype({"page_id": 'int'})

    analizable = data[~data['page_id'].isin(paginas['page_id'].tolist())]

    datax = analizable[analizable['amount_spent_pen'] != '≤100'].astype({'amount_spent_pen': 'float', 'number_of_ads': 'int'})
    datax.to_csv("anomalias/dataxx.csv")

    paginasx = datax[['page_id', 'page_name','number_of_ads', 'amount_spent_pen']].groupby(['page_id', 'page_name'])
    paginasx = paginasx.sum()
    paginasx[paginasx['amount_spent_pen'] > minimum_amount].sort_values('amount_spent_pen', ascending=False).to_csv('anomalias/acumulado.csv')


def filter_ads():
    data = pd.read_csv('anomalias/anuncios_total.csv')
    data['elecciones'] = ((data['ad_creation_time'] > '2020-07-08') & (data['ad_creation_time'] < '2021-08-11'))
    anuncios_elecciones = data[data['elecciones']==True]
    #print(anuncios_elecciones)
    anuncios_elecciones['lower_bound'] = anuncios_elecciones['spend'].apply(lambda x: transform_spend(x, 'lower_bound'))
    anuncios_elecciones['upper_bound'] = anuncios_elecciones['spend'].apply(lambda x: transform_spend(x, 'upper_bound'))
    anuncios_elecciones[anuncios_elecciones['page_id'] == 101816778261420].to_csv('anomalias/piensape.csv')
    #print(anuncios_elecciones)
    #aggregated = anuncios_elecciones[['page_id','currency', 'lower_bound', 'upper_bound']].groupby(['page_id', 'currency']).sum().unstack()
    #aggregated = anuncios_elecciones[['id', 'page_id']].groupby(['page_id']).count().unstack()
    #print(aggregated.to_csv('anomalias/cantidad_anuncios_paginas.csv'))
    #aggregated

if __name__ == '__main__':
    #read_id_file()
    filter_ads()
