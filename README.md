# Analysis of propaganda and declared expenses in the Peruvian elections 2021
This project seeks to analyse the spending of political parties in the 2021 presidential and congressional elections.

The data to be analysed has two specific sources. On the one hand, there are the expenditures declared by each political party and by each candidate for congress in [Claridad](https://claridad.onpe.gob.pe/). On the other hand, we obtain the data from [Meta Ad Library](https://www.facebook.com/ads/library/) with the information about the spending made by each fanpage on the different Meta platforms (Facebook, Instagram, Messenger). For each ad on each platform we have the following data:
* Page information.
* The name of the person who paid for the ad.
* Dates when the ad was active.
* Reach of the ad at a demographic level.

## Getting the data
Because we need the IDs of each page to be able to query the Meta API, a simple way to get these IDs is from the report [peru summary](https://www.facebook.com/ads/library/report/?source=archive-landing-page&country=PE) and search for the match with the match name. Some pages appear twice because in the **Disclaimer** column there are two or more people who made the payment for the same match page.

After filterin specific pages, we added three columns `cargo`,`partido_politico` and `region`. Cargo specifies if the page is running for presidencial or for a congress position. `partido_politico` is the column to save the name of the political party and `region` is the name of the region the candidate is running for.

This csv file is named `ids_paginas_elecciones.csv`.

After getting the IDs of pages to be analyzed, it's time to query the Ad Library API. But the API has some limitations like "_it's not possible to query for more than 10 pages_". Then it's necessary to split up the `page_id` column in groups of 10.

The file `anuncios.py` has all this process. The function `read_ids_file` takes the csv file and returns a list of lists with the maximum length 10 containing the `page_ids` of each fanpage after filtering the `cargo` column. 

```python
def read_ids_file(cargo):
    data = pd.read_csv("ids_paginas_elecciones.csv", dtype=str)
    pages_ids = data[data['cargo'] == cargo].page_id.values.tolist()
    chunks = int(len(pages_ids)/num_chunks)+1
    return [pages_ids[i*num_chunks:(i+1)*num_chunks] for i in range(chunks)]
```

Now it's time to query the API. We use the url pattern to make the request along with other parameters.
```python
url_pattern = "https://graph.facebook.com/{}/ads_archive?ad_type={}&fields={}&ad_reached_countries={}&search_page_ids={}&ad_active_status={}&limit={}"

## page_ids: list of lists containing the page_ids needed
## cargo: either if the page is for presidential or congress 
def get_ads(cargo, page_ids):
    for page_group_ids in page_ids:
        print(page_group_ids)
        url = url_pattern.format(api_version, ad_type, fields, countries, page_group_ids, 'ALL', page_limit)
        get(cargo, url)
```

```python 
## this function makes the corresponding request to the API, in case there is some error, should print a message of "no data". Then make a following request if there is more than one page in the results.
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
```

When the request is succesful we save the ad into a CSV file.
```python
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
```

Thus, the file `congresales.csv` contains all the ads of the official pages of individuals running for congress in 2021. The same happens with `presidenciales.csv`, this file contains all ads of official political parties that run for the presidencial spot in 2021.


## Filtering the data
After getting all the ads for every page involved in the elections 2021, it's time to filter according to some rules and dates.

We use lambda functions to ease the process of filtering and to have a cleaner code. First, one of the columns in the file has the JSON format, it's better to have this value as a separate column for aggregation purposes. Then we use the following function:

```python
def transform_json(row, field):
    row = row.replace("\'", "\"")
    res = json.loads(row)
    try:
        return int(res[field])
    except KeyError:
        pass
```

Then we add two "boolean" columns. One is 'electorales' columns which means that the ad ran during the election process. The second column is 'silencio' which means that the ad ran 24 hours before the official day of elections. Which is not allowed according to the Organic Law of Elections.

```python
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

```

These three functions are applied as lambda operations to the full dataframe. This will allow us to save distinct files according to the rol being monitored (presidential or congress).
```python
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
```

All charts, information and anomalies retrieved from this investigation are shown in the [following document](https://hiperderecho.org/wp-content/uploads/2022/09/informe-4_reportes-y-transparencia-en-gasto-de-propaganda-electoral-en-redes-sociales.pdf).

Also, this information has been replicated by local platforms: [here](https://ojo-publico.com/4525/podemos-peru-millonarios-gastos-digitales-investigados-por-lavado).


