from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from tqdm import tqdm
import time
import pandas as pd
from bs4 import BeautifulSoup
import numpy as np
import os
import random
import multiprocessing as mp
import requests
from math import radians, cos, sin, asin, sqrt

np.random.seed(13)

headers = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36",
    'referer': 'https://www.google.com/'
}


def distance(lat1: float, lat2: float, lon1: float, lon2: float) -> float:

    lon1 = radians(lon1)
    lon2 = radians(lon2)
    lat1 = radians(lat1)
    lat2 = radians(lat2)

    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2

    c = 2 * asin(sqrt(a))

    r = 6371

    return(c * r)


chrome_options = Options()
chrome_options.add_argument("--no-sandbox")
# chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument('start-maximized')
chrome_options.add_argument('disable-infobars')
chrome_options.add_argument('--disable-extensions')


def get_transport(point: tuple) -> int:

    driver = webdriver.Chrome(ChromeDriverManager(
        version='106.0.5249.61').install(), chrome_options=chrome_options)

    lat1, long1 = point
    url = f'https://yandex.ru/maps/35/krasnodar/?l=trf%2Ctrfe%2Cmasstransit&ll={lat1:05f}%2C{long1:05f}&mode=whatshere&whatshere%5Bpoint%5D={lat1:05f}%2C{long1:05f}&whatshere%5Bzoom%5D=18.3&z=14'
    # url = f'https://2gis.ru/krasnodar/geo/3237700966548103?m={lat1:05f}%2C{long1:05f}%2F16&layer=eta'
    driver.get(url)
    # r = requests.get(url=url, headers=headers).content
    # soup = BeautifulSoup(r, 'html.parser')
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    time.sleep(1)
    cls = "masstransit-animated-placemarks"
    # transport = len(soup.find_all('circle', {'cx': 13}))
    # print(soup.find_all(
    #     'div', {'class': 'masstransit-animated-placemarks__frame'}))
    transport = len(soup.find_all(
        'div', {'class': lambda x: x and cls in x}))
    driver.close()
    return transport


def get_dir(point1: tuple, point2: tuple) -> tuple:
    lat1, long1 = point1
    lat2, long2 = point2
    # driver = webdriver.Chrome(ChromeDriverManager(
    #     version='104.0.5112.79').install(), chrome_options=chrome_options)

    URL = f'https://2gis.ru/krasnodar/directions/points/{lat1:05f}%2C{long1:05f}%3B3237597887431657%7C{lat2:05f}%2C{long2:05f}%3B3237572117528577?m=38.963096%2C45.040056%2F16'
    r = requests.get(url=URL, headers=headers).content
    soup = BeautifulSoup(r, 'html.parser')
    time.sleep(random.uniform(1, 5))
    try:
        t = soup.find('div', {'class': '_iilzoe9'}).text
        d = soup.find('div', {'class': '_sgs1pz'}).text
    except:
        t = 0
        d = 0
    # driver.close()
    return t, d


def get_data(pts: list, start: int, out_dir: str):
    data = {
        'lat1': [],
        'lon1': [],
        'lat2': [],
        'lon2': [],
        'time': [],
        'distance': []
    }

    for lat1, lon1, lat2, lon2 in tqdm(pts, total=len(pts)):
        d = distance(lat1, lat2, lon1, lon2)
        if d >= 1 and d < 10:
            try:
                t, d = get_dir((lat1, lon1), (lat2, lon2))
            except:
                continue
            if t == 0:
                continue
            data['lon1'].append(lon1)
            data['lon2'].append(lon2)
            data['lat1'].append(lat1)
            data['lat2'].append(lat2)
            data['time'].append(t)
            data['distance'].append(d)

    pd.DataFrame(data).to_excel(f'{out_dir}/data_gis_{start}.xlsx')


if __name__ == '__main__':

    processes = []
    num_process = 20
    N = 100
    lats = np.random.uniform(low=38.9, high=39.1, size=(N,))
    longs = np.random.uniform(low=45, high=45.1, size=(N,))
    pts = []
    for lat1, lon1 in zip(lats, longs):
        for lat2, lon2 in zip(lats, longs):
            pts.append((lat1, lon1, lat2, lon2))

    # N = len(pts)

    rngs = [(i*int(N/num_process), (i+1)*int(N/num_process))
            for i in range(num_process)]
    print(rngs)
    out_dir = 'data'
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    s = time.time()
    for rng in rngs:
        start, end = rng
        p = mp.Process(target=get_data, args=[
                       pts[start:end], start, out_dir])
        p.start()
        processes.append(p)
    for p in processes:
        p.join()
    e = time.time()

    print(f'Processed in {round(e-s, 2)} seconds')
    fs = [pd.read_excel(f'data/{f}', index_col=0) for f in os.listdir('data')]
    pd.concat(fs).reset_index().drop(
        'index', axis=1).to_excel(f'data_gis_{N}.xlsx')

    data = pd.read_excel('data_gis_10000.xlsx')
    lats = pd.concat([data['lat1'], data['lat2']]).values
    longs = pd.concat([data['lon1'], data['lon2']]).values

    points = list(set(zip(lats, longs)))
    transports = {'lat': [], 'lon': [], 'transport': []}
    for point in tqdm(points, total=len(points)):
        lat, lon = point
        transports['lat'].append(lat)
        transports['lon'].append(lon)
        tr = get_transport((lat, lon))
        transports['transport'].append(tr)

    pd.DataFrame(transports).to_excel(f'transports_{N}.xlsx')
