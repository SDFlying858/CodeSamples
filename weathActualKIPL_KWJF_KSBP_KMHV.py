import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests
from sqlalchemy import create_engine
import datetime

remote = create_engine('',connect_args={"options": "-c statement_timeout=1000000"}, echo=True)

stations = {'1' : 'https://www.wrh.noaa.gov/mesowest/getobextXml.php?sid=KIPL&num=72',
            '2' : 'https://www.wrh.noaa.gov/mesowest/getobextXml.php?sid=KWJF&num=72',
            '3' : 'https://www.wrh.noaa.gov/mesowest/getobextXml.php?sid=KSBP&num=72',
            '4' : 'https://www.wrh.noaa.gov/mesowest/getobextXml.php?sid=KMHV&num=72'}

def wind_chiller(t, w):
    """function for calculating windchill
       takes t:temperature and w:wind_gust 
       returns wind_chill
    """
    t = float(t)
    w = float(w)
    if t >= 50:
        wc = 0
    elif w == 0:
        wc = 0
    else:
        wc = 35.74 + (0.6215*t) - 35.75*(w**0.16) + 0.4275*t*(w**0.16)
    wind_chill.append(wc)
    

def weather_parser(x):
    """function for parsing actual weather XML provided by
    https://www.wrh.noaa.gov/
    
    Sets return values to list for zipping
    """
    
    for time in x.findAll('ob'):
        oclock = time['time'].split(':')
        if oclock[1].startswith('00'):
            if oclock[0].startswith(str(yest)):
                starttime.append(time['time'])

    for time in x.findAll('ob'):
        oclock = time['time'].split(':')
        if oclock[1].startswith('00'):
            if oclock[0].startswith(str(yest)):
                temp = time.findNext('variable')
                if temp['value'] == 'CLR':
                    temp['value'] = np.nan
                temperature.append(temp['value'])

    for time in x.findAll('ob'):
        oclock = time['time'].split(':')
        if oclock[1].startswith('00'):
            if oclock[0].startswith(str(yest)):
                temp = time.findNext('variable')
                dew = temp.findNext('variable')
                if dew['value'] == 'W' or "WNW":
                    dew['value'] = np.nan
                dew_point.append(dew['value'])

    for time in x.findAll('ob'):
        oclock = time['time'].split(':')
        if oclock[1].startswith('00'):
            if oclock[0].startswith(str(yest)):
                temp = time.findNext('variable')
                dew = temp.findNext('variable')
                rel = dew.findNext('variable')
                if rel['value'] == 'SE' or 'SW':
                    rel['value'] = 0
                relative_humidity.append(rel['value'])

    for time in x.findAll('ob'):
        oclock = time['time'].split(':')
        if oclock[1].startswith('00'):
            if oclock[0].startswith(str(yest)):
                temp = time.findNext('variable')
                dew = temp.findNext('variable')
                rel = dew.findNext('variable')
                wd = rel.findNext('variable')
                if wd['value'] == 'N':
                    wd['value'] = 0
                wind_direction.append(wd['value'])

    for time in x.findAll('ob'):
        oclock = time['time'].split(':')
        if oclock[1].startswith('00'):
            if oclock[0].startswith(str(yest)):
                temp = time.findNext('variable')
                dew = temp.findNext('variable')
                rel = dew.findNext('variable')
                wd = rel.findNext('variable')
                cardinaldirection = wd.findNext('variable')
                ws = cardinaldirection.findNext('variable')
                if ws['value'] == 'Clear' or 'CLR':
                    ws['value'] = 0
                wind_gust.append(ws['value'])

    for time in x.findAll('ob'):
        oclock = time['time'].split(':')
        if oclock[1].startswith('00'):
            if oclock[0].startswith(str(yest)):
                temp = time.findNext('variable')
                dew = temp.findNext('variable')
                rel = dew.findNext('variable')
                wd = rel.findNext('variable')
                cardinaldirection = wd.findNext('variable')
                ws = cardinaldirection.findNext('variable')
                visibility = ws.findNext('variable')
                null_weather = visibility.findNext('variable')
                null_clouds = null_weather.findNext('variable')
                slp = null_clouds.findNext('variable')
                if slp['value'] == 'CLR' or 'FEW005':
                    slp['value'] = np.nan
                slpressure.append(slp['value'])

    for time in x.findAll('ob'):
        oclock = time['time'].split(':')
        if oclock[1].startswith('00'):
            if oclock[0].startswith(str(yest)):
                temp = time.findNext('variable')
                dew = temp.findNext('variable')
                rel = dew.findNext('variable')
                wd = rel.findNext('variable')
                cardinaldirection = wd.findNext('variable')
                ws = cardinaldirection.findNext('variable')
                visibility = ws.findNext('variable')
                null_weather = visibility.findNext('variable')
                null_clouds = null_weather.findNext('variable')
                slp = null_clouds.findNext('variable')
                alt = slp.findNext('variable')
                if alt['value'] == 'OK' or 'CLR':
                    alt['value'] = np.nan
                altimeter.append(alt['value'])

    for time in x.findAll('ob'):
        oclock = time['time'].split(':')
        if oclock[1].startswith('00'):
            if oclock[0].startswith(str(yest)):
                temp = time.findNext('variable')
                dew = temp.findNext('variable')
                rel = dew.findNext('variable')
                wd = rel.findNext('variable')
                cardinaldirection = wd.findNext('variable')
                ws = cardinaldirection.findNext('variable')
                visibility = ws.findNext('variable')
                null_weather = visibility.findNext('variable')
                null_clouds = null_weather.findNext('variable')
                slp = null_clouds.findNext('variable')
                alt = slp.findNext('variable')
                station_psr = alt.findNext('variable')
                if station_psr['value'] == 'OK' or 'CLR':
                    station_psr['value'] = np.nan
                station_pressure.append(station_psr['value'])
                
    tmp = map(int,temperature)
    win = map(int,wind_gust)
    wchilfactors = list(zip(tmp,win))
    for w in wchilfactors:
        wind_chiller(w[0], w[1])
                
for key, value in stations.items():
    
    now = datetime.datetime.now()
    yest = now.day - 1
    if yest < 10:
        yest = "{:02d}".format(yest)


    starttime = []
    temperature = []
    dew_point = []
    relative_humidity = []
    wind_direction = []
    wind_chill = []
    wind_gust = []
    slpressure = []
    altimeter = []
    station_pressure = []
    
    reqs = requests.get(value)
    soup = BeautifulSoup(reqs.content, "xml")
    weather_parser(soup)
    timehour = list(zip(starttime, temperature, dew_point, relative_humidity,
                    wind_direction, wind_chill, wind_gust, slpressure,
                    altimeter, station_pressure))

    df = pd.DataFrame(timehour, columns= ['date', 'temperature', 'dew_point',
                                          'relative_humidity', 'wind_direction',
                                          'wind_chill', 'wind_gust', 'slpressure',
                                           'altimeter', 'station_pressure'])

    df[['temperature', 'dew_point',
        'relative_humidity','wind_direction',
        'wind_chill','wind_gust', 'slpressure',
        'altimeter', 'station_pressure']] = df[['temperature', 'dew_point',
                                                'relative_humidity', 'wind_direction',
                                                'wind_chill', 'wind_gust', 'slpressure',
                                                'altimeter', 'station_pressure']].apply(pd.to_numeric)

    df['hour'] = df['date'].str.extract('(.. ... ..)', expand=True)
    now = datetime.datetime.now()
    df.date = str(now.year) + ' ' + df.date
    df['date'] = pd.to_datetime(df['date'],format='%Y %d %b %I:%M %p')
    df['hour']= pd.to_datetime(df['date'],format='%H:00:00')
    df.hour = pd.to_datetime(df.hour, format = '%H')
    df.hour = df.hour.dt.hour
    df['start_time']= df['date']
    df['end_time'] = df['start_time'] + pd.Timedelta(hours=1)
    df['date'] = df.date.dt.date
    df.date = df.date.astype(str)
    df.date = df.date.str.replace('-','')
    df['location_id'] = key
    df['imported'] = datetime.datetime.now()
    df.to_sql('weather_actual',remote,if_exists='append', index=False)
