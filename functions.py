import datetime

def is_anomaly(row):
    if ((row['temperature'] > row['mean_temperature'] + 2 * row['std_temperature']) or
       (row['temperature'] < row['mean_temperature'] - 2 * row['std_temperature'])):
        return 1
    else:
        return 0
    

def rolling_mean(df, window):
    df[f'rolling_mean_{str(window)}'] = df.groupby(['city'])['temperature'].rolling(window=window).mean().reset_index(level=0, drop=True)
    return df

def mean_std(df):
    mean = df.groupby(['city', 'season'])['temperature'].mean().reset_index(name='mean_temperature')
    std = df.groupby(['city', 'season'])['temperature'].std().reset_index(name='std_temperature')
    df = df.merge(mean, on=['city', 'season']).merge(std, on=['city', 'season'])
    return df

# Напишем функцию для определения границ аномалий
def get_anomaly_range(df_stats, city, day=None):
    if day is None:
        day = datetime.datetime.now()
    month = day.month

    if month in [12, 1, 2]:
        season = 'winter'
    elif month in [3, 4, 5]:
        season = 'spring'
    elif month in [6, 7, 8]:
        season = 'summer'
    else:
        season = 'autumn'

    filt = df_stats[(df_stats['city']==city) & (df_stats['season']==season)][['mean_temperature', 'std_temperature']]
    mean = filt.iloc[0]['mean_temperature']
    std = filt.iloc[0]['std_temperature']

    min = mean - 2 * std
    max = mean + 2 * std
    return min, max


async def fetch_weather(session, city, api_key):
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}"
    
    async with session.get(url) as response:
        if response.status == 200:
            result = await response.json()
            try:
                temp_kelvin = result["main"]["temp"]
                temp_celsius = temp_kelvin - 273.15
                return temp_celsius
            except KeyError:
                print(f"Не удалось получить температуру для {city}")
                return None
        elif response.status == 401:
            error_message = await response.json()
            raise ValueError(error_message["message"]) 
        else:
            print(f"Произошла ошибка при запросе погоды для {city}. Статус-код: {response.status}")
            return None