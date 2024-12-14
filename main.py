import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import httpx
import json
import asyncio
import aiohttp
import datetime

from functions import rolling_mean, mean_std, is_anomaly, get_anomaly_range, fetch_weather


async def main():
    st.title("Анализ температуры воздуха в мегаполисах")

    st.header("Шаг 1: Загрузка данных")

    uploaded_file = st.file_uploader("Выберите CSV-файл", type=["csv"])

    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)
        st.write("Превью данных:")
        st.dataframe(df)
    else:
        st.write("Пожалуйста, загрузите CSV-файл.")

    if uploaded_file is not None:
        st.header("Шаг 2: Выбор города")

    if uploaded_file:
        city = st.selectbox("Выберите город для отображения", options=df['city'].unique())
        if city:
            df = df[df['city'] == city]
            st.dataframe(df)

    if uploaded_file is not None:
        st.header("Шаг 3: Рассчет статистик")

        if st.checkbox("Добавить скользящее среднее, среднее значение и стандартное отклонение температуры по сезону, флаги для аномалий"):
            window = st.slider("Выберите окно", min_value=5, max_value=100, value=30)
            df = rolling_mean(df, window)
            df = mean_std(df)
            df['is_anomaly'] = df.apply(is_anomaly, axis=1)
            st.dataframe(df)

            if st.checkbox("Построить график температур"):
                fig = px.scatter(df, 
                                 x="timestamp", 
                                 y="temperature", 
                                 color='is_anomaly', 
                                 category_orders={'is_anomaly': ['True', 'False']},  
                                 height=600, 
                                 width=1200,
                                 title=f"Динамика температуры воздуха в городе {city}"  
                                )

                df_filt = df[df[f'rolling_mean_{str(window)}'].isna()==0]
                line_fig = px.line(df_filt, x="timestamp", y=f'rolling_mean_{str(window)}', labels={"value": f"Скользящее среднее ({window})"} )

                fig.add_trace(line_fig.data[0])
                fig.update_traces(line_color='red')

                st.plotly_chart(fig)

            if st.checkbox("Вывести статистики по сезонам"):
                df_stats = df[['city', 'season', 'mean_temperature', 'std_temperature']].drop_duplicates().reset_index(drop=True)
                st.dataframe(df_stats)


            st.header("Шаг 4: Получение текущей температуры")

            API_KEY  = st.text_input("Введите ключ API")
            df_stats = df[['city', 'season', 'mean_temperature', 'std_temperature']].drop_duplicates().reset_index(drop=True)

            if st.button("Загрузить данные с API") and API_KEY:
                try:
                    async with aiohttp.ClientSession() as session:
                        temperature = await fetch_weather(session, city, API_KEY)
                except ValueError as e:
                    st.error({"cod": 401, "message": e.args[0]})
                else:

                    if temperature is not None:
                        min, max = get_anomaly_range(df_stats, city)
                        if min <= temperature <= max:
                            st.write(f"Температура в {city}: {round(temperature,2)} °C (соответствует сезону).")
                        else:
                            st.write(f"Температура в {city} не соответствует сезону.")

if __name__ == "__main__":
    asyncio.run(main())