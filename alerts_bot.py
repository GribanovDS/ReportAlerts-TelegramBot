import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import pandahouse as ph
import telegram
from datetime import date
import os
import io
import sys
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

my_token = '6236079184:AAH3K2RQS2VBRmqYGnBrW8AO5UTrB7OE104'
bot = telegram.Bot(token=my_token)

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20230220'
}

default_args = {
    'owner': 'dm-gribanov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes =  5),
    'start_date': datetime(2023, 3, 24)
}

# Интервал запуска DAG
schedule_interval = '*/15 * * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_gribanov_alerts():
    
    @task
    def extract_query():
        query = """
        select ts, date, hm, dau, likes, views, ctr, sent from (
        select toStartOfFifteenMinutes(time) as ts 
        , toDate(time) as date
        , formatDateTime(ts, '%R') as hm
        , count(distinct user_id) as dau
        , countIf(action='like') as likes
        , countIf(action='view') as views
        , likes / views as ctr
        from simulator_20230320.feed_actions
        group by date, ts, hm
        ) s1
        left join (
        select toStartOfFifteenMinutes(time) as ts 
        , toDate(time) as date
        , formatDateTime(ts, '%R') as hm
        , count(reciever_id) as sent
        from simulator_20230320.message_actions
        group by date, ts, hm
        ) s2
        using(date, ts, hm)
        where date >= toDate(today()) - 1 and ts != toStartOfFifteenMinutes(now())
        order by ts
        """
        data = ph.read_clickhouse(query, connection=connection)
        return data

    

    @task
    def run_alerts(data, chat_id=None):
        def check_anomaly(df, metric, a=5, n=5):
            df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
            df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
            df['iqr'] = df['q75'] - df['q25']
            df['up'] = df['q75'] + a*df['iqr']
            df['down'] = df['q25'] - a*df['iqr']

            df['up'] = df['up'].rolling(n, center = True, min_periods = 1).mean()
            df['down'] = df['down'].rolling(n, center = True, min_periods = 1).mean()

            if df[metric].iloc[-1] > df['up'].iloc[-1] or df[metric].iloc[-1] < df['down'].iloc[-1]:
                is_alert = 1
            else:
                is_alert = 0

            return is_alert, df 
        
        metric_list = ['dau','likes', 'views', 'ctr', 'sent']
        metric_namelist = ['DAU', 'Лайки', 'Просмотры', 'CTR', 'Отправленные сообщения']
        for i, metric in enumerate(metric_list):
            df = data[['ts','date','hm',metric]].copy()
            is_alert, df = check_anomaly(df, metric)
            metric_name = metric_namelist[i]

            if is_alert == 1:
                current_val = round(df[metric].iloc[-1],3)
                temp = round(abs((float(df[metric].iloc[-1]) - float(df[metric].iloc[-2]))*100) / float(df[metric].iloc[-2]),2)
                if df[metric].iloc[-1] > df[metric].iloc[-2]:
                    last_val_diff = f"+{temp}%"
                else: 
                    last_val_diff = f"-{temp}%"
                msg = f"""
🔔<b>{metric_name}:</b>
Текущее значение: <b>{current_val}</b>
Отклонение от предыдущего значения: <b>{last_val_diff}</b>

Ссылка на дашборд для детального обзора:
https://superset.lab.karpov.courses/superset/dashboard/3156/
                """
                #fig = plt.figure
                sns.set(rc={'figure.figsize': (16, 10)})
                ax = sns.lineplot(x=df['ts'], y = df[metric], label='metric')
                ax = sns.lineplot(x=df['ts'], y = df['up'], label='up')
                ax = sns.lineplot(x=df['ts'], y = df['down'], label='down')

                for ind, label in enumerate(ax.get_xticklabels()):
                    if ind % 2 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)
                ax.set(xlabel='time')
                ax.set(ylabel = metric)

                plot_object = io.BytesIO()
                plt.savefig(plot_object)
                plot_object.seek(0)
                plt.close()
                print('draw_graph')

                bot.send_photo(chat_id = chat_id, photo = plot_object, caption=msg, parse_mode="HTML", disable_notification=True)
        return
            
    data = extract_query()
    run_alerts(data, chat_id=-958942131)
            
dag_gribanov_alerts = dag_gribanov_alerts()
        

