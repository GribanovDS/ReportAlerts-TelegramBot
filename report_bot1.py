import telegram
import pandahouse as ph
import seaborn as sns
import matplotlib.pyplot as plt
import io
import os
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.gridspec as gridspec
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

chat_id = -802518328

# Дефолтные параметры, которые прокидываются в таски

default_args = {
    'owner': 'dm-gribanov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes =  5),
    'start_date': datetime(2023, 3, 24)
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_gribanov_bot():
    @task
    def extract_query():
        query = """
        select toDate(time) as day,
        count(distinct user_id) as DAU,
        countIf(action = 'view') as Views,
        countIf(action = 'like') as Likes,
        round(Likes / Views, 3) as CTR
        from simulator_20230220.feed_actions
        where day = today() - 1
        group by day
        order by day
        """
        df = ph.read_clickhouse(query, connection = connection)
        print('extract_query')
        return df
    
    @task
    def extract_extra_query():
        extra_query = """
        select toDate(time) as day,
        count(distinct user_id) as DAU,
        countIf(action = 'view') as Views,
        countIf(action = 'like') as Likes,
        round(Likes / Views, 3) as CTR
        from simulator_20230220.feed_actions
        where day = today() - 2
        group by day
        order by day
        """
        df_extra = ph.read_clickhouse(extra_query, connection = connection)
        print('extract_extra_query')
        return df_extra
    
    @task
    def write_msg(df, df_extra):
        day = df['day'][0]
        dau = df['DAU'][0]
        views = df['Views'][0]
        likes = df['Likes'][0]
        ctr = df['CTR'][0]
        dau_extra = df_extra['DAU'][0]
        views_extra = df_extra['Views'][0]
        likes_extra = df_extra['Likes'][0]
        ctr_extra = df_extra['CTR'][0]

        dau_norma = 'Показатель не изменился ✔'
        if abs((int(dau)-int(dau_extra))*100//int(dau) > 10):
            if (dau > dau_extra):
                dau_norma = f'Показатель вырос на {round((dau-dau_extra)*100/dau,2)}% ✅'
            else: 
                dau_norma = f'Показатель упал на {round((dau_extra-dau)*100/dau,2)}% ❌'
        views_norma = 'Показатель не изменился ✔'
        if abs((int(views)-int(views_extra))*100//int(views) > 10):
            if (views > views_extra):
                views_norma = f'Показатель вырос на {round((views-views_extra)*100/views,2)}% ✅'
            else: 
                views_norma = f'Показатель упал на {round((views_extra-views)*100/views,2)}% ❌'
        likes_norma = 'Показатель не изменился ✔'
        if abs((int(likes)-int(likes_extra))*100//int(likes) > 10):
            if (likes > likes_extra):
                likes_norma = f'Показатель вырос на {round((likes-likes_extra)*100/likes,2)}% ✅'
            else: 
                likes_norma = f'Показатель упал на {round((likes_extra-likes)*100/likes,2)}% ❌'
        ctr_norma = 'Показатель не изменился ✔'
        if abs((int(ctr*10000)-int(ctr_extra*10000))//int(ctr*100))> 10:
            if (ctr > ctr_extra):
                ctr_norma = f'Показатель вырос на {round((ctr-ctr_extra)*100/ctr,2)}% ✅'
            else: 
                ctr_norma = f'Показатель упал на {round((ctr_extra-ctr)*100/ctr,2)}% ❌'

        msg = f"""
📝 Отчет по ленте за <b>{day.strftime('%Y-%m-%d')}</b>:

👨🏻‍💻 DAU - <b>{dau}</b>, 
{dau_norma}
👍🏻 Лайки - <b>{likes}</b>, 
{likes_norma}
👀 Просмотры - <b>{views}</b>, 
{views_norma}
🖱️ CTR - <b>{ctr}</b>, 
{ctr_norma}

📊 На фото - график со значениями метрик за предыдущие 7 дней
        """
        print('write_msg')
        return msg

    @task
    def extract_query2():
        query = """
        select toDate(time) as day,
        count(distinct user_id) as DAU,
        countIf(action = 'view') as Views,
        countIf(action = 'like') as Likes,
        round(Likes / Views, 3) as CTR
        from simulator_20230220.feed_actions
        where day >= today() - 7 and day != today()
        group by day
        order by day
        """
        df = ph.read_clickhouse(query, connection=connection)
        print('extract_query2')
        return df

    @task
    def draw_graph(df):
        fig = plt.figure(figsize = [20, 15]) 
        gs = gridspec.GridSpec(2, 2)
        ax = plt.subplot(gs[0, 0])
        bx = plt.subplot(gs[0, 1])
        cx = plt.subplot(gs[1, 0])
        dx = plt.subplot(gs[1, 1])

        ax.plot(df['day'], df['Views'], linewidth=2, color = 'blue')
        ax.set_title('Views', fontsize=20)
        ax.set_ylim(0,900000)
        ax.set_ylabel('Views')
        ax.set_xlabel('Day')
        bx.plot(df['day'], df['Likes'], linewidth=2, color = 'red')
        bx.set_title('Likes', fontsize=20)
        bx.set_ylim(0,200000)
        bx.set_ylabel('Likes')
        bx.set_xlabel('Day')
        cx.plot(df['day'], df['DAU'], linewidth=2, color = 'y')
        cx.set_title('DAU', fontsize=20)
        cx.set_ylim(0,50000)
        cx.set_ylabel('Active Users')
        cx.set_xlabel('Day')
        dx.plot(df['day'], df['CTR'], linewidth=2, color = 'green')
        dx.set_title('CTR', fontsize=20)
        dx.set_ylim(0,1)
        dx.set_ylabel('CTR')
        dx.set_xlabel('Day')
        plot_object = io.BytesIO()
        fig.savefig(plot_object)
        plot_object.seek(0)
        plt.close()
        print('draw_graph')
        return plot_object
    
    @task
    def send_photo(chat_id, plot_object, msg):
        bot.send_photo(chat_id = chat_id, photo = plot_object, caption=msg, parse_mode="HTML", disable_notification=True)
        print('send_photo')
        return
    
    df = extract_query()
    df_extra = extract_extra_query()
    msg = write_msg(df, df_extra)
    df2 = extract_query2()
    plot_object = draw_graph(df2)
    send_photo(chat_id, plot_object, msg)
    
dag_gribanov_bot = dag_gribanov_bot()