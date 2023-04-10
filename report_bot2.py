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
def dag_gribanov_whole_bot():
    @task
    def extract_query_fandm():
        # Пользователи, которые пользуются и лентой новостей, и сервисом сообщений
        query = """
        select toDate(time) as day, count(distinct user_id) as dau from simulator_20230220.feed_actions
        where user_id in (select user_id from simulator_20230220.message_actions)
        and day >= today() - 7 and day != today()
        group by day
        order by day
        """
        df_fandm = ph.read_clickhouse(query, connection=connection)
        print('extract_query_fandm')
        return df_fandm
    @task
    def extract_query_fnotm():
        # Пользователи, использующие только ленту новостей и не пользуются сообщениями
        query = """
        select toDate(time) as day, count(distinct user_id) as dau from simulator_20230220.feed_actions
        where user_id not in (select user_id from simulator_20230220.message_actions)
        and day >= today() - 7 and day != today()
        group by day
        order by day
        """
        df_fnotm = ph.read_clickhouse(query, connection=connection)
        print('extract_query_fnotm')
        return df_fnotm
    @task
    def extract_query_user_ratio():
        # Отношение пользователей ленты и мессенджера
        query = """
        select day, round(feed_users / message_users,3) as ratio from (
        select day, feed_users, message_users from (
        select toDate(time) as day, count(distinct user_id) as feed_users 
        from simulator_20230220.feed_actions
        group by day) s0
        left join (
        select toDate(time) as day, count(distinct user_id) as message_users from simulator_20230220.message_actions
        group by day) s1
        using(day)
        )
        where day >= today() - 7 and day != today()
        order by day
        """
        df_user_ratio = ph.read_clickhouse(query, connection=connection)
        print('extract_query_user_ratio')
        return df_user_ratio

    @task
    def extract_query_action_ratio():
        # Отношение событий ленты и мессенджера
        query = """
        select day, round(feed_users / message_users,3) as ratio from (
        select day, feed_users, message_users from (
        select toDate(time) as day, count(user_id) as feed_users 
        from simulator_20230220.feed_actions
        group by day) s0
        left join (
        select toDate(time) as day, count(user_id) as message_users from simulator_20230220.message_actions
        group by day) s1
        using(day)
        )
        where day >= today() - 7 and day != today()
        order by day
        """
        df_action_ratio = ph.read_clickhouse(query, connection=connection)
        print('extract_query_action_ratio')
        return df_action_ratio
    @task
    def extract_action_per_user_f():
        # Количество событий на пользователя ленты
        query = """
        select toDate(time) as day, round(count(user_id) / count(distinct user_id), 3) as ratio 
        from simulator_20230220.feed_actions
        where day >= today() - 7 and day != today()
        group by day
        order by day
        """
        df_action_per_user_f = ph.read_clickhouse(query, connection=connection)
        print('extract_action_per_user_f')
        return df_action_per_user_f
    @task
    def extract_action_per_user_m():
        # Количество событий на пользователя мессенджера
        query = """
        select toDate(time) as day, round(count(user_id) / count(distinct user_id), 3) as ratio 
        from simulator_20230220.message_actions
        where day >= today() - 7 and day != today()
        group by day
        order by day
        """
        df_action_per_user_m = ph.read_clickhouse(query, connection=connection)
        print('extract_action_per_user_m')
        return df_action_per_user_m

    @task
    def write_msg(df_fandm, df_fnotm, df_user_ratio, df_action_ratio, df_action_per_user_f, df_action_per_user_m):
        msg = f"""
📝 Отчет по приложению за <b>{df_fandm.day[len(df_fandm.day)-1].strftime('%Y-%m-%d')}</b>

🧑🏼‍💻Пользователи, которые пользуются и лентой, и сообщениями
<b>{df_fandm.dau[len(df_fandm.dau)-1]}</b>
🧑🏼‍💻Пользователи, использующие только ленту новостей 
<b>{df_fnotm.dau[len(df_fnotm.dau)-1]}</b>

➗Отношение пользователей ленты и мессенджера
<b>{df_user_ratio.ratio[len(df_user_ratio.ratio)-1]}</b>
➗Отношение событий ленты и мессенджера - 
<b>{df_action_ratio.ratio[len(df_action_ratio.ratio)-1]}</b>

📈Количество событий на пользователя ленты
<b>{df_action_per_user_f.ratio[len(df_action_per_user_f.ratio)-1]}</b>
📈Количество событий на пользователя мессенджера
<b>{df_action_per_user_m.ratio[len(df_action_per_user_m.ratio)-1]}</b>

📊 На фото - график со значениями метрик за предыдущие 7 дней
        """
        return msg

    @task
    def draw_graph(df_fandm, df_fnotm, df_user_ratio, df_action_ratio, df_action_per_user_f, df_action_per_user_m):
        fig = plt.figure(figsize = [18, 18])
        gs = gridspec.GridSpec(3, 2)
        ax1 = plt.subplot(gs[0, 0])
        ax1.set_title('Пользователи, которые пользуются и лентой, и сообщениями', fontsize=15)
        ax1.set_ylim(0,20000)
        ax1.set_ylabel('DAU')
        ax1.set_xlabel('Day')
        ax2 = plt.subplot(gs[0, 1])
        ax2.set_title('Пользователи, использующие только ленту новостей', fontsize=15)
        ax2.set_ylim(0,20000)
        ax2.set_ylabel('Ratio')
        ax2.set_xlabel('Day')
        bx1 = plt.subplot(gs[1, 0])
        bx1.set_title('Отношение пользователей ленты и мессенджера', fontsize=15)
        bx1.set_ylim(0,100)
        bx1.set_ylabel('Ratio')
        bx1.set_xlabel('Day')
        bx2 = plt.subplot(gs[1, 1])
        bx2.set_title('Отношение событий ленты и мессенджера', fontsize=15)
        bx2.set_ylim(0,100)
        bx2.set_ylabel('Ratio')
        bx2.set_xlabel('Day')
        cx1 = plt.subplot(gs[2, 0])
        cx1.set_title('Количество событий на пользователя ленты', fontsize=15)
        cx1.set_ylim(0,100)
        cx1.set_ylabel('Actions')
        cx1.set_xlabel('Day')
        cx2 = plt.subplot(gs[2, 1])
        cx2.set_title('Количество событий на пользователя мессенджера', fontsize=15)
        cx2.set_ylim(0,100)
        cx2.set_ylabel('Actions')
        cx2.set_xlabel('Day')
        ax1.plot(df_fandm.day, df_fandm.dau, linewidth = 2, color = 'blue')
        ax2.plot(df_fnotm.day, df_fnotm.dau, linewidth = 2, color = 'red')
        bx1.plot(df_user_ratio.day, df_user_ratio.ratio, linewidth = 2, color = 'yellow')
        bx2.plot(df_action_ratio.day, df_action_ratio.ratio, linewidth = 2, color = 'green')
        cx1.plot(df_action_per_user_f.day, df_action_per_user_f.ratio, linewidth = 2, color = 'blue')
        cx2.plot(df_action_per_user_m.day, df_action_per_user_m.ratio, linewidth = 2, color = 'red')
        plot_object = io.BytesIO()
        fig.savefig(plot_object)
        plot_object.seek(0)
        plt.close()
        return plot_object
        
    @task
    def send_photo(chat_id, plot_object, msg):
        bot.send_photo(chat_id = chat_id, photo = plot_object, caption=msg, parse_mode="HTML", disable_notification=True)
        print('send_photo')
        return

    df_fandm = extract_query_fandm()
    df_fnotm = extract_query_fnotm()
    df_user_ratio = extract_query_user_ratio()
    df_action_ratio = extract_query_action_ratio()
    df_action_per_user_f = extract_action_per_user_f()
    df_action_per_user_m = extract_action_per_user_m()
    msg = write_msg(df_fandm, df_fnotm, df_user_ratio, df_action_ratio, df_action_per_user_f, df_action_per_user_m)
    plot_object = draw_graph(df_fandm, df_fnotm, df_user_ratio, df_action_ratio, df_action_per_user_f, df_action_per_user_m)
    send_photo(chat_id, plot_object, msg)
    
dag_gribanov_whole_bot = dag_gribanov_whole_bot()