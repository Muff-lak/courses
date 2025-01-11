from datetime import datetime, timedelta
import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

connection = {'host': 'https://clickhouse.lab.karpov.courses',
'database':'simulator_20241020',
'user':'student',
'password':'dpo_python_2020'
}

default_args = {
    'owner': 'n.shulmina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': '2024-12-01',
}

schedule_interval = '0 11 * * *'
connection_test = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'test',
                      'user':'student-rw', 
                      'password':'656e2b0c9c'
                     }



def report_text(chat=None):
    chat_id = chat or -938659451
    my_token = '8150897728:AAFcOLx5PU48baHFT1tGHdHlzXiMFIK0Mjk'
    bot = telegram.Bot(token=my_token)
    q1 = """
    SELECT toDate(time) as day, uniq(user_id) AS DAU, countIf(action = 'view') AS views, countIf(action='like') AS likes, likes/views AS CTR
    FROM {db}.feed_actions
    WHERE day ==  yesterday()
    GROUP BY day
    """
    df1 =  ph.read_clickhouse(q1, connection=connection)
    report = "Отчет за " + df1['day'].astype(str)[0] + ':' + '\n' + 'DAU - ' + df1['DAU'].astype(str)[0] + '\n' + 'Просмотры - ' + df1['views'].astype(str)[0] + '\n' + 'Лайки - ' + df1['likes'].astype(str)[0] \
    + '\n' + 'CTR - ' + df1['CTR'][0].round(2).astype(str)
    bot.sendMessage(chat_id=chat_id, text=report)
    
def report_plot(chat=None):
    chat_id = chat or -938659451
    my_token = '8150897728:AAFcOLx5PU48baHFT1tGHdHlzXiMFIK0Mjk'
    bot = telegram.Bot(token=my_token)
    q2 = """
    SELECT toDate(time) as day, uniq(user_id) AS DAU, countIf(action = 'view') AS views, countIf(action='like') AS likes, likes/views AS CTR
    FROM {db}.feed_actions
    WHERE day <= yesterday() AND day >= yesterday() - 6
    GROUP BY day
    """
    df2 =  ph.read_clickhouse(q2, connection=connection)
    plt.figure(figsize=[15, 10])

    plt.suptitle('Динамика за неделю', fontsize=19)

    plt.subplot(2, 2, 1)
    plt.plot(df2.day, df2.DAU, 'r', linewidth=3)
    plt.title('DAU')

    plt.subplot(2, 2, 2)
    plt.plot(df2.day, df2.views, 'b', linewidth=3)
    plt.title('Просмотры')

    plt.subplot(2, 2, 3)
    plt.plot(df2.day, df2.likes, 'g', linewidth=3)
    plt.title('Лайки')

    plt.subplot(2, 2, 4)
    plt.plot(df2.day, df2.CTR, 'y', linewidth=3)
    plt.title('CTR')

    plot = io.BytesIO()
    plt.savefig(plot)
    plot.seek(0)
    plot.name = 'plot.png'
    plt.close()

    bot.sendPhoto(chat_id=chat_id, photo=plot)
    
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def report_shulmina_2():
    
    @task()
    def send_text():
        report_text()
        
    @task()
    def send_plot():
        report_plot()
        
    send_text()    
    send_plot()

report_shulmina_2 = report_shulmina_2()