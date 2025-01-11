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

my_token = '№№№№№№№№№№№'
chat_id = '-938659451'

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
    'start_date': '2024-12-12',
}

schedule_interval = '0 11 * * *'
connection_test = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'test',
                      'user':'student-rw', 
                      'password':'656e2b0c9c'
                     }

def metrica_report(os, metrica, mean, sd, chat=None):
    chat_id = chat or '-938659451'
    bot = telegram.Bot(token=my_token)
    pd.options.mode.copy_on_write = True
    q2 = """
    SELECT toDateTime(intDiv(toUInt32(toDateTime(time)), 1800)*1800) as date_time, count(user_id) as active_users, countIf(action = 'like') AS likes, countIf(action = 'view') AS views, os, likes/views as CTR
    FROM {db}.feed_actions
    WHERE date_time >= yesterday() - 6 AND date_time <= yesterday()
    GROUP BY date_time, os
    ORDER BY date_time
    """
    df2 = ph.read_clickhouse(q2, connection = connection)
    df_os = df2[df2.os == os]
    df_os = df_os[['date_time', str(metrica)]]
    limit1 = mean + 3 * sd
    limit2 = mean - 3 * sd
    df_os['status'] = df_os[str(metrica)].apply(lambda x: 'outlier' if x > limit1 or x < limit2 else 'ok')
    out_os = df_os[df_os.status == 'outlier'].reset_index().date_time
    ls_out_os = [str(out_os[i]) for i in range(len(out_os))]
    report = 'Отчет на текущий момент ' + '(' + str(os) + ')' + ': '+ '\n'+ str(metrica) + ': ' + str(list(df_os.tail(1)[str(metrica)])).replace('[', '').replace(']', '') \
            + ", Выбросы : " + str(len(ls_out_os)) + ", Дата/время выбросов: " + str(ls_out_os)
    bot.sendMessage(chat_id, text=report)
    df_os['limit_red1'] = mean + 3 * sd
    df_os['limit_red2'] = mean - 3 * sd
    df_os['limit_or1'] = mean + 1.96 * sd
    df_os['limit_or2'] = mean - 1.96 * sd
    
    plt.figure(figsize = [15, 10])
    plt.title(os + ' ' + metrica, fontsize=19)
    plt.plot(df_os['date_time'], df_os[metrica], 'b')
    plt.plot(df_os.date_time, df_os.limit_red1, 'r')
    plt.plot(df_os.date_time, df_os.limit_red2, 'r')
    plt.plot(df_os.date_time, df_os.limit_or1, 'y')
    plt.plot(df_os.date_time, df_os.limit_or2, 'y')
    
    plot_metrica = io.BytesIO()
    plt.savefig(plot_metrica)
    plot_metrica.seek(0)
    plot_metrica.name = 'metrica_plot_{os}.png'
    plt.close()
    bot.sendPhoto(chat_id, photo=plot_metrica)
    
def message_report(os, mean, sd, chat=None):
    chat_id = chat or '-938659451'
    bot = telegram.Bot(token=my_token)
    pd.options.mode.copy_on_write = True
    q3 = """
    SELECT toDateTime(intDiv(toUInt32(toDateTime(time)), 1800)*1800) as date_time, count(user_id) as messages, os
    FROM {db}.message_actions
    WHERE date_time >= yesterday() - 6 AND date_time <= yesterday()
    GROUP BY date_time, os
    ORDER BY date_time
    """
    df3 = ph.read_clickhouse(q3, connection = connection)
    df_os = df3[df3.os == os]
    df_os = df_os[['date_time', 'messages']]
    limit1 = mean + 3 * sd
    limit2 = mean - 3 * sd
    df_os['status'] = df_os['messages'].apply(lambda x: 'outlier' if x > limit1 or x < limit2 else 'ok')
    out_os = df_os[df_os.status == 'outlier'].reset_index().date_time
    ls_out_os = [str(out_os[i]) for i in range(len(out_os))]
    report = 'Отчет на текущий момент ' + '(' + str(os) + ')' + ': '+ '\n'+ 'messages' + ': ' + str(list(df_os.tail(1)['messages'])).replace('[', '').replace(']', '') \
            + ", Выбросы : " + str(len(ls_out_os)) + ", Дата/время выбросов: " + str(ls_out_os)
    bot.sendMessage(chat_id, text=report)
    df_os['limit_red1'] = mean + 3 * sd
    df_os['limit_red2'] = mean - 3 * sd
    df_os['limit_or1'] = mean + 1.96 * sd
    df_os['limit_or2'] = mean - 1.96 * sd
    
    plt.figure(figsize = [15, 10])
    plt.title(os + ' messages', fontsize=19)
    plt.plot(df_os['date_time'], df_os['messages'], 'b')
    plt.plot(df_os.date_time, df_os.limit_red1, 'r')
    plt.plot(df_os.date_time, df_os.limit_red2, 'r')
    plt.plot(df_os.date_time, df_os.limit_or1, 'y')
    plt.plot(df_os.date_time, df_os.limit_or2, 'y')
    
    plot_metrica = io.BytesIO()
    plt.savefig(plot_metrica)
    plot_metrica.seek(0)
    plot_metrica.name = 'metrica_plot_{os}.png'
    plt.close()
    bot.sendPhoto(chat_id, photo=plot_metrica)
    
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def alert_shulmina():
    
    @task()
    def metrica_report():
        metrica_report(os = 'Android', metrica = 'active_users', mean = 10486, sd = 3408.5)
        metrica_report(os = 'Android', metrica = 'likes', mean = 1826, sd = 580.0)
        metrica_report(os = 'Android', metrica = 'views', mean = 8660, sd = 2833)
        metrica_report(os = 'Android', metrica = 'CTR', mean = 0.21206, sd = 0.010207)
        metrica_report(os = 'iOS', metrica = 'active_users', mean = 5640, sd = 1856.3)
        metrica_report(os = 'iOS', metrica = 'likes', mean = 983, sd = 317.4)
        metrica_report(os = 'iOS', metrica = 'views', mean = 4656, sd = 1542)
        metrica_report(os = 'iOS', metrica = 'CTR', mean = 0.21227, sd = 0.011323)
        
    @task()
    def message_report():
        message_report(os = "Android", mean = 173, sd = 72.56)
        message_report(os = "iOS", mean = 95, sd = 41.19)
        
    metrica_report()
    message_report()

alert_shulmina = alert_shulmina()
