from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests
import pandahouse

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
    'start_date': '2024-11-14',
}

schedule_interval = '0 23 * * *'
connection_test = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'test',
                      'user':'student-rw', 
                      'password':'656e2b0c9c'
                     }


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_n_shulmina_1():
     
    @task()
    def exctract_table_1():
        q1 = """
        SELECT toDate(time) AS date,
            user_id, 
            countIf(action = 'view') AS views, 
            countIf(action = 'like') AS likes,
            If(gender == 0, 'female', 'male') as gender,
            multiIf(age <= 17, 'до 18', age > 17
                                        and age <= 30, '18-30', age > 30
                                        and age <= 50, '31-50', '50+') as age,
            os
        FROM simulator_20241020.feed_actions
        WHERE date = today() - 1
        GROUP BY user_id, date, gender, age, os
        """
        df1 = pandahouse.read_clickhouse(q1, connection=connection)
        return df1
        
    @task()
    def exctract_table_2():
        q2 = """
        SELECT date,
                user_id,
                sent_messages,
                sent_users,
                resevied_messages,
                received_users
        FROM
        (SELECT toDate(time) AS date,
                user_id,
                count() AS sent_messages,
                count(DISTINCT receiver_id) AS sent_users
        FROM simulator_20241020.message_actions
        WHERE date = today() - 1
        GROUP BY date, user_id) t1

        LEFT JOIN

        (SELECT toDate(time) AS date,
                receiver_id,
                count() AS resevied_messages,
                uniqExact(user_id) AS received_users
        FROM simulator_20241020.message_actions
        WHERE date = today() - 1
        GROUP BY date, receiver_id) t2
        ON t1.user_id = t2.receiver_id
        """
        df2 = pandahouse.read_clickhouse(q2, connection=connection)
        return df2
    
    @task()
    def merge_df(df1, df2):
        df = pd.merge(df1, df2, on = ['user_id', 'date'], how = 'outer')
        return df
        
    @task()
    def gender(df):
        gender_df = df.groupby('gender', as_index = False) \
                    .agg({'date':'min', 'views':'sum', 'likes':'sum', 'sent_messages':'sum', \
                              'sent_users':'sum', 'resevied_messages':'sum', 'received_users':'sum'})\
                    .reset_index().copy()
        gender_df['metric'] = 'gender'
        gender_df.rename(columns={'gender':'value_of_metric'}, inplace = True)
        return gender_df
        
    @task()
    def os(df):
        os_df = df.groupby('os', as_index=False) \
                .agg({'date':'min', 'views':'sum', 'likes':'sum', 'sent_messages':'sum', \
                              'sent_users':'sum', 'resevied_messages':'sum', 'received_users':'sum'})\
                    .reset_index().copy()
        os_df['metric'] = 'os'
        os_df.rename(columns = {'os':'value_of_metric'}, inplace = True)
        return os_df
        
    @task()
    def age(df):
        age_df = df.groupby('age', as_index=False) \
                .agg({'date':'min', 'views':'sum', 'likes':'sum', 'sent_messages':'sum', \
                              'sent_users':'sum', 'resevied_messages':'sum', 'received_users':'sum'})\
                    .reset_index().copy()
        age_df['metric'] = 'age'
        age_df.rename(columns = {'age':'value_of_metric'}, inplace = True)
        return age_df
        
    @task()
    def concat_table(gender_df, os_df, age_df):
        concat_df = pd.concat([gender_df, os_df, age_df])
        full_df = concat_df.rename(columns={'date':'event_date', 'metric':'dimension', 'value_of_metric':'dimension_value', 'resevied_messages':'messages_received', 'sent_messages':'messages_sent', \
                                    'received_users':'users_received', 'sent_users':'users_sent'})
        full_df = full_df.reset_index().drop('index', axis=1)
        full_df = full_df[['event_date', 'dimension', 'dimension_value', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]
        full_df = full_df.astype({
            'dimension': 'str', \
            'dimension_value': 'str', \
            'views': 'int', \
            'likes': 'int', \
            'messages_received': 'int', \
            'messages_sent': 'int', \
            'users_received': 'int', \
            'users_sent': 'int'})
        return full_df
        
    @task()
    def load_df(full_df):
        q3 = """
            CREATE TABLE IF NOT EXISTS test.nshulmina_1
            (   event_date Date,
                dimension String,
                dimension_value String,
                views UInt64,
                likes UInt64,
                messages_received UInt64,
                messages_sent UInt64,
                users_received UInt64,
                users_sent UInt64
                ) ENGINE = Log()"""
        pandahouse.execute(connection=connection_test, query=q3)
        pandahouse.to_clickhouse(df=full_df, table='nshulmina_1', index=False, connection=connection_test)
    
    
    df1 = exctract_table_1()
    df2 = exctract_table_2()
    df = merge_df(df1, df2)
    gender_df = gender(df)
    age_df = age(df)
    os_df = os(df)
    full_df = concat_table(gender_df, os_df, age_df)
    load_df(full_df)


dag_test_nshulmina = dag_n_shulmina_1()