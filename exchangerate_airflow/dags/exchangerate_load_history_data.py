from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from clickhouse_driver import Client
from requests import get as get_request
from datetime import datetime, date

default_args = {
    'owner': 'ns_lukyanov',
    'depends_on_past': False,
    'start_date': datetime(2022, 12, 7),
    'email': ['nikitalukyanov@ngs.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('exchangerate_load_history_data',
          default_args=default_args,
          schedule_interval="@once",
          catchup=False
          )

def load_history_exchange_rates():
    base = "BTC"
    symbols = "USD"

    end_date = date.today()
    start_date = end_date + timedelta(days=-365)

    url = 'https://api.exchangerate.host/timeseries?base={0}&start_date={1}&end_date={2}&symbols={3}'.format(base, start_date, end_date, symbols)
    response = get_request(url)
    data = response.json()["rates"]

    rate_list = []

    for dt, val in data.items():
        for cur, rate in val.items():
            rate_list.append((datetime.now(), datetime.strptime(dt, "%Y-%m-%d"), "BTC/" + cur, rate))

    client = Client('ch_server',
                    port=9000)

    client.execute(
        "INSERT INTO exchangerate_db.exchangerate_parsed (CreateDTime, RateDate, CurrencyPair, Rate) VALUES",
        rate_list
    )


load_history_exchange_rates_task = PythonOperator(
    task_id='load_history_exchange_rates_task',
    python_callable=load_history_exchange_rates,
    dag=dag)

load_history_exchange_rates_task
