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
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('exchangerate_get_data',
          default_args=default_args,
          schedule_interval = timedelta(hours=3),
          catchup=False)

def get_exchange_rate():
    base = "BTC"
    symbols = "USD"

    url = 'https://api.exchangerate.host/latest?base={0}&symbols={1}'.format(base, symbols)
    response = get_request(url)

    client = Client('ch_server', port=9000)

    client.execute(
        "INSERT INTO exchangerate_db.exchangerate_landing (CreateDTime, RateDate, response) VALUES",
        [(datetime.now(), date.today(), response.text)]
    )


def parse_exchange_rate():
    client = Client('ch_server', port=9000)

    max_date = client.execute("select max(CreateDTime) from exchangerate_db.exchangerate_parsed")
    max_date_str = max_date[0][0].strftime('%Y-%m-%d %H:%M:%S')

    client.execute("""INSERT INTO exchangerate_db.exchangerate_parsed (CreateDTime, RateDate, CurrencyPair, Rate)
                      select CreateDTime, RateDate, 'BTC/USD', JSONExtractString(response,'rates', 'USD') 
                      from exchangerate_db.exchangerate_landing where CreateDTime > '{0}'""".format(max_date_str))


get_exchange_rate_task = PythonOperator(
    task_id='get_exchange_rate_task',
    python_callable=get_exchange_rate,
    dag=dag)

parse_exchange_rate_task = PythonOperator(
    task_id='parse_exchange_rate_task',
    python_callable=parse_exchange_rate,
    dag=dag)


get_exchange_rate_task >> parse_exchange_rate_task