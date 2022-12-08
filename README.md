# exchangerate_airflow

Таблицы в clickhouse создаются при сборке docker-compose. Скрипт лежит в папке ch-db-init-scripts. Dag-и лежат в папке dags. Папка примонтирована к контейнеру и Airflow их распознает. 
В clickhouse создается БД exchangerate_db с таблицами exchangerate_landing и exchangerate_parsed. В exchangerate_landing скалдываются ответы сервиса как есть (json-ы), в exchangerate_parsed - курсы, полученные из этих json-ов. 
В Airflow создано 2 dag-а: exchangerate_get_data и exchangerate_load_history_data. exchangerate_get_data выполняется каждые 3 часа и добавляет в обе таблицы БД по одной свежей записи для актуального курса. exchangerate_load_history_data запускается вручную и загружает в таблицу exchangerate_parsed исторические данные за последние 365 дней. 

Логин/пароль в Airflow - airflow/airflow
