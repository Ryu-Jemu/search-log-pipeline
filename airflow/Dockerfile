# Airflow 3.3.2 + 지표 DAG가 쓰는 클라이언트(호스트 requirements.txt와 같은 버전).
# Airflow 공식 이미지 안내대로 apache-airflow를 같은 버전으로 함께 지정해, 의존성 설치가 Airflow를 바꾸지 않게 한다.
FROM mirror.gcr.io/apache/airflow:3.3.2-python3.13
RUN pip install --no-cache-dir "apache-airflow==3.3.2" "clickhouse-connect==1.10.0" "confluent-kafka==2.16.0"
