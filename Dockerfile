FROM docker.io/python:3.10

COPY . /work
WORKDIR /work

RUN apt-get update && apt-get install -y postgresql-client
RUN pip install psycopg2-binary clickhouse-connect

CMD ["/work/run_cdc.sh"]