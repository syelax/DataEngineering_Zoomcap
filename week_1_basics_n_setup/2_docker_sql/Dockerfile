FROM python:3.9

RUN apt-get install wget
RUN pip install pandas pyarrow sqlalchemy psycopg2

WORKDIR /app
COPY taxi+_zone_lookup.csv taxi_zone.csv
COPY ingest_data.py ingest_data.py

ENTRYPOINT ["python", "ingest_data.py"]