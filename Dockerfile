FROM ubuntu:artful

RUN apt-get update \
 && apt-get install -y sqlite3 python3-pip

RUN pip3 install flask-restful \
                loompy \
                boto3 \
                hca \
		db-sqlite3

COPY app.py app.py
COPY schema.sql schema.sql

RUN sqlite3 app.db < schema.sql

CMD ["python3", "app.py"]
