FROM ubuntu:18.04

RUN apt-get update -y \
 && apt-get install -y python3-pip wget \
 && rm -rf /var/lib/apt/lists/*

RUN pip3 install -U pip \
 && pip install loompy pandas s3fs scanpy scipy zarr boto3==1.7.69 botocore==1.10.84 requests==2.20.0

RUN sed -i -e "s/TkAgg/Agg/g" /usr/local/lib/python3.6/dist-packages/matplotlib/mpl-data/matplotlibrc

ADD code /

ENV AWS_DEFAULT_REGION='us-east-1'

RUN chmod +x /matrix_converter.py

CMD ["python3", "/matrix_converter.py"]
