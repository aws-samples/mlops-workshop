FROM 763104351884.dkr.ecr.us-west-2.amazonaws.com/tensorflow-training:2.7.0-cpu-py38-ubuntu20.04-e3-v1.0
COPY requirements.txt requirements.txt

RUN apt-get update && apt-get install -y --no-install-recommends \
    nginx \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip

RUN pip install --no-cache-dir -U \
    flask \
    gevent \
    gunicorn

RUN pip install -r requirements.txt

RUN mkdir -p /opt/program
RUN mkdir -p /opt/ml
RUN mkdir -p /opt/ml/code

ENV SAGEMAKER_SUBMIT_DIRECTORY /opt/ml/code

COPY app.py /opt/ml/code
COPY evaluation.py /opt/ml/code
COPY model.py /opt/ml/code
COPY wsgi.py /opt/ml/code
COPY nginx.conf /opt/program
WORKDIR /opt/ml/code


EXPOSE 8080

ENTRYPOINT ["python", "app.py"]
