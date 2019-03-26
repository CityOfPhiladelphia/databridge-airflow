FROM python:3.6.1

# Airflow
ARG AIRFLOW_VERSION=1.10.2
# GPL dependency
ENV SLUGIFY_USES_TEXT_UNIDECODE yes

COPY . /home/ubuntu/databridge-airflow/
WORKDIR /home/ubuntu/databridge-airflow/
RUN pip install apache-airflow[celery,postgres,ssh,rabbitmq]==${AIRFLOW_VERSION} \
    -r requirements.txt

RUN chown -R airflow: ${WORKDIR}
USER airflow