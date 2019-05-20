FROM ubuntu:16.04

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# Airflow
ARG AIRFLOW_VERSION=1.10.2
ENV AIRFLOW_HOME /usr/local/airflow
ENV AIRFLOW_INSTALL /usr/local/lib/python3.5/dist-packages/airflow
ENV SLUGIFY_USES_TEXT_UNIDECODE yes

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8
ENV LC_ALL  en_US.UTF-8

RUN set -ex \
    && buildDeps=' \
        python3-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        build-essential \
        libblas-dev \
        liblapack-dev \
   ' \
    && apt-get update -yqq \
    && apt-get install -yqq --no-install-recommends \
        $buildDeps \
        python3 \
        python3-pip \
        netbase \
        apt-utils \
        curl \
        netcat \
        locales \
        git \
        alien \
        libgdal-dev \
        libgeos-dev \
        binutils \
        libproj-dev \
        gdal-bin \
        libspatialindex-dev \
        libaio1 \
        freetds-dev \
        libpq-dev \
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow \
    && python3 -m pip install -U pip \
    && pip3 install -U setuptools \
    && pip3 install Cython==0.29.7 \
                    pytz==2015.7 \
                    pyOpenSSL==19.0.0 \
                    ndg-httpsclient==0.5.1 \
                    pyasn1==0.4.5 \
                    click==7.0 \
                    apache-airflow[crypto,postgres,s3,celery,rabbitmq,mssql,slack]==1.10.2 \
                    awscli==1.16.161 \
                    boto3==1.7.84 \
                    cryptography==2.6.1 \
                    cx-Oracle==7.0.0 \
                    Flask-Bcrypt==0.7.1 \
                    pyodbc==4.0.26 \
                    PyYAML==5.1 \
                    pytest==4.5.0 \
    && apt-get remove --purge -yqq $buildDeps \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

# instant basic-lite instant oracle client
RUN set -ex \
    && aws s3api get-object \
           --bucket citygeo-oracle-instant-client \
           --key oracle-instantclient12.1-basiclite-12.1.0.2.0-1.x86_64.rpm \
                 oracle-instantclient12.1-basiclite-12.1.0.2.0-1.x86_64.rpm \
    && alien -i oracle-instantclient12.1-basiclite-12.1.0.2.0-1.x86_64.rpm \
    && rm oracle-instantclient12.1-basiclite-12.1.0.2.0-1.x86_64.rpm

# instant oracle-sdk
RUN set -ex \
    && aws s3api get-object \
           --bucket citygeo-oracle-instant-client \ 
           --key oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm \
                 oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm \
    && alien -i oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm \
    && rm oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm

COPY scripts/entrypoint.sh /entrypoint.sh
COPY scripts/secrets_manager.py /secrets_manager.py

COPY config/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
COPY dags ${AIRFLOW_HOME}/dags
COPY plugins ${AIRFLOW_HOME}/plugins

RUN chown -R airflow: ${AIRFLOW_HOME} \
    && chmod +x /entrypoint.sh \
    && chmod +x /secrets_manager.py

EXPOSE 8080

USER airflow
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["/entrypoint.sh"]
