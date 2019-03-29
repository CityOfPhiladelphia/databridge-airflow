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
        libpq-dev \
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
        wget \
        alien \
        libgdal-dev \
        libgeos-dev \
        binutils \
        libproj-dev \
        gdal-bin \
        libspatialindex-dev \
        libaio1 \
        freetds-dev \
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow \
    && python3 -m pip install -U pip \
    && pip3 install -U setuptools \
    && pip3 install Cython \
    && pip3 install pytz==2015.7 \
    && pip3 install pyOpenSSL \
    && pip3 install ndg-httpsclient \
    && pip3 install pyasn1 \
    && pip3 install click \
    && apt-get remove --purge -yqq $buildDeps \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

COPY bin/entrypoint.sh /entrypoint.sh
COPY requirements.txt /requirements.txt

RUN pip3 install -r requirements.txt

COPY airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
COPY dags ${AIRFLOW_HOME}/dags
COPY plugins ${AIRFLOW_HOME}/plugins
COPY schemas ${AIRFLOW_HOME}/schemas
COPY utils ${AIRFLOW_INSTALL}/utils

RUN chown -R airflow: ${AIRFLOW_HOME} \
    && chmod +x /entrypoint.sh

EXPOSE 8080

USER airflow
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["/entrypoint.sh"]
