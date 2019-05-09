FROM ubuntu:16.04

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

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
    && apt-get update -y \
    && apt-get install -y --no-install-recommends \
        $buildDeps \
        python3 \
        python3-pip \
        netbase \
        apt-utils \
        curl \
        netcat \
        locales \
        git \
        binutils \
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && useradd -ms /bin/bash worker \ 
    && python3 -m pip install -U pip \
    && pip3 install -U setuptools \
    && pip3 install Cython \
    && pip3 install pytz==2015.7 \
    && pip3 install ndg-httpsclient \
    && pip3 install pyasn1

# Cache bust
ENV updated-adds-on 5-9-2019
RUN set -ex \
    && pip3 install git+https://github.com/CityOfPhiladelphia/extract-knack#egg=extract_knack

USER worker
#CMD ["/bin/bash"] 