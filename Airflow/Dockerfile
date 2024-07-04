FROM apache/airflow:2.3.3

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
           libaio1 \
           wget \
           unzip \
           vim \
           telnet \
           curl \
           iputils-ping \
           systemctl \
           default-jdk \
           ca-certificates \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir /usr/local/airflow \
    && mkdir /opt/oracle \
    && cd /opt/oracle \
    && wget https://download.oracle.com/otn_software/linux/instantclient/185000/instantclient-basiclite-linux.x64-18.5.0.0.0dbru.zip --no-check-certificate \
    && unzip instantclient-basiclite-linux.x64-18.5.0.0.0dbru.zip \
    && rm -f instantclient-basiclite-linux.x64-18.5.0.0.0dbru.zip \
    && cd /opt/oracle/instantclient* \
    && rm -f *jdbc* *occi* *mysql* *README *jar uidrvci genezi adrci \
    && echo /opt/oracle/instantclient* > /etc/ld.so.conf.d/oracle-instantclient.conf \
    && ldconfig

ARG AIRFLOW_USER_HOME=/usr/local/airflow
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}

RUN chown -R airflow: ${AIRFLOW_HOME}

USER airflow

RUN python -m pip install --upgrade pip \
     && pip install pyspark==3.2.1 \
     && pip install apache-airflow-providers-apache-spark \
     && pip install minio \
     && pip install requests \
     && pip install delta-spark==2.3.0

ENV FLASK_APP="airflow.www.app flask fab create-admin"

COPY ./entrypoint.sh /entrypoint.sh
COPY ./config/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg

EXPOSE 8080

USER root
RUN chmod 777 /entrypoint.sh

USER airflow
WORKDIR ${AIRFLOW_HOME}

ENTRYPOINT ["/entrypoint.sh"]
