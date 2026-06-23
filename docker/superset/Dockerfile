FROM apache/superset:latest

USER root

# 1. Cài đặt các thư viện hệ thống cần thiết để biên dịch mysqlclient
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    default-libmysqlclient-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# 2. Cài đặt các package python vào đúng môi trường ảo của Superset
RUN pip install --no-cache-dir --target /app/.venv/lib/python3.10/site-packages \
    "trino[sqlalchemy]==0.328.0" \
    "sqlalchemy-trino==0.5.0" \
    "psycopg2-binary==2.9.9" \
    "pymysql==1.1.0" \
    "mysqlclient==2.2.4" 

RUN mkdir -p /app/superset_home && \
    chown -R superset:superset /app/superset_home && \
    chmod -R 775 /app/superset_home

RUN mkdir -p /app/pythonpath && \
    echo "SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://db_user:db_password@postgres:5432/superset'" \
    > /app/pythonpath/superset_config.py

USER superset