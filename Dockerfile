# -*- coding: utf-8 -*-
#
# Copyright (C) 2023-2024 SBA Research.

FROM python:3.8-slim

RUN apt update && \
    apt install -y --no-install-recommends build-essential gcc supervisor nginx && \
    apt clean && \
    rm -rf /var/lib/apt/lists/*

COPY server_config/supervisord.conf /supervisord.conf
COPY server_config/nginx.conf /etc/nginx/sites-available/default
COPY server_config/docker-entrypoint.sh /entrypoint.sh
COPY . /app

WORKDIR /app

RUN pip3 install --user --upgrade pip && \
    pip3 install pipenv && \
    pipenv install --system --deploy

EXPOSE 9000 9001
ENTRYPOINT ["sh", "/entrypoint.sh"]
