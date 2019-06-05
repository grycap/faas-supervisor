FROM python:3-alpine

ENV SCAR_LOG_PATH='/var/log/'
ENV SUPERVISOR_TYPE='BATCH'

#RUN pip3 install faas-supervisor
RUN apk update && apk add git && git clone --branch lintify https://github.com/grycap/faas-supervisor
RUN pip3 install -U /faas-supervisor/

COPY batch_handler.py batch_handler.py

ENTRYPOINT python3 batch_handler.py
