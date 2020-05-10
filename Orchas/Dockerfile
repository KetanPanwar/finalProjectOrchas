FROM ubuntu:18.04

RUN apt-get update && \
    apt-get install -y python3 && \
    apt-get install -y python3-pip && \
    pip3 install pandas && \
    pip3 install Flask-PyMongo==2.3.0 && \
    pip3 install requests && \
    pip3 install pika && \
    pip3 install docker && \
    pip3 install Timer && \
    pip3 install kazoo &&

ENV TEAM_NAME=CC_0195_0309

WORKDIR /app

COPY . /app

EXPOSE 80

CMD ["python3","orchas.py"]
