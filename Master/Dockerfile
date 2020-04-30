FROM python:3.7-slim-stretch

RUN pip install ipython pika kazoo flask pymongo requests uuid docker pandas Flask-PyMongo==2.3.0

RUN apt update

RUN apt install -y mongodb

ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1
ENV TEAM_NAME=CC_0195_0309



WORKDIR /app

COPY . /app

EXPOSE 5001


CMD ["service","mongodb","start"]