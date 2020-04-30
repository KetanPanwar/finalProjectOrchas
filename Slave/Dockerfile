FROM mvertes/alpine-mongo

RUN apk update


RUN apk install python3
RUN apk add py-pip

RUN pip install pika
RUN pip install ipython pika kazoo flask pymongo requests uuid docker pandas Flask-PyMongo==2.3.0





ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1
ENV TEAM_NAME=CC_0195_0309



WORKDIR /app

COPY . /app

EXPOSE 5002


