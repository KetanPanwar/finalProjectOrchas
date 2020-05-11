# finalProjectOrchas
# install docker on your system
# build the orchastrator and worker images
$ sudo docker build -t orchas:latest ./Orchastrator
$ sudo docker build -t worker:latest ./Worker
# start a container with zookeeper image
$ sudo docker run -d --name=zoo -p 2181:2181 zookeeper
# start a container with rabbitmq image
$ sudo docker run -d --name=rmq -p 5672:5672 -p 15672:15672 rabbitmq:3.6-management-alpine
# run the orchastrator and it will automatically spawn the first slave and master
$ sudo docker run --name=orchast -p80:80 orchas:latest 
