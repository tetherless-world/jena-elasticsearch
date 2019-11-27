FROM circleci/openjdk:8-jdk-stretch

COPY /.m2 .

RUN mkdir /home/circleci/project
WORKDIR /home/circleci/project
COPY /pom.xml .
COPY /src .
RUN mvn package -DskipTests
