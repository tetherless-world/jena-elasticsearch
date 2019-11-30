FROM circleci/openjdk:8-jdk-stretch

USER circleci
WORKDIR /home/circleci
COPY --chown=circleci /.m2/ .m2/
RUN find .m2 | wc

RUN mkdir /home/circleci/project
WORKDIR /home/circleci/project
COPY --chown=circleci /pom.xml ./
COPY --chown=circleci /src/ src/
RUN mvn package -DskipTests
RUN find /home/circleci/.m2 | wc
