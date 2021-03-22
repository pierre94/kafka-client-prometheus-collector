#!/usr/bin/env bash

mvn clean package -DskipTests

mvn install:install-file -DgroupId=cn.bear2.kafka -DartifactId=kafka-client-prometheus-collector -Dversion=1.0.1 -Dpackaging=jar -Dfile=./target/kafka-client-prometheus-collector-1.0.1.jar
