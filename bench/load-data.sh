#!/bin/bash

cd "$(dirname "$0")"

java -jar target/sparql-server-1.0.0.jar elasticsearch-persistent bsbmtools-0.2/dataset.nt
