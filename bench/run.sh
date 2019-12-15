#!/bin/bash

cd "$(dirname "$0")"

cd bsbmtools-0.2
java -cp bin:lib/* benchmark.testdriver.TestDriver http://localhost:3331/dataset/sparql
