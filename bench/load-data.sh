#!/bin/bash

cd "$(dirname "$0")"

java -jar ../../twks/java/dist/twks-cli-current.jar post-nanopublications bsbmtools-0.2/dataset.nt
