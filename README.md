# Jena-Elasticsearch: An Elasticsearch-Backed Triple Store

An Elasticsearch-backed triple store implementation using the Apache Jena RDF API.

# Design
This project aims to determine if Elasticsearch is viable for use as a triple store for realistic use cases. Elasticsearch is a scalable document database that implements an inverted index for fast exact matches. An RDF triple store backed by Elasticsearch was implemented using the Apache Jena RDF API. By using Elasticsearch, we aim to create a scalable triple store capable of fast exact query matches.
Each Jena Graph is its own Elasticsearch index. Graph names are sanitized using reversible Base32 encoding to avoid invalid characters.
Each triple is stored as its own document, which has three keyword fields: subject, predicate, and object. Blank nodes are stored in Elasticsearch with the prefix `"_:"`, and literal nodes are stored with the prefix `"L:"`.
Elasticsearch-backed graphs can be created using the `ElasticsearchGraphMaker` factory, which is configured using an `ElasticsearchGraphMakerConfiguration`. The factory is configured with a set of Elasticsearch nodes (HttpHost instances) and a synchronization type (`ASYNCHRONOUS` or `SYNCHRONOUS`). An asynchronous graph does not guarantee that changes are readable when the update calls return, whereas a synchronous graph does.

# Prerequisites
This project requires Elasticsearch `7.4.1`.

# Build
To build jena-elasticsearch, run `mvn install` in the root directory.

# [Berlin SPARQL Benchmark](http://wifo5-03.informatik.uni-mannheim.de/bizer/berlinsparqlbenchmark/)

The Berlin SPARQL Benchmark (BSBM) was used to evaluate the triple store performance. Because BSBM runs against a SPARQL endpoint, an Apache Fuseki server using an Elasticsearch-backed graph was implemented. The Fuseki server can also be run with a TDB2 datastore (either persistent or in-memory), used to compare benchmarks against the Elasticsearch datastore.
To build the Fuseki server, run `mvn build` in the `bench` directory.

Ensure that Elasticsearch is running on port 9200 (default). The Fuseki server can be started by running
```java -jar target/[jar_filename.jar] [triplestore] [dataset]```
where [jar_filename.txt] is the Java archive from Maven, [triplestore] is either `elasticsearch-persistent`, `tdb2-memory`, or `tdb2-persistent`, and `dataset` is the filename of the BSBM dataset.

1. (One time) Generate data: run `./generate` in the `bsbmtools-0.2` directory
1. Start Elasticsearch
1. Start the jena-elasticsearch Fuseki server: `java -jar target/sparql-server-xxx.jar [dataset.nt]`
1. Run the benchmark: run `rn.sh` in the current directory
