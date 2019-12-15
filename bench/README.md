# [Berlin SPARQL Benchmark](http://wifo5-03.informatik.uni-mannheim.de/bizer/berlinsparqlbenchmark/)
  
1. (One time) Generate data: run `generate-data.sh` in the current directory
1. Start Elasticsearch
1. Build the Fuseki server: `mvn package`
1. Start the jena-elasticsearch Fuseki server: `load-data.sh [datastore]`
where [jar_filename.txt] is the Java archive from Maven, [triplestore] is either `elasticsearch-persistent`, `tdb2-memory`, or `tdb2-persistent`.
1. Run the benchmark: run `rn.sh` in the current directory
