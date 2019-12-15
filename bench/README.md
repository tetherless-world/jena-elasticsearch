# [Berlin SPARQL Benchmark](http://wifo5-03.informatik.uni-mannheim.de/bizer/berlinsparqlbenchmark/)
  
1. (One time) Generate data: run `generate-data.sh` in the current directory
1. Start Elasticsearch
1. Start the jena-elasticsearch Fuseki server: `java -jar target/sparql-server-xxx.jar [dataset.nt]`
1. Run the benchmark: run `rn.sh` in the current directory
