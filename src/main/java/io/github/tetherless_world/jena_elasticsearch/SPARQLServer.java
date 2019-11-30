package io.github.tetherless_world.jena_elasticsearch;

import org.apache.http.HttpHost;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.graph.Graph;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SPARQLServer {
    private static final Logger logger = LoggerFactory.getLogger(SPARQLServer.class);
    private static FusekiServer server = null;

    public static void main(String args[]) {
        try {
            Graph g = new ElasticsearchGraphMaker(
                    new HttpHost("elasticsearch", 9200, "http")
            ).createGraph();
            Model m = ModelFactory.createModelForGraph(g);
            Dataset ds = DatasetFactory.wrap(m);

            server = FusekiServer.create()
                    .add("/dataset", ds)
                    .port(3331)
                    .build();

            server.start();

        } catch (IOException e) {
            logger.error("Could not create Elasticsearch Graph", e);
            server.stop();
        }
    }
}
