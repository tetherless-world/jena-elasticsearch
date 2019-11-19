package io.github.tetherless_world.jena_elasticsearch;

import org.apache.http.HttpHost;
import org.apache.jena.graph.GraphMaker;
import org.apache.jena.graph.test.AbstractTestGraphMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticsearchGraphMakerTest extends AbstractTestGraphMaker {
    private GraphMaker gf;
    private final Logger logger = LoggerFactory.getLogger(ElasticsearchGraphMakerTest.class);

    public ElasticsearchGraphMakerTest(String name) {
        super(name);
    }

    @Override
    public GraphMaker getGraphMaker() {
        try {
            // Initialize the GraphMaker
            this.gf = new ElasticsearchGraphMaker(
                    new HttpHost("localhost", 9200, "http")
            );

            // Create and return the graphmaker
            return this.gf;
        } catch (Exception e) {
            // Error reading ES index settings
            this.logger.error("Could not contact Elasticsearch instance");
            throw new RuntimeException(e);
        }
    }

    @Override
    public void tearDown() {
        // Remove all graphs from Elasticsearch (and thus all indices)
        this.gf.removeGraph("_all");

        // Close the GraphMaker
        super.tearDown();
    }
}
