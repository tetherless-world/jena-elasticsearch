package io.github.tetherless_world.jena_elasticsearch;

import org.apache.http.HttpHost;
import org.apache.jena.graph.GraphMaker;
import org.apache.jena.graph.test.AbstractTestGraphMaker;

public class ElasticsearchGraphMakerTest extends AbstractTestGraphMaker {
    private GraphMaker gf;

    public ElasticsearchGraphMakerTest(String name) {
        super(name);
    }

    @Override
    public GraphMaker getGraphMaker() {
        ElasticsearchGraphMaker graphMaker;
        try {
            // Initialize the GraphMaker
            this.gf = new ElasticsearchGraphMaker(
                    new HttpHost("localhost", 9200, "http")
            );

            // Create and return the graph
            return this.gf;
        } catch (Exception e) {
            // Error reading ES index settings
            e.printStackTrace();
            return null;
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
