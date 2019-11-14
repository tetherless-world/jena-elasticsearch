package io.github.tetherless_world.jena_elasticsearch;

import org.apache.http.HttpHost;
import org.apache.jena.graph.GraphMaker;
import org.apache.jena.graph.test.AbstractTestGraphMaker;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Random;

public class ElasticsearchGraphMakerTest extends AbstractTestGraphMaker {

    public ElasticsearchGraphMakerTest(String name) {
        super(name);
    }

    public GraphMaker getGraphMaker() {
        ElasticsearchGraphMaker graphMaker;
        try {
            // Initialize the GraphMaker
            graphMaker = new ElasticsearchGraphMaker(
                    new HttpHost("localhost",9200, "http")
            );

            // Create and return the graph
            return graphMaker;
        } catch (Exception e) {
            // Error reading ES index settings
            e.printStackTrace();
            return null;
        }
    }
}
