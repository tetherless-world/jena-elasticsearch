package io.github.tetherless_world.jena_elasticsearch;

import junit.framework.TestSuite;
import org.apache.http.HttpHost;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.test.AbstractTestGraph;

import java.nio.charset.Charset;
import java.util.Random;

public class ElasticsearchGraphTest extends AbstractTestGraph {

    public ElasticsearchGraphTest(String name) {
        super(name);
    }

    public static TestSuite suite() {
        return new TestSuite(ElasticsearchGraphTest.class);
    }

    public Graph getGraph() {
        ElasticsearchGraphMaker graphMaker;
        try {
            // Initialize the GraphMaker
            ElasticsearchGraphMakerConfiguration config = new ElasticsearchGraphMakerConfiguration(
                    ElasticsearchGraphMakerConfiguration.SyncType.SYNCHRONOUS,
                    new HttpHost("elasticsearch", 9200, "http")
            );
            graphMaker = new ElasticsearchGraphMaker(config);

            // Generate a random name for a new graph (to avoid name collisions between sequent test runs)
            byte[] array = new byte[10];
            new Random().nextBytes(array);
            String randomGraphName = new String(array, Charset.forName("UTF-8"));

            // Create and return the graph
            return graphMaker.createGraph(randomGraphName, true);
        } catch (Exception e) {
            // Error reading ES index settings
            e.printStackTrace();
            return null;
        }
    }
}
