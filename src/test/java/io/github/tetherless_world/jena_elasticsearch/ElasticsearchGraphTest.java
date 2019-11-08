package io.github.tetherless_world.jena_elasticsearch;

import junit.framework.TestSuite;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.test.AbstractTestGraph;
import org.apache.jena.reasoner.test.TestInfGraph;

public class ElasticsearchGraphTest extends AbstractTestGraph {


    public ElasticsearchGraphTest(String name) {
        super(name);
    }

    public static TestSuite suite()
    { return new TestSuite( ElasticsearchGraphTest.class ); }

    public Graph getGraph() {
        return new ElasticsearchGraph();
    }
}
