package io.github.tetherless_world.jena_elasticsearch;

import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.elasticsearch.client.RestHighLevelClient;

public class ElasticsearchGraph extends GraphBase {

    private RestHighLevelClient client;
    private String name;

    public ElasticsearchGraph(RestHighLevelClient aClient, String aName) {
        this.client = aClient;
        this.name = aName;
    }

    @Override
    protected ExtendedIterator<Triple> graphBaseFind(Triple triple) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExtendedIterator<Triple> find() {
        throw new UnsupportedOperationException();
    }
}
