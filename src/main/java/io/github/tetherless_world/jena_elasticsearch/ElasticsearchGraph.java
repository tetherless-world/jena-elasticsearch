package io.github.tetherless_world.jena_elasticsearch;

import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.util.iterator.ExtendedIterator;

public class ElasticsearchGraph extends GraphBase {


    protected ExtendedIterator<Triple> graphBaseFind(Triple triple) {
        throw new UnsupportedOperationException();
    }

    public ExtendedIterator<Triple> find() {
        throw new UnsupportedOperationException();
    }
}
