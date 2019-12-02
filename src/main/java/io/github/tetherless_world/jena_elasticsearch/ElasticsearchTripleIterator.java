package io.github.tetherless_world.jena_elasticsearch;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.NiceIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;

class ElasticsearchTripleIterator extends NiceIterator<Triple> {
    private final static Logger logger = LoggerFactory.getLogger(ElasticsearchGraph.class);
    private final Graph graph;
    private final Collection<Triple> collection;
    private final Iterator<Triple> iter;
    private Triple curr = null;

    public ElasticsearchTripleIterator(Collection<Triple> c, Graph g) {
        this.graph = g;
        this.collection = c;
        this.iter = this.collection.iterator();
    }

    @Override
    public void remove() {
        if (this.curr != null) {
            this.graph.delete(this.curr);
        } else {
            logger.debug("No such element but remove called on iterator: collection={}",this.collection);
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public Triple next() {
        this.curr = this.iter.next();
        return this.curr;
    }

    @Override
    public boolean hasNext() {
        return this.iter.hasNext();
    }
}
