package io.github.tetherless_world.jena_elasticsearch;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ElasticsearchGraph extends GraphBase {

    private RestHighLevelClient client;
    private String name;

    public ElasticsearchGraph(RestHighLevelClient aClient, String aName) {
        this.client = aClient;
        this.name = aName;
    }

    private static String getNodeName(Node n) {
        if (n.isBlank()) {
            return n.getBlankNodeLabel();
        } else if (n.isLiteral()) {
            return n.getLiteral().toString();
        } else if (n.isURI()) {
            return n.getURI();
        } else {
            return null;
        }
    }

    @Override
    public ExtendedIterator<Triple> find() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void performAdd(Triple t) {
        Logger logger = LoggerFactory.getLogger(ElasticsearchGraph.class);

        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("subject", getNodeName(t.getSubject()));
        jsonMap.put("predicate", getNodeName(t.getPredicate()));
        jsonMap.put("object", getNodeName(t.getObject()));

//        jsonMap.put("subject", t.getSubject().toString());
//        jsonMap.put("predicate", t.getPredicate().toString());
//        jsonMap.put("object", t.getObject().toString());

        try {
            IndexRequest request = new IndexRequest(this.name).source(jsonMap);
            //request.id(Integer.toString(t.hashCode())); // for now, use the triple hash as the ID
            this.client.index(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            logger.error("Error indexing triple: " + t.toString());
            e.printStackTrace();
        }
    }

    @Override
    public void performDelete(Triple t) {
        Logger logger = LoggerFactory.getLogger(ElasticsearchGraph.class);

        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("subject", getNodeName(t.getSubject()));
        jsonMap.put("predicate", getNodeName(t.getPredicate()));
        jsonMap.put("object", getNodeName(t.getObject()));

        try {
            DeleteByQueryRequest request = new DeleteByQueryRequest(this.name);
//            request.setQuery(new TermQueryBuilder("subject", getNodeName(t.getSubject())));
            //request.id(Integer.toString(t.hashCode())); // for now, use the triple hash as the ID
            this.client.deleteByQuery(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            logger.error("Error indexing triple: " + t.toString());
            e.printStackTrace();
        }
    }

    @Override
    protected ExtendedIterator<Triple> graphBaseFind(Triple triple) {
        return null;
    }

    @Override
    protected int graphBaseSize() {
        return super.graphBaseSize();
    }
}
