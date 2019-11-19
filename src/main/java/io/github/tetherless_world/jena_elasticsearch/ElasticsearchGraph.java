package io.github.tetherless_world.jena_elasticsearch;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticsearchGraph extends GraphBase {
    private Logger logger = LoggerFactory.getLogger(ElasticsearchGraph.class);
    private RestHighLevelClient client;
    private String name;

    public ElasticsearchGraph(RestHighLevelClient aClient, String aName) {
        this.client = aClient;
        this.name = aName;
    }

    private static String getNodeContent(Node n) {
        if (n.isBlank()) {
            return n.getBlankNodeLabel();
        } else if (n.isLiteral()) {
            return n.getLiteral().toString();
        } else if (n.isURI()) {
            return n.getURI();
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public void performAdd(Triple t) {
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("subject", getNodeContent(t.getSubject()));
        jsonMap.put("predicate", getNodeContent(t.getPredicate()));
        jsonMap.put("object", getNodeContent(t.getObject()));

        try {
            IndexRequest request = new IndexRequest(this.name).source(jsonMap);
            this.client.index(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            logger.error("Error indexing triple: {}", t, e);
            e.printStackTrace();
        }
    }

    @Override
    public void performDelete(Triple t) {
        QueryBuilder queryBuilder = constructTripleMatchingQuery(t);

        try {
            DeleteByQueryRequest request = new DeleteByQueryRequest(this.name);
            request.setQuery(queryBuilder);
            this.client.deleteByQuery(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            logger.error("Error deleting triple: {}", t);
            e.printStackTrace();
        }
    }

    @Override
    protected ExtendedIterator<Triple> graphBaseFind(Triple triple) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(this.constructTripleMatchingQuery(triple));

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(this.name);
        searchRequest.source(searchSourceBuilder);

        try {
            SearchResponse searchResponse = this.client.search(searchRequest, RequestOptions.DEFAULT);
            List<Triple> triple_results = new ArrayList<>();
            for (SearchHit hit : searchResponse.getHits().getHits()) {
                Map<String, Object> fields = hit.getSourceAsMap();
                String s = (String) fields.get("subject");
                String p = (String) fields.get("predicate");
                String o = (String) fields.get("object");

                triple_results.add(Triple.create(createNode(s), createNode(p), createNode(o)));
            }

            return WrappedIterator.create(triple_results.iterator());
        } catch (IOException e) {
            logger.error("Search for triple {} failed", triple, e);
        }

        return WrappedIterator.create(new ArrayList<Triple>().iterator());
    }

    /**
     * Constructs a BoolQueryBuilder that matches the subject, predicate, and object of a given triple. Any of the
     * subject, predicate, or object can be Node.ANY, meaning that the given field will match anything
     *
     * @param triple the triple that the query must match
     * @return a QueryBuilder that will match that triple
     */
    private QueryBuilder constructTripleMatchingQuery(Triple triple) {
        BoolQueryBuilder queryBuilder = new BoolQueryBuilder();

        if (!triple.getSubject().equals(Node.ANY)) {
            MatchQueryBuilder matchSubjectQueryBuilder = new MatchQueryBuilder("subject", getNodeContent(triple.getSubject()));
            matchSubjectQueryBuilder.operator(Operator.AND);
            queryBuilder = queryBuilder.must(matchSubjectQueryBuilder);
        }
        if (!triple.getPredicate().equals(Node.ANY)) {
            MatchQueryBuilder matchPredicateQueryBuilder = new MatchQueryBuilder("predicate", getNodeContent(triple.getPredicate()));
            matchPredicateQueryBuilder.operator(Operator.AND);
            queryBuilder = queryBuilder.must(matchPredicateQueryBuilder);
        }
        if (!triple.getObject().equals(Node.ANY)) {
            MatchQueryBuilder matchObjectQueryBuilder = new MatchQueryBuilder("object", getNodeContent(triple.getObject()));
            matchObjectQueryBuilder.operator(Operator.AND);
            queryBuilder = queryBuilder.must(matchObjectQueryBuilder);
        }

        if (queryBuilder.must().size() == 0) {
            // i.e. if s,p,o are all Node.ANY and thus this query should match all triples
            // construct and return a MatchAllQueryBuilder
            return QueryBuilders.matchAllQuery();
        }
        return queryBuilder;
    }

    /**
     * Returns the number of triples in this graph
     * @return the number of triples in this graph
     */
    @Override
    protected int graphBaseSize() {
        try {
            CountRequest countRequest = new CountRequest(this.name);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
            countRequest.source(searchSourceBuilder);

            CountResponse response = this.client.count(countRequest, RequestOptions.DEFAULT);
            return (int) response.getCount();
        } catch (IOException e) {
            logger.error("Could not retrieve size of graph '{}'", this.name, e);
        }
        // TODO: Is this an appropriate default return value??
        return -1;
    }

    /**
     * Returns a blank, literal, or URI node for the given string
     *
     * @param s the value of the node
     * @return a node corresponding to s
     */
    private Node createNode(String s) {
        if (s.startsWith("_:")) {
            return NodeFactory.createBlankNode(s);
        }
        if (s.startsWith("\"")) {
            return NodeFactory.createLiteral(s);
        }
        return NodeFactory.createURI(s);
    }
}