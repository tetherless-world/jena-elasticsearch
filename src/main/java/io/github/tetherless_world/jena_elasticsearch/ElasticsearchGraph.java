package io.github.tetherless_world.jena_elasticsearch;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.Scroll;
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
    private final Logger logger = LoggerFactory.getLogger(ElasticsearchGraph.class);
    private final RestHighLevelClient client;
    private final String name;
    private final ElasticsearchGraphMakerConfiguration.SyncType syncType;

    public ElasticsearchGraph(RestHighLevelClient aClient, String aName, ElasticsearchGraphMakerConfiguration.SyncType st) {
        this.client = aClient;
        this.name = aName;
        this.syncType = st;
    }

    private static String getNodeContent(Node n) {

        if (n.isBlank()) {
            return "_:" + n.getBlankNodeLabel();
        } else if (n.isLiteral()) {
            return "L:" + n.getLiteral().toString();
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
            final IndexRequest request = new IndexRequest(this.name).source(jsonMap);
            if (this.syncType.equals(ElasticsearchGraphMakerConfiguration.SyncType.SYNCHRONOUS)) {
                // if this is a synchronous graph, wait for the triple to be added
                request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
            }
            this.client.index(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            logger.error("Error indexing triple: {}", t, e);
        }

        this.logger.debug("Added triple {}; graph size = {}", t, this.graphBaseSize());
    }

    private void blockUntilAdded(Triple t) {
        final RefreshRequest refreshRequest = new RefreshRequest(this.name);
        final SearchRequest searchRequest = new SearchRequest();
        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        searchSourceBuilder.query(this.constructExactTripleMatchingQuery(t));
        searchRequest.indices(this.name);
        searchRequest.source(searchSourceBuilder);

        // Try 10 times to refresh Elasticsearch
        for (int i = 0; i < 10; ++i) {
            try {
                SearchResponse searchResponse = this.client.search(searchRequest, RequestOptions.DEFAULT);
                if (searchResponse.getHits().getHits().length != 0) {
                    // contains the triple
                    return;
                }
                RefreshResponse refreshResponse = client.indices().refresh(refreshRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                this.logger.error("Error refreshing index for graph '{}'", this.name, e);
            }

            if (i > 0) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    this.logger.error("Error waiting for retrying refresh request", e);
                }
            }
        }
    }

    @Override
    public void performDelete(Triple t) {
        QueryBuilder queryBuilder = constructTripleMatchingQuery(t);

        logger.debug("Deleting with query {}", queryBuilder.getWriteableName());

        try {
            DeleteByQueryRequest request = new DeleteByQueryRequest(this.name);
            request.setQuery(queryBuilder);
            if (this.syncType.equals(ElasticsearchGraphMakerConfiguration.SyncType.SYNCHRONOUS)) {
                // if this is a synchronous graph, request a refresh
                request.setRefresh(true);
            }

            this.client.deleteByQuery(request, RequestOptions.DEFAULT);

        } catch (IOException e) {
            logger.error("Error deleting triple: {}", t, e);
        }

        if (this.syncType.equals(ElasticsearchGraphMakerConfiguration.SyncType.SYNCHRONOUS)) {
            // if this is a synchronous graph, wait for the triple to be added
            this.blockUntilDeleted(t);
        }

        this.logger.debug("Deleted triple {}; graph size = {}", t, this.graphBaseSize());
    }

    private void blockUntilDeleted(Triple t) {
        final RefreshRequest refreshRequest = new RefreshRequest(this.name);
        final SearchRequest searchRequest = new SearchRequest();
        final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        searchSourceBuilder.query(this.constructTripleMatchingQuery(t));
        searchRequest.indices(this.name);
        searchRequest.source(searchSourceBuilder);

        // Try 10 times to refresh Elasticsearch
        for (int i = 0; i < 10; ++i) {
            try {
                SearchResponse searchResponse = this.client.search(searchRequest, RequestOptions.DEFAULT);
                if (searchResponse.getHits().getHits().length == 0) {
                    // contains the triple
                    return;
                }
                RefreshResponse refreshResponse = client.indices().refresh(refreshRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                this.logger.error("Error refreshing index for graph '{}'", this.name, e);
            }

            if (i > 0) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    this.logger.error("Error waiting for retrying refresh request", e);
                }
            }
        }
    }

    @Override
    protected ExtendedIterator<Triple> graphBaseFind(Triple triple) {
        logger.debug("Called graphBaseFind for triple {}", triple);

        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(this.constructTripleMatchingQuery(triple));
        searchSourceBuilder.size(100); // receive up to 100 hits per call

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(this.name);
        searchRequest.source(searchSourceBuilder);
        searchRequest.scroll(scroll);

        try {
            SearchResponse searchResponse = this.client.search(searchRequest, RequestOptions.DEFAULT);
            String scrollId = searchResponse.getScrollId();
            SearchHit[] searchHits = searchResponse.getHits().getHits();
            List<Triple> tripleResults = new ArrayList<>();

            for (SearchHit hit : searchHits) {
                logger.debug("Hit: {}", hit.getSourceAsMap());
                Map<String, Object> fields = hit.getSourceAsMap();
                String s = (String) fields.get("subject");
                String p = (String) fields.get("predicate");
                String o = (String) fields.get("object");

                tripleResults.add(Triple.create(createNode(s), createNode(p), createNode(o)));
            }

            while (searchHits != null && searchHits.length > 0) {
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(scroll);
                searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
                scrollId = searchResponse.getScrollId();
                searchHits = searchResponse.getHits().getHits();

                for (SearchHit hit : searchHits) {
                    logger.debug("Hit: {}", hit.getSourceAsMap());
                    Map<String, Object> fields = hit.getSourceAsMap();
                    String s = (String) fields.get("subject");
                    String p = (String) fields.get("predicate");
                    String o = (String) fields.get("object");

                    tripleResults.add(Triple.create(createNode(s), createNode(p), createNode(o)));
                }
            }

            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);
            ClearScrollResponse clearScrollResponse = this.client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
            if (!clearScrollResponse.isSucceeded()) {
                logger.error("Could not clear scroll {} from Elasticsearch",scrollId);
                throw new IOException("Could not clear scroll "+scrollId);
            }

            logger.debug("Find {}; results: {}", triple.toString(), tripleResults.toString());
            return new ElasticsearchTripleIterator(tripleResults, this);
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
            TermQueryBuilder matchSubjectQueryBuilder = new TermQueryBuilder("subject", getNodeContent(triple.getSubject()));
            queryBuilder = queryBuilder.must(matchSubjectQueryBuilder);
        }
        if (!triple.getPredicate().equals(Node.ANY)) {
            TermQueryBuilder matchPredicateQueryBuilder = new TermQueryBuilder("predicate", getNodeContent(triple.getPredicate()));
            queryBuilder = queryBuilder.must(matchPredicateQueryBuilder);
        }
        if (!triple.getObject().equals(Node.ANY)) {
            TermQueryBuilder matchObjectQueryBuilder = new TermQueryBuilder("object", getNodeContent(triple.getObject()));
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
     * Constructs a BoolQueryBuilder that matches the subject, predicate, and object of a given triple exactly
     *
     * @param triple the triple that the query must match
     * @return a QueryBuilder that will match that triple
     */
    private QueryBuilder constructExactTripleMatchingQuery(Triple triple) {
        BoolQueryBuilder queryBuilder = new BoolQueryBuilder();

        TermQueryBuilder matchSubjectQueryBuilder = new TermQueryBuilder("subject", getNodeContent(triple.getSubject()));
        TermQueryBuilder matchPredicateQueryBuilder = new TermQueryBuilder("predicate", getNodeContent(triple.getPredicate()));
        TermQueryBuilder matchObjectQueryBuilder = new TermQueryBuilder("object", getNodeContent(triple.getObject()));

        queryBuilder = queryBuilder.must(matchSubjectQueryBuilder);
        queryBuilder = queryBuilder.must(matchPredicateQueryBuilder);
        queryBuilder = queryBuilder.must(matchObjectQueryBuilder);

        return queryBuilder;
    }

    /**
     * Returns the number of triples in this graph
     *
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
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns a blank, literal, or URI node for the given string
     *
     * @param s the value of the node
     * @return a node corresponding to s
     */
    private static Node createNode(String s) {
        if (s.startsWith("_:")) {
            return NodeFactory.createBlankNode(s.substring(2));
        }
        if (s.startsWith("L:")) {
            return NodeFactory.createLiteral(s.substring(2));
        }
        return NodeFactory.createURI(s);
    }
}