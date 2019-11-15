package io.github.tetherless_world.jena_elasticsearch;

import org.apache.http.HttpHost;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.impl.BaseGraphMaker;
import org.apache.jena.shared.AlreadyExistsException;
import org.apache.jena.shared.DoesNotExistException;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

public class ElasticsearchGraphMaker extends BaseGraphMaker {
    public Set<String> graphNames;
    public RestHighLevelClient client;
    public String elasticsearchIndexSettings;
    public final String settingsPath = "elasticsearchIndexSettings.json";

    /**
     * Constructor for ElasticsearchGraphMaker
     *
     * @param httpHosts Elasticsearch nodes in the cluster
     * @throws IOException
     */
    public ElasticsearchGraphMaker(HttpHost... httpHosts) throws IOException, URISyntaxException {
        Logger logger = LoggerFactory.getLogger(ElasticsearchGraphMaker.class);

        // Read the Elasticsearch index settings from the settings resource file
        InputStream inputStream = getClass().getResourceAsStream(this.settingsPath);
        StringBuilder resultStringBuilder = new StringBuilder();
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
        String line;
        while ((line = br.readLine()) != null) {
            resultStringBuilder.append(line).append("\n");
        }
        this.elasticsearchIndexSettings = resultStringBuilder.toString();

        // Construct the high-level ES REST client
        this.client = new RestHighLevelClient(RestClient.builder(httpHosts));

        // Get all the graphs currently on this Elasticsearch cluster
        // and add them to the locally tracked set
        GetIndexRequest request = new GetIndexRequest("_all");

        try {
            GetIndexResponse response = this.client.indices().get(request, RequestOptions.DEFAULT);
            this.graphNames = new LinkedHashSet<>();
            this.graphNames.addAll(Arrays.asList(response.getIndices()));
        } catch (ElasticsearchStatusException e) {
            // no indices exist yet
            // Elasticsearch throws an exception when requesting to get all indices but none exist
            this.graphNames = new LinkedHashSet<>();
        }

        System.out.println("ER 1");
    }

    /**
     * Return an ElasticsearchGraph with the given name
     *
     * @param name   the name of the ElasticsearchGraph
     * @param strict if true, throw AlreadyExistsException a graph if a graph with the given name already exists
     *               instead of returning the existing graph
     * @return
     */
    @Override
    public Graph createGraph(String name, boolean strict) throws AlreadyExistsException {
        Logger logger = LoggerFactory.getLogger(ElasticsearchGraphMaker.class);

        // Elasticsearch only allows lowercase index names
        String lowerName = name.toLowerCase();
        if (lowerName.startsWith("_")) {
            logger.error("Index name cannot begin with underscore");
            return null;
        }

        if (this.graphNames.contains(lowerName)) {
            // there is already a graph with this name
            if (strict) {
                logger.error("Graph already exists");
                throw new AlreadyExistsException("Graph '" + lowerName + "' already exists");
            } else {
                // return the associated graph
                return new ElasticsearchGraph(this.client, lowerName);
            }
        } else {
            try {
                // there is no graph with this name yet
                // create the index for this graph
                CreateIndexRequest request = new CreateIndexRequest(lowerName);
                request.source(this.elasticsearchIndexSettings, XContentType.JSON);
                this.client.indices().create(request, RequestOptions.DEFAULT);

                this.graphNames.add(lowerName);
                logger.info("Created graph with name '" + lowerName + "'");

                // return the graph object
                return new ElasticsearchGraph(this.client, lowerName);
            } catch (IOException e) {
                logger.error("Could not create index");
            }
        }
        return null;
    }

    /**
     * Return an ElasticsearchGraph with the given name, creating one if it does not exist
     *
     * @param name   the name of the ElasticsearchGraph
     * @param strict if true, throw AlreadyExistsException a graph if a graph with the given name does not exist,
     *               instead of creating the graph
     * @return
     */
    @Override
    public Graph openGraph(String name, boolean strict) {
        Logger logger = LoggerFactory.getLogger(ElasticsearchGraphMaker.class);

        // Elasticsearch only allows lowercase index names
        String lowerName = name.toLowerCase();
        if (lowerName.startsWith("_")) {
            logger.error("Index name cannot begin with underscore");
            return null;
        }

        if (this.graphNames.contains(lowerName)) {
            // there is already a graph with this name
            // return the associated graph
            return new ElasticsearchGraph(this.client, lowerName);
        } else {
            // there is no graph with this name yet
            if (strict) {
                // throw an error instead of creating the graph
                logger.error("Graph '" + lowerName + "' does not exist");
                throw new DoesNotExistException("Graph '" + lowerName + "' does not exist");
            } else {
                // there is no graph with this name yet
                // create the index for this graph
                try {
                    CreateIndexRequest request = new CreateIndexRequest(lowerName);
                    request.source(this.elasticsearchIndexSettings, XContentType.JSON);
                    this.client.indices().create(request, RequestOptions.DEFAULT);

                    this.graphNames.add(lowerName);
                    logger.info("Created graph with name '" + lowerName + "'");

                    // return the graph object
                    return new ElasticsearchGraph(this.client, lowerName);
                } catch (IOException e) {
                    logger.error("Could not create index");
                }
            }
        }
        return null;
    }

    /**
     * Removes a graph from Elasticsearch
     *
     * @param name the name of the graph to remove (or "_all" to remove all graphs)
     */
    @Override
    public void removeGraph(String name) {
        Logger logger = LoggerFactory.getLogger(ElasticsearchGraphMaker.class);
        String lowerName = name.toLowerCase();

        try {
            DeleteIndexRequest request = new DeleteIndexRequest(lowerName);
            this.client.indices().delete(request, RequestOptions.DEFAULT);
            this.graphNames.remove(lowerName);
            logger.info("Graph '" + lowerName + "' removed");
        } catch (IOException e) {
            logger.error("Could not remove graph '" + name + "'");
        }
    }

    @Override
    public boolean hasGraph(String name) {
        Logger logger = LoggerFactory.getLogger(ElasticsearchGraphMaker.class);
        return this.graphNames.contains(name.toLowerCase());
    }

    @Override
    public void close() {

    }

    @Override
    public ExtendedIterator<String> listGraphs() {
        Logger logger = LoggerFactory.getLogger(ElasticsearchGraphMaker.class);
        //this.graphNames.iterator();

        return null;
    }
}
