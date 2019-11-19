package io.github.tetherless_world.jena_elasticsearch;

import org.apache.commons.codec.binary.Base32;
import org.apache.http.HttpHost;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.impl.BaseGraphMaker;
import org.apache.jena.shared.AlreadyExistsException;
import org.apache.jena.shared.DoesNotExistException;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;
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
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ElasticsearchGraphMaker extends BaseGraphMaker {
    private Logger logger = LoggerFactory.getLogger(ElasticsearchGraphMaker.class);
    private Set<String> graphNames;
    private RestHighLevelClient client;
    private String elasticsearchIndexSettings;
    private final static String settingsPath = "elasticsearchIndexSettings.json";

    /**
     * Constructor for ElasticsearchGraphMaker
     *
     * @param httpHosts Elasticsearch nodes in the cluster
     * @throws IOException
     */
    public ElasticsearchGraphMaker(HttpHost... httpHosts) throws IOException {
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
            this.graphNames = new LinkedHashSet<String>();
            this.graphNames.addAll(Arrays.asList(response.getIndices()));
        } catch (ElasticsearchStatusException e) {
            // no indices exist yet
            // Elasticsearch throws an exception when requesting to get all indices but none exist
            this.graphNames = new LinkedHashSet<String>();
        }
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
        // Encode the given graph name into a name safe for Elasticsearch
        String validIndexName = encodeIndexName(name);
        if (validIndexName.startsWith("_")) {
            logger.error("Index name cannot begin with underscore");
            return null;
        }

        if (this.graphNames.contains(validIndexName)) {
            // there is already a graph with this name
            if (strict) {
                logger.error("Cannot create graph '{}'; already exists", validIndexName);
                throw new AlreadyExistsException("Graph '" + validIndexName + "' already exists");
            } else {
                // return the associated graph
                return new ElasticsearchGraph(this.client, validIndexName);
            }
        } else {
            try {
                // there is no graph with this name yet
                // create the index for this graph
                CreateIndexRequest request = new CreateIndexRequest(validIndexName);
                request.source(this.elasticsearchIndexSettings, XContentType.JSON);
                this.client.indices().create(request, RequestOptions.DEFAULT);

                this.graphNames.add(validIndexName);
                logger.info("Created graph with name '{}'", validIndexName);

                // return the graph object
                return new ElasticsearchGraph(this.client, validIndexName);
            } catch (Exception e) {
                logger.error("Could not create index '{}'", validIndexName, e);
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
        // Elasticsearch only allows lowercase index names
        String validIndexName = encodeIndexName(name);
        if (validIndexName.startsWith("_")) {
            logger.error("Index name cannot begin with underscore");
            return null;
        }

        if (this.graphNames.contains(validIndexName)) {
            // there is already a graph with this name
            // return the associated graph
            return new ElasticsearchGraph(this.client, validIndexName);
        } else {
            // there is no graph with this name yet
            if (strict) {
                // throw an error instead of creating the graph
                logger.error("Cannot open graph '{}'; does not exist", validIndexName);
                throw new DoesNotExistException("Graph '" + validIndexName + "' does not exist");
            } else {
                // there is no graph with this name yet
                // create the index for this graph
                try {
                    CreateIndexRequest request = new CreateIndexRequest(validIndexName);
                    request.source(this.elasticsearchIndexSettings, XContentType.JSON);
                    this.client.indices().create(request, RequestOptions.DEFAULT);

                    this.graphNames.add(validIndexName);
                    logger.info("Created graph with name '{}'", validIndexName);

                    // return the graph object
                    return new ElasticsearchGraph(this.client, validIndexName);
                } catch (IOException e) {
                    logger.error("Could not create index '{}'", validIndexName, e);
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
        String validIndexName = encodeIndexName(name);

        try {
            DeleteIndexRequest request = new DeleteIndexRequest(validIndexName);
            this.client.indices().delete(request, RequestOptions.DEFAULT);
            this.graphNames.remove(validIndexName);
            logger.info("Graph '" + validIndexName + "' removed");
        } catch (IOException e) {
            logger.error("Could not remove graph '{}'", name, e);
        }
    }

    @Override
    public boolean hasGraph(String name) {
        return this.graphNames.contains(encodeIndexName(name));
    }

    @Override
    public void close() {

    }

    @Override
    public ExtendedIterator<String> listGraphs() {
        // Make a list of the decoded index names (i.e. a list of graph names)
        List<String> graphNamesList = this.graphNames.stream().map(this::decodeIndexName).collect(Collectors.toList());

        // Construct and return the iterator over these values
        ExtendedIterator<String> iter = WrappedIterator.create(graphNamesList.iterator());
        return iter;
    }

    /**
     * Given a string, transform it so that it can be used as an index name in Elasticsearch. I.e.:
     * - Cannot begin with _, -, or +
     * - Cannot contain characters [ , \", *, \\, <, |, ,, >, /, ?, :, #]
     * - Cannot exceed 255 characters
     * - Cannot be '.' or '..'
     * - Must contain all lowercase characters
     *
     * @param graphName the string to transform into a safe index name
     * @return
     */
    private String encodeIndexName(String graphName) {
        if (graphName.equals("_all")) {
            return graphName;
        }
        String encodedName = (new Base32()).encodeAsString(graphName.getBytes()).replaceAll("=", "_").toLowerCase();
        if (encodedName.length() > 255) {
            // TODO: Implement better checking for graph names that are too long?
            throw new IllegalArgumentException();
        }
        return encodedName;
    }

    private String decodeIndexName(String indexName) {
        if (indexName.equals("_all")) {
            return indexName;
        }
        return new String((new Base32()).decode(indexName.replaceAll("_", "=").toUpperCase()));
    }
}
