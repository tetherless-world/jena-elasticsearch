package io.github.tetherless_world.jena_elasticsearch;

import org.apache.http.HttpHost;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.impl.BaseGraphMaker;
import org.apache.jena.shared.AlreadyExistsException;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

public class ElasticsearchGraphMaker extends BaseGraphMaker {
    public Set<String> graphNames = new LinkedHashSet<>();
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
        // Read the Elasticsearch index settings from the settings resource file
        InputStream inputStream = getClass().getResourceAsStream(settingsPath);
        StringBuilder resultStringBuilder = new StringBuilder();
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
        String line;
        while ((line = br.readLine()) != null) {
            resultStringBuilder.append(line).append("\n");
        }
        elasticsearchIndexSettings = resultStringBuilder.toString();

        // Construct the high-level ES REST client
        client = new RestHighLevelClient(RestClient.builder(httpHosts));

        // Get all the graphs currently on this Elasticsearch cluster
        // and add them to the locally tracked set
        GetIndexRequest request = new GetIndexRequest("_all");
        GetIndexResponse response = client.indices().get(request, RequestOptions.DEFAULT);
        graphNames.addAll(Arrays.asList(response.getIndices()));
    }

    /**
     * Return an ElasticsearchGraph with the given name
     *
     * @param name   the name of the ElasticsearchGraph
     * @param strict if true, throw AlreadyExistsException a graph if a graph with the given name already exists
     * @return
     */
    public Graph createGraph(String name, boolean strict) throws AlreadyExistsException {
        // Elasticsearch only allows lowercase index names
        String lowerName = name.toLowerCase();

        if (graphNames.contains(lowerName)) {
            // there is already a graph with this name
            if (strict) {
                // return the associated graph
            } else {
                throw new AlreadyExistsException("Graph '" + lowerName + "' already exists");
            }
        } else {
            // there is no graph with this name yet
            CreateIndexRequest request = new CreateIndexRequest(lowerName);
            request.source(elasticsearchIndexSettings, XContentType.JSON);
            try {
                client.indices().create(request, RequestOptions.DEFAULT);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    /**
     * Return an ElasticsearchGraph with the given name
     *
     * @param name   the name of the ElasticsearchGraph
     * @param strict if true, throw AlreadyExistsException a graph if a graph with the given name already exists
     * @return
     */
    public Graph openGraph(String name, boolean strict) {
        return null;
    }

    public void removeGraph(String name) {

    }

    public boolean hasGraph(String name) {
        return false;
    }

    public void close() {

    }

    public ExtendedIterator<String> listGraphs() {
        return null;
    }
}
