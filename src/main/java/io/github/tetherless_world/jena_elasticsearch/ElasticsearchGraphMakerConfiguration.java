package io.github.tetherless_world.jena_elasticsearch;

import org.apache.http.HttpHost;

/**
 * A configuration object consisting of:
 * - a SyncType: ASYNCHRONOUS (do not wait for updated data to be read-visible)
 * or SYNCHRONOUS (wait for updated data to be read-visible)
 * - a list of HttpHosts: each HttpHost refers to an Elasticsearch node
 * <p>
 * The configuration object can be used to initialize an ElasticsearchGraphMaker
 * factory.
 */
public class ElasticsearchGraphMakerConfiguration {
    public enum SyncType {
        ASYNCHRONOUS, // updated data may not be immediately read-visible
        SYNCHRONOUS   // updated data will be immediately read-visible
    }

    public final HttpHost[] hosts;
    public final SyncType syncType;

    public ElasticsearchGraphMakerConfiguration(SyncType st, HttpHost... httpHosts) {
        this.syncType = st;
        this.hosts = httpHosts;
    }
}
