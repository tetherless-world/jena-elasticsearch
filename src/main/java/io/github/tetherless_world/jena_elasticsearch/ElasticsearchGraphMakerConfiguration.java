package io.github.tetherless_world.jena_elasticsearch;

import org.apache.http.HttpHost;

public class ElasticsearchGraphMakerConfiguration {
    public enum SyncType {
        ASYNCHRONOUS, SYNCHRONOUS
    }

    public final HttpHost[] hosts;
    public final SyncType syncType;

    public ElasticsearchGraphMakerConfiguration(SyncType st, HttpHost... httpHosts) {
        this.syncType = st;
        this.hosts = httpHosts;
    }
}
