import io.github.tetherless_world.jena_elasticsearch.ElasticsearchGraphMaker;
import io.github.tetherless_world.jena_elasticsearch.ElasticsearchGraphMakerConfiguration;
import org.apache.http.HttpHost;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.system.FusekiLogging;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.GraphMaker;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.tdb2.TDB2Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class SPARQLServer {
    private static final Logger logger = LoggerFactory.getLogger(SPARQLServer.class);
    private static FusekiServer server = null;

    public static void main(String args[]) {
        FusekiLogging.setLogging();

        try {
            Dataset ds;
            Model m;
            if (args[0].equals("elasticsearch-persistent")) {
                logger.info("Creating persistent Elasticsearch graph");

                ElasticsearchGraphMakerConfiguration config = new ElasticsearchGraphMakerConfiguration(
                        ElasticsearchGraphMakerConfiguration.SyncType.ASYNCHRONOUS,
                        new HttpHost("elasticsearch", 9200, "http")
                );
                GraphMaker graphMaker = new ElasticsearchGraphMaker(config);
                graphMaker.removeGraph("_all");

                Graph g = graphMaker.createGraph();
                m = ModelFactory.createModelForGraph(g);
                ds = DatasetFactory.wrap(m);
            } else if (args[0].equals("tdb2-mem")) {
                logger.info("Creating in-memory TDB2 graph");
                ds = TDB2Factory.createDataset();
                m = ds.getDefaultModel();
            } else if (args[0].equals("tdb2-persistent")) {
                String location = "tdb2-dataset"; // path to the TDB2 location
                logger.info("Creating persistent TDB2 graph at {}", location);
                ds = TDB2Factory.connectDataset("tdb2-dataset");
                m = ds.getDefaultModel();
            } else {
                throw new IllegalArgumentException("Invalid store type provided");
            }

            // Load data into Model if necessary
            if (args.length == 2) {
                // if a dataset was supplied as an argument
                logger.info("Loading file {} into dataset", args[1]);

                long startTime = System.currentTimeMillis();
                ds.begin(ReadWrite.WRITE);
                m.read(new File(args[1]).toURI().toString());
                ds.commit();
                ds.end();
                long endTime = System.currentTimeMillis();

                logger.info("Loaded file {} into dataset in {}ms", args[1], endTime - startTime);
            } else {
                logger.info("No files loaded");
            }

            // Create dataset from model
            //Dataset ds = DatasetFactory.wrap(m);

            // Build and start the server
            server = FusekiServer.create()
                    .add("/dataset", ds)
                    .port(3331)
                    .build();
            server.start();
            logger.info("Fuseki is running on port 3331");
        } catch (IOException e) {
            logger.error("Could not create graph", e);
            server.stop();
        }
    }
}
