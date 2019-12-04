import io.github.tetherless_world.jena_elasticsearch.ElasticsearchGraphMaker;
import io.github.tetherless_world.jena_elasticsearch.ElasticsearchGraphMakerConfiguration;
import org.apache.http.HttpHost;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.graph.Graph;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class SPARQLServer {
    private static final Logger logger = LoggerFactory.getLogger(SPARQLServer.class);
    private static FusekiServer server = null;

    public static void main(String args[]) {
        try {
            ElasticsearchGraphMakerConfiguration config = new ElasticsearchGraphMakerConfiguration(
                    ElasticsearchGraphMakerConfiguration.SyncType.ASYNCHRONOUS,
                    new HttpHost("elasticsearch", 9200, "http")
            );
            Graph g = new ElasticsearchGraphMaker(config).createGraph();

            Model m = ModelFactory.createModelForGraph(g);

            if (args.length == 1) {
                // if a dataset was supplied as an argument
                logger.info("Loading file {} into dataset", args[0]);
                System.out.println("Loading file into dataset");
                m.read(new File(args[0]).toURI().toString());
                logger.info("Loaded file {} into dataset", args[0]);
                System.out.println("Loaded file into dataset");
            } else {
                System.out.println("No files loaded");
                logger.info("No files loaded");
            }

            Dataset ds = DatasetFactory.wrap(m);

            server = FusekiServer.create()
                    .add("/dataset", ds)
                    .port(3331)
                    .build();

            server.start();
            logger.info("Fuseki is running on port 3331");
            System.out.println("Fuseki is running on port 3331");

        } catch (IOException e) {
            logger.error("Could not create Elasticsearch Graph", e);
            server.stop();
        }
    }
}
