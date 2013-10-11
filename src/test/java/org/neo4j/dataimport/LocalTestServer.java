package org.neo4j.dataimport;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.core.GraphPropertiesImpl;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.configuration.PropertyFileConfigurator;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.WrappingDatabase;
import org.neo4j.server.modules.RESTApiModule;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.modules.ThirdPartyJAXRSModule;
import org.neo4j.server.preflight.PreFlightTasks;
import org.neo4j.server.web.Jetty6WebServer;
import org.neo4j.server.web.WebServer;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.neo4j.helpers.collection.IteratorUtil.asCollection;

public class LocalTestServer {
    private CommunityNeoServer neoServer;
    private final int port;
    private final String hostname;
    protected String propertiesFile = "test-db.properties";
    private final ImpermanentGraphDatabase graphDatabase;

    public LocalTestServer() {
        this("localhost",7473);
    }

    public LocalTestServer(String hostname, int port) {
        this.port = port;
        this.hostname = hostname;
        graphDatabase = (ImpermanentGraphDatabase) new TestGraphDatabaseFactory().newImpermanentDatabase();
    }

    public void start() {
        if (neoServer!=null) throw new IllegalStateException("Server already running");
        URL url = getClass().getResource("/" + propertiesFile);
        if (url==null) throw new IllegalArgumentException("Could not resolve properties file "+propertiesFile);
        final Jetty6WebServer jettyWebServer = new Jetty6WebServer();
        neoServer = new CommunityNeoServer(new PropertyFileConfigurator(new File(url.getPath()))) {
            @Override
            protected int getWebServerPort() {
                return port;
            }

            @Override
            protected Database createDatabase() {
                return new WrappingDatabase(graphDatabase);
            }

            @Override
            protected PreFlightTasks createPreflightTasks() {
                return new PreFlightTasks();
            }

            @Override
            protected WebServer createWebServer() {
                return jettyWebServer;
            }

            @Override
            protected Iterable<ServerModule> createServerModules() {

                return Arrays.<ServerModule>asList(
                        new RESTApiModule(webServer,database,configurator.configuration()),
                        new ThirdPartyJAXRSModule(webServer, configurator, this));
            }
        };
        neoServer.start();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void stop() {
        try {
            neoServer.stop();
        } catch(Exception e) {
            System.err.println("Error stopping server: "+e.getMessage());
        }
        neoServer=null;
    }

    public int getPort() {
        return port;
    }

    public String getHostname() {
        return hostname;
    }

    public LocalTestServer withPropertiesFile(String propertiesFile) {
        this.propertiesFile = propertiesFile;
        return this;
    }
    public Database getDatabase() {
        return neoServer.getDatabase();
    }

    public URI baseUri() {
        return neoServer.baseUri();
    }

    public void cleanDb() {
        graphDatabase.cleanContent();
        cleanGraphProperties();
    }

    private void cleanGraphProperties() {
        try (Transaction tx = graphDatabase.beginTx()) {
            GraphPropertiesImpl props = graphDatabase.getDependencyResolver().resolveDependency(NodeManager.class).getGraphProperties();
            for (String key : asCollection(props.getPropertyKeys())) {
                props.removeProperty(key);
            }
            tx.success();
        }
    }

    public GraphDatabaseService getGraphDatabase() {
        return graphDatabase;
    }
}
