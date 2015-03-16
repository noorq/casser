package casser.test.integration.build;

import java.io.IOException;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;

public abstract class AbstractEmbeddedCassandraTest {

	private Cluster cluster;

	private String keyspace = BuildProperties.getRandomKeyspace();

	private Session session;

	private boolean keep;
	
	public AbstractEmbeddedCassandraTest() {
		this(true);
	}
	
	public AbstractEmbeddedCassandraTest(boolean keep) {
		this.keep = keep;
	}
	
	@BeforeClass
	public static void beforeClass() throws ConfigurationException, TTransportException, IOException,
			InterruptedException {
		EmbeddedCassandraServerHelper.startEmbeddedCassandra(BuildProperties.getCassandraConfig());
	}
	
	
	public boolean isConnected() {
		return session != null;
	}

	public Cluster getCluster() {
		return cluster;
	}

	public Session getSession() {
		return session;
	}
	
	public String getKeyspace() {
		return keyspace;
	}
	
	@Before
	public void before() {
		if (!isConnected()) {
			
			cluster = Cluster.builder()
					.addContactPoint(BuildProperties.getCassandraHost())
					.withPort(BuildProperties.getCassandraNativePort())
					.build();

			KeyspaceMetadata kmd = cluster.getMetadata().getKeyspace(keyspace);
			if (kmd == null) { 
				session = cluster.connect();
				session.execute("CREATE KEYSPACE " + keyspace
						+ " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};");
				session.execute("USE " + keyspace + ";");
			} else {
				session = cluster.connect(keyspace);
			}
			
		}
	}

	@After
	public void after() {
		if (!keep && isConnected()) {
			session.close();
			EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
		}
	}
}
