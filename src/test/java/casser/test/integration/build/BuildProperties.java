package casser.test.integration.build;

import java.io.InputStream;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.io.IOUtils;


public final class BuildProperties {

	private final static class Singleton {
		private final static BuildProperties INSTANCE = new BuildProperties();
	}

	private final Properties props = new Properties();

	private BuildProperties() {
		loadFromClasspath("/build.properties");
	}
	
	private void loadFromClasspath(String resourceName) {
		InputStream in = getClass().getResourceAsStream(resourceName);
		if (in == null) {
			throw new RuntimeException("resource is not found in classpath: " + resourceName);
		}
		try {
			props.load(in);
		} catch (Exception x) {
			throw new RuntimeException(x);
		} finally {
			IOUtils.closeQuietly(in);
		}
	}
	
	public static String getCassandraConfig() {
		return "build-cassandra.yaml";
	}
	
	public static String getCassandraHost() {
		return "localhost";
	}
	
	public static String getRandomKeyspace() {
		return "test" + UUID.randomUUID().toString().replace("-", "");
	}
	
	public static int getCassandraNativePort() {
		return Singleton.INSTANCE.getInt("build.cassandra.native_transport_port");
	}

	public static int getCassandraRpcPort() {
		return Singleton.INSTANCE.getInt("build.cassandra.rpc_port");
	}

	public static int getCassandraStoragePort() {
		return Singleton.INSTANCE.getInt("build.cassandra.storage_port");
	}

	public static int getCassandraSslStoragePort() {
		return Singleton.INSTANCE.getInt("build.cassandra.ssl_storage_port");
	}

	private int getInt(String key) {
		return Integer.parseInt(props.getProperty(key));
	}

}
