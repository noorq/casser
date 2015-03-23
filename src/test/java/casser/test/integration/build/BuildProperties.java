/*
 *      Copyright (C) 2015 Noorq Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package casser.test.integration.build;

import java.io.InputStream;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.io.IOUtils;


public final class BuildProperties {

	private final static class Singleton {
		private final static BuildProperties INSTANCE = new BuildProperties();
	}

	private final String cassandraConfig;
	private final Properties props = new Properties();

	private BuildProperties() {
		
		if (isRunInMaven()) {
			this.cassandraConfig = "build-cassandra.yaml";
			loadFromClasspath("build.properties");			
		}
		else {
			this.cassandraConfig = "eclipse-cassandra.yaml";
			loadFromClasspath("eclipse.properties");
		}
	}
	
	private boolean isRunInMaven() {
		return System.getProperty("maven.integration.test") != null;
	}
	
	private void loadFromClasspath(String resourceName) {
		InputStream in = getClass().getResourceAsStream("/" + resourceName);
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
		return Singleton.INSTANCE.cassandraConfig;
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
