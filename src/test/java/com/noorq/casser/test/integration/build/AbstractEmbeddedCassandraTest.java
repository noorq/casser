/*
 *      Copyright (C) 2015 The Casser Authors
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
package com.noorq.casser.test.integration.build;

import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;

/**
 * AbstractEmbeddedCassandraTest
 * 
 * @author Alex Shvid
 *
 */

public abstract class AbstractEmbeddedCassandraTest {

	private static Cluster cluster;

	private static String keyspace = BuildProperties.getRandomKeyspace();

	private static Session session;

	private static boolean keep;
	
	public static boolean isConnected() {
		return session != null;
	}

	public static Cluster getCluster() {
		return cluster;
	}

	public static Session getSession() {
		return session;
	}
	
	public static String getKeyspace() {
		return keyspace;
	}
	
	public static void setKeep(boolean enable) {
		keep = enable;
	}
	
	@BeforeClass
	public static void before() throws Exception {
		EmbeddedCassandraServerHelper.startEmbeddedCassandra(BuildProperties.getCassandraConfig());
	
		cluster = Cluster.builder()
				.addContactPoint(BuildProperties.getCassandraHost())
				.withPort(BuildProperties.getCassandraNativePort())
				.build();

		KeyspaceMetadata kmd = cluster.getMetadata().getKeyspace(keyspace);
		if (kmd == null) { 
			session = cluster.connect();
			
			String cql = "CREATE KEYSPACE " + keyspace
			+ " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};";
			System.out.println(cql + "\n");
			session.execute(cql);
			
			cql = "USE " + keyspace + ";";
			System.out.println(cql + "\n");
			session.execute(cql);
		} else {
			session = cluster.connect(keyspace);
		}

	}

	@AfterClass
	public static void after() {
		if (!keep && isConnected()) {
			session.close();
			session = null;
			EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
			//EmbeddedCassandraServerHelper.stopEmbeddedCassandra();
		}
	}
}
