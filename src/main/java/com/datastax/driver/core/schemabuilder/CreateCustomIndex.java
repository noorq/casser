package com.datastax.driver.core.schemabuilder;

import static com.datastax.driver.core.schemabuilder.SchemaStatement.*;

import com.google.common.base.Optional;

public class CreateCustomIndex extends CreateIndex {

	private String indexName;
	private boolean ifNotExists = false;
	private Optional<String> keyspaceName = Optional.absent();
	private String tableName;
	private String columnName;
	private boolean keys;

	CreateCustomIndex(String indexName) {
		super(indexName);
		validateNotEmpty(indexName, "Index name");
		validateNotKeyWord(indexName,
				String.format("The index name '%s' is not allowed because it is a reserved keyword", indexName));
		this.indexName = indexName;
	}

	/**
	 * Add the 'IF NOT EXISTS' condition to this CREATE INDEX statement.
	 *
	 * @return this CREATE INDEX statement.
	 */
	public CreateIndex ifNotExists() {
		this.ifNotExists = true;
		return this;
	}

	/**
	 * Specify the keyspace and table to create the index on.
	 *
	 * @param keyspaceName
	 *            the keyspace name.
	 * @param tableName
	 *            the table name.
	 * @return a {@link CreateIndex.CreateIndexOn} that will allow the specification
	 *         of the column.
	 */
	public CreateIndex.CreateIndexOn onTable(String keyspaceName, String tableName) {
		validateNotEmpty(keyspaceName, "Keyspace name");
		validateNotEmpty(tableName, "Table name");
		validateNotKeyWord(keyspaceName,
				String.format("The keyspace name '%s' is not allowed because it is a reserved keyword", keyspaceName));
		validateNotKeyWord(tableName,
				String.format("The table name '%s' is not allowed because it is a reserved keyword", tableName));
		this.keyspaceName = Optional.fromNullable(keyspaceName);
		this.tableName = tableName;
		return new CreateCustomIndex.CreateIndexOn();
	}

	/**
	 * Specify the table to create the index on.
	 *
	 * @param tableName
	 *            the table name.
	 * @return a {@link CreateIndex.CreateIndexOn} that will allow the specification
	 *         of the column.
	 */
	public CreateIndex.CreateIndexOn onTable(String tableName) {
		validateNotEmpty(tableName, "Table name");
		validateNotKeyWord(tableName,
				String.format("The table name '%s' is not allowed because it is a reserved keyword", tableName));
		this.tableName = tableName;
		return new CreateCustomIndex.CreateIndexOn();
	}

	public class CreateIndexOn extends CreateIndex.CreateIndexOn {
		/**
		 * Specify the column to create the index on.
		 *
		 * @param columnName
		 *            the column name.
		 * @return the final CREATE INDEX statement.
		 */
		public SchemaStatement andColumn(String columnName) {
			validateNotEmpty(columnName, "Column name");
			validateNotKeyWord(columnName,
					String.format("The column name '%s' is not allowed because it is a reserved keyword", columnName));
			CreateCustomIndex.this.columnName = columnName;
			return SchemaStatement.fromQueryString(buildInternal());
		}

		/**
		 * Create an index on the keys of the given map column.
		 *
		 * @param columnName
		 *            the column name.
		 * @return the final CREATE INDEX statement.
		 */
		public SchemaStatement andKeysOfColumn(String columnName) {
			validateNotEmpty(columnName, "Column name");
			validateNotKeyWord(columnName,
					String.format("The column name '%s' is not allowed because it is a reserved keyword", columnName));
			CreateCustomIndex.this.columnName = columnName;
			CreateCustomIndex.this.keys = true;
			return SchemaStatement.fromQueryString(buildInternal());
		}
	}

	String getCustomClassName() {
		return "";
	}

	String getOptions() {
		return "";
	}

	@Override
	public String buildInternal() {
		StringBuilder createStatement = new StringBuilder(STATEMENT_START).append("CREATE CUSTOM INDEX ");

		if (ifNotExists) {
			createStatement.append("IF NOT EXISTS ");
		}

		createStatement.append(indexName).append(" ON ");

		if (keyspaceName.isPresent()) {
			createStatement.append(keyspaceName.get()).append(".");
		}
		createStatement.append(tableName);

		createStatement.append("(");
		if (keys) {
			createStatement.append("KEYS(");
		}

		createStatement.append(columnName);

		if (keys) {
			createStatement.append(")");
		}
		createStatement.append(")");

		createStatement.append(" USING '");
		createStatement.append(getCustomClassName());
		createStatement.append("' WITH OPTIONS = {");
		createStatement.append(getOptions());
		createStatement.append(" }");

		return createStatement.toString();
	}
}
