package de.esailors.dataheart.drillviews.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

import de.esailors.dataheart.drillviews.data.EventType;

public abstract class JdbcMetadata {

	private static final Logger log = LogManager.getLogger(JdbcMetadata.class.getName());
	
	private static final String COUNT_COLUMN_ALIAS = "cnt";

	protected JdbcConnection jdbcConnection;

	protected String[] targetDatabases;
	protected Set<String> databases;
	protected Map<String, Set<String>> existingTables;

	public JdbcMetadata(JdbcConnection jdbcConnection, String... targetDatabases) {
		this.jdbcConnection = jdbcConnection;
		this.targetDatabases = targetDatabases;
		fetchDatabases();
		ensureTargetDatabasesExist();
		fetchViewsInTargetDatabses();
	}

	private void fetchDatabases() {
		log.debug("Fetching databases");
		databases = jdbcConnection.listDatabases();
		for (String database : databases) {
			log.debug("Found Database: " + database);
		}
	}

	protected Optional<Long> runDayCount(EventType eventType, String database) {
		String viewName = viewNameFor(eventType);
		log.info("Running day count on: " + viewName);
		String countQuery = "SELECT COUNT(*) as " + COUNT_COLUMN_ALIAS + " FROM " + database + "." + viewName;
		ResultSet resultSet = null;
		try {
			resultSet = jdbcConnection.query(countQuery);
			if (!resultSet.next()) {
				log.error("Unable to fetch first row of resultSet after count query: " + countQuery);
				return Optional.empty();
			}
			return Optional.of(resultSet.getLong(COUNT_COLUMN_ALIAS));
		} catch (SQLException e) {
			log.error("Unexpected SQLException after running day count query " + countQuery, e);
			return Optional.empty();
		} finally {
			try {
				if (resultSet != null) {
					resultSet.close();
				}
			} catch (SQLException e) {
				log.warn("Unable to close ResultSet after day count query", e);
			}
		}
	}

	public abstract String viewNameFor(EventType eventType);

	public boolean doesViewExist(EventType eventType) {
		return doesViewExist(viewNameFor(eventType));
	}

	public boolean doesViewExist(String viewName) {
		log.debug("Checking if view exists already: " + viewName);
		for (String database : existingTables.keySet()) {
			if (existingTables.get(database).contains(viewName)) {
				log.debug("Found view " + viewName + "in database: " + database);
				return true;
			}
		}
		log.info("View does not exist yet: " + viewName);
		return false;
	}

	private void fetchViewsInTargetDatabses() {
		existingTables = new HashMap<>();
		for (String targetDatabase : targetDatabases) {
			existingTables.put(targetDatabase, fetchTablesFromTargetDatabase(targetDatabase));
		}
	}

	private Set<String> fetchTablesFromTargetDatabase(String targetDatabase) {
		log.debug("Fetching views in " + targetDatabase);
		Set<String> tables = jdbcConnection.listTablesinDatabase(targetDatabase);

		for (String table : tables) {
			log.debug("Found table: " + table);
		}

		return tables;
	}

	private void ensureTargetDatabasesExist() {
		for (String targetDatabase : targetDatabases) {
			ensureDatabaseExists(targetDatabase);
		}
	}

	private void ensureDatabaseExists(String database) {
		log.debug("Making sure database exists: " + database);
		if (!databases.contains(database)) {
			throw new IllegalStateException("Did not find required databse: " + database);
		}
	}
}
