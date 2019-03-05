package de.esailors.dataheart.drillviews.drill;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.esailors.dataheart.drillviews.conf.Config;

public class DrillConnection {

	private static final String DRILL_JDBC_DRIVER_CLASS = "org.apache.drill.jdbc.Driver";

	private static final Logger log = LogManager.getLogger(DrillConnection.class.getName());

	private Connection connection;

	private Config config;

	public DrillConnection(Config config) {
		this.config = config;
		connection = connectToDrill();
		initShutdownHook();
	}
	
	public void executeSqlStatements(String sql) throws SQLException {
		for(String singleStatement : sql.split(";")) {
			if(singleStatement.trim().isEmpty()) {
				continue;
			}
			log.info("Executing statement: " + singleStatement);
			connection.createStatement().execute(singleStatement);
		}
	}
	
	public ResultSet query(String query) {
		log.debug("Running query: " + query);
		try {
			Statement statement = connection.createStatement();
			return statement.executeQuery(query);
		} catch (Exception e) {
			log.error("Unable to open connection to drill", e);
			throw new IllegalStateException("Unable to execute query: " + query, e);
		}
	}
	
	public Set<String> listDatabases() {
		return resultSetToStringSet(query("SHOW DATABASES"));
	}
	
	public Set<String> listTablesinDatabase(String database) {
		return resultSetToStringSet(query("SHOW TABLES FROM " + database), 1);
	}
	
	public void logResultSet(ResultSet resultSet) {
		try {
			while (resultSet.next()) {
				log.debug(resultSet.getString(1));
			}
		} catch (SQLException e) {
			log.error("Error while printing ResultSet", e);
		}		
	}
	
	private Set<String> resultSetToStringSet(ResultSet resultSet) {
		return resultSetToStringSet(resultSet, 0);
	}
	
	private Set<String> resultSetToStringSet(ResultSet resultSet, int columnIndex) {
		HashSet<String> r = new HashSet<String>();
		try {
			while (resultSet.next()) {
				r.add(resultSet.getString(columnIndex + 1)); // index starts at 1 in ResultSet
			}
		} catch (SQLException e) {
			throw new IllegalStateException("Error while transforming ResultSet", e);
		}
		
		return r;
	}

	private Connection connectToDrill() {
		try {
			log.info("Connecting to Drill at: " + config.DRILL_JDBC_URL);
			Class.forName(DRILL_JDBC_DRIVER_CLASS);
			return DriverManager.getConnection(config.DRILL_JDBC_URL);
		} catch (Exception e) {
			log.error("Unable to open connection to drill", e);
			throw new IllegalStateException("Unable to connect to drill", e);
		}
	}

	public void close() {
		try {
			if(connection == null || connection.isClosed()) {
				log.warn("Attempted to close a connection that was not open");
				return;
			}
		} catch (SQLException e) {
			log.warn("Unable to check if connection is closed, skipping manual closing", e);
			return;
		}
		
		log.debug("Closing connection to drill");
		
		try {
			connection.close();
		} catch (SQLException e) {
			log.error("Error while closing connection to Drill", e);
		}
	}
	
	private void initShutdownHook() {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				log.debug("Shutdown Hook triggered");
				close();
			}
		});
	}

}
