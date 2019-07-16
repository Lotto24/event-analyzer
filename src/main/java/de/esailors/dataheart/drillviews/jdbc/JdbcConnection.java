package de.esailors.dataheart.drillviews.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class JdbcConnection {

	private static final Logger log = LogManager.getLogger(JdbcConnection.class.getName());

	private String jdbcClass;
	private String jdbcUrl;
	private String jdbcUser;
	private String jdbcPassword;
	
	private Connection connection;

	public JdbcConnection(String jdbcClass, String jdbcUrl, String jdbcUser, String jdbcPassword) {
		this.jdbcClass = jdbcClass;
		this.jdbcUrl = jdbcUrl;
		this.jdbcUser = jdbcUser;
		this.jdbcPassword = jdbcPassword;
		connection = connect();
		initShutdownHook();
	}

	public void executeSqlStatements(String sql) throws SQLException {
		for (String singleStatement : sql.split(";")) {
			if (singleStatement.trim().isEmpty()) {
				continue;
			}
			log.debug("Executing statement: " + singleStatement);
			connection.createStatement().execute(singleStatement);
		}
	}

	public ResultSet query(String query) throws SQLException {
		log.debug("Running query: " + query);
		Statement statement = connection.createStatement();
		return statement.executeQuery(query);
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

	protected Set<String> resultSetToStringSet(ResultSet resultSet) {
		return resultSetToStringSet(resultSet, 0);
	}

	protected Set<String> resultSetToStringSet(ResultSet resultSet, int columnIndex) {
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

	private Connection connect() {
		try {
			log.info("Connecting to: " + jdbcUrl);
			Class.forName(jdbcClass);
			return DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
		} catch (Exception e) {
			log.error("Unable to open connection", e);
			throw new IllegalStateException("Unable to connect to " + jdbcUrl, e);
		}
	}

	public void close() {
		try {
			if (connection == null || connection.isClosed()) {
				log.warn("Attempted to close a connection that was not open");
				return;
			}
		} catch (SQLException e) {
			log.warn("Unable to check if connection is closed, skipping manual closing", e);
			return;
		}

		log.debug("Closing connection to " + jdbcUrl);

		try {
			connection.close();
		} catch (SQLException e) {
			log.error("Error while closing connection to " + jdbcUrl, e);
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
