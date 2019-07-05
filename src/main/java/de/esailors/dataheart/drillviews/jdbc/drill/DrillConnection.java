package de.esailors.dataheart.drillviews.jdbc.drill;

import java.sql.SQLException;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.esailors.dataheart.drillviews.conf.Config;
import de.esailors.dataheart.drillviews.jdbc.JdbcConnection;

public class DrillConnection extends JdbcConnection {

	private static final String DRILL_JDBC_DRIVER_CLASS = "org.apache.drill.jdbc.Driver";

	private static final Logger log = LogManager.getLogger(DrillConnection.class.getName());

	public DrillConnection() {
		super(DRILL_JDBC_DRIVER_CLASS, Config.getInstance().DRILL_JDBC_URL, Config.getInstance().DRILL_JDBC_USER, Config.getInstance().DRILL_JDBC_PASSWORD);
	}
	
	public Set<String> listDatabases() {
		try {
			return resultSetToStringSet(query("SHOW DATABASES"));
		} catch (SQLException e) {
			log.error("Error when listing Drill databases", e);
			throw new IllegalStateException("Unable to list databases", e);
		}
	}
	
	public Set<String> listTablesinDatabase(String database) {
		try {
			return resultSetToStringSet(query("SHOW TABLES FROM " + database), 1);
		} catch (SQLException e) {
			log.error("Error when listing Drill tables in database: " + database, e);
			throw new IllegalStateException("Unable to list tables in database: " + database, e);
		}
	}

}
