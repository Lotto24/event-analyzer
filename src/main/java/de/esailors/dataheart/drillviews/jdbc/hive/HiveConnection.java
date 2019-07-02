package de.esailors.dataheart.drillviews.jdbc.hive;

import java.sql.SQLException;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.esailors.dataheart.drillviews.conf.Config;
import de.esailors.dataheart.drillviews.jdbc.JdbcConnection;

public class HiveConnection extends JdbcConnection {

	private static final String HIVE_JDBC_DRIVER_CLASS = "org.apache.hive.jdbc.HiveDriver";

	private static final Logger log = LogManager.getLogger(HiveConnection.class.getName());

	public HiveConnection() {
		super(HIVE_JDBC_DRIVER_CLASS, Config.getInstance().HIVE_JDBC_URL);
	}
	
	public Set<String> listDatabases() {
		try {
			return resultSetToStringSet(query("SHOW DATABASES"));
		} catch (SQLException e) {
			log.error("Error when listing Hive databases", e);
			throw new IllegalStateException("Unable to list databases", e);
		}
	}
	
	public Set<String> listTablesinDatabase(String database) {
		try {
			return resultSetToStringSet(query("SHOW TABLES FROM " + database), 0);
		} catch (SQLException e) {
			log.error("Error when listing Hive tables in database: " + database, e);
			throw new IllegalStateException("Unable to list tables in database: " + database, e);
		}
	}

}
