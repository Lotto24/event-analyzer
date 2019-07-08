package de.esailors.dataheart.drillviews.jdbc.hive;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Optional;

import de.esailors.dataheart.drillviews.conf.Config;
import de.esailors.dataheart.drillviews.data.EventType;

public class HiveViews {
	
	private static final Logger log = LogManager.getLogger(HiveViews.class.getName());
	

	// in hive there is only one default database
	private static final String DATABASE = "default";
	private static final String COUNT_COLUMN_ALIAS = "cnt";
	
	private static final String LAST_DAY_VIEW_SUFFX = "_last_day";
	private static final String LAST_WEEK_VIEW_SUFFX = "_last_week";

	private HiveConnection hiveConnection;
	private Set<String> existingTables;

	public HiveViews(HiveConnection hiveConnection) {
		this.hiveConnection = hiveConnection;
		fetchExistingTables();
	}
	
	public Optional<Long> runDayCount(EventType eventType) {
		String viewName = viewNameLastDayFor(eventType);
		log.info("Running day count on hive on: " + viewName);
		String countQuery = "SELECT COUNT(*) as " + COUNT_COLUMN_ALIAS + " FROM " + viewName;
		ResultSet resultSet = null;
		try {
			resultSet = hiveConnection.query(countQuery);
			if (!resultSet.next()) {
				log.error("Unable to fetch first row of resultSet after count query: " + countQuery);
				return Optional.absent();
			}
			return Optional.of(resultSet.getLong(COUNT_COLUMN_ALIAS));
		} catch (SQLException e) {
			log.error("Unexpected SQLException after running day count query " + countQuery, e);
			return Optional.absent();
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
	
	public String viewNameFor(EventType eventType) {
		return Config.getInstance().HIVE_VIEW_NAME_PREFIX.toLowerCase() + eventType.getName().toLowerCase();
	}
	
	public String viewNameLastDayFor(EventType eventType) {
		return viewNameFor(eventType) + LAST_DAY_VIEW_SUFFX;
	}
	
	public String viewNameLastWeekFor(EventType eventType) {
		return viewNameFor(eventType) + LAST_WEEK_VIEW_SUFFX;
	}

	public boolean doesViewExist(EventType eventType) {
		return doesViewExist(viewNameFor(eventType));
	}

	private boolean doesViewExist(String viewName) {
		return existingTables.contains(viewName);
	}

	private void fetchExistingTables() {
		existingTables = hiveConnection.listTablesinDatabase(DATABASE);
		log.info("Found existing tables in hive:");
		for(String existingTable : existingTables) {
			log.debug(" * " + existingTable);
		}
	}
}
