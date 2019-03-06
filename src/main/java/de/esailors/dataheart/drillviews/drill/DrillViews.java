package de.esailors.dataheart.drillviews.drill;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.esailors.dataheart.drillviews.conf.Config;

public class DrillViews {

	private static final Logger log = LogManager.getLogger(DrillViews.class.getName());

	private Config config;
	private DrillConnection drillConnection;

	private String[] targetDatabases;
	private Set<String> databases;
	private Map<String, Set<String>> existingTables;
	

	public DrillViews(Config config, DrillConnection drillConnection) {
		log.debug("Initializing DrillViewGenerator");
		this.config = config;
		this.drillConnection = drillConnection;

		fetchDatabases();
		initTargetDatabases();
		fetchViewsInTargetDatabses();
	}
	
	public boolean doesViewExist(String viewName) {
		log.debug("Checking if view exists already in Drill: " + viewName);
		for(String database : existingTables.keySet()) {
			if(existingTables.get(database).contains(viewName)) {
				log.debug("Found view " + viewName + "in database: " + database);
				return true;
			}
		}
		log.info("View does not exist yet: " + viewName);
		return false;
	}
	
	private void fetchDatabases() {
		log.debug("Fetching databases from Drill");
		databases = drillConnection.listDatabases();
		for (String database : databases) {
			log.debug("Found Database: " + database);
		}
	}

	private void fetchViewsInTargetDatabses() {
		existingTables = new HashMap<>();
		for (String targetDatabase : targetDatabases) {
			existingTables.put(targetDatabase, fetchTablesFromTargetDatabase(targetDatabase));
		}
	}

	private Set<String> fetchTablesFromTargetDatabase(String targetDatabase) {
		log.debug("Fetching views in " + targetDatabase);
		Set<String> tables = drillConnection.listTablesinDatabase(targetDatabase);
		
		for(String table : tables) {
			log.debug("Found table: " + table);
		}
		
		return tables;
	}

	private void initTargetDatabases() {
		String[] targetDatabases = { config.DRILL_VIEW_ALL_DATABASE, config.DRILL_VIEW_DAY_DATABASE,
				config.DRILL_VIEW_WEEK_DATABASE };
		this.targetDatabases = targetDatabases;
		ensureTargetDatabasesExist();
	}
	
	private void ensureTargetDatabasesExist() {
		for (String targetDatabase : targetDatabases) {
			ensureDatabaseExists(targetDatabase);
		}
	}

	private void ensureDatabaseExists(String database) {
		log.debug("Making sure database exists: " + database);
		if (!databases.contains(database)) {
			throw new IllegalStateException("Did not find required databse in drill: " + database);
		}
	}

}
