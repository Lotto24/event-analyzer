package de.esailors.dataheart.drillviews.generator;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.esailors.dataheart.drillviews.conf.Config;
import de.esailors.dataheart.drillviews.drill.DrillConnection;

public class DrillViewGenerator {

	private static final Logger log = LogManager.getLogger(DrillViewGenerator.class.getName());

	private Config config;
	private DrillConnection drillConnection;

	private String[] targetDatabases;
	private Set<String> databases;
	private Map<String, Set<String>> existingTables;
	

	public DrillViewGenerator(Config config) {
		log.debug("Initializing DrillViewGenerator");
		this.config = config;
		this.drillConnection = new DrillConnection(config);

		fetchDatabases();

		initTargetDatabases();

		fetchViewsInTargetDatabses();
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

	private void fetchDatabases() {
		log.info("Fetching databases from Drill");
		databases = drillConnection.listDatabases();
		for (String database : databases) {
			log.debug("Found Database: " + database);
		}
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
