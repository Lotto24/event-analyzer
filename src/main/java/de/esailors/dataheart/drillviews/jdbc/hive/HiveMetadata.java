package de.esailors.dataheart.drillviews.jdbc.hive;

import java.util.Optional;

import de.esailors.dataheart.drillviews.conf.Config;
import de.esailors.dataheart.drillviews.data.EventType;
import de.esailors.dataheart.drillviews.jdbc.JdbcMetadata;

public class HiveMetadata extends JdbcMetadata {
	
	public HiveMetadata(HiveConnection hiveConnection) {
		super(hiveConnection, Config.getInstance().HIVE_VIEW_ALL_DATABASE,
			Config.getInstance().HIVE_VIEW_DAY_DATABASE, Config.getInstance().HIVE_VIEW_WEEK_DATABASE);
	}
	
	public Optional<Long> runDayCount(EventType eventType) {
		String database = Config.getInstance().HIVE_VIEW_DAY_DATABASE;
		return runDayCount(eventType, database);
	}
	
	public String viewNameFor(EventType eventType) {
		return eventType.getName().toLowerCase();
	}

}
