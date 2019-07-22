package de.esailors.dataheart.drillviews.jdbc.drill;

import com.google.common.base.Optional;

import de.esailors.dataheart.drillviews.conf.Config;
import de.esailors.dataheart.drillviews.data.EventType;
import de.esailors.dataheart.drillviews.jdbc.JdbcMetadata;

public class DrillMetadata extends JdbcMetadata {

	public DrillMetadata(DrillConnection drillConnection) {
		super(drillConnection, Config.getInstance().DRILL_VIEW_ALL_DATABASE,
				Config.getInstance().DRILL_VIEW_DAY_DATABASE, Config.getInstance().DRILL_VIEW_WEEK_DATABASE);
	}

	public Optional<Long> runDayCount(EventType eventType) {
		String database = Config.getInstance().DRILL_VIEW_DAY_DATABASE;
		return runDayCount(eventType, database);
	}

	public String viewNameFor(EventType eventType) {
		return eventType.getName();
	}
	

}
