package de.esailors.dataheart.drillviews.jdbc.drill;

import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

import de.esailors.dataheart.drillviews.conf.Config;
import de.esailors.dataheart.drillviews.data.EventStructure;
import de.esailors.dataheart.drillviews.data.EventType;
import de.esailors.dataheart.drillviews.data.Node;

public class DrillViewSqlBuilder {

	private static final Logger log = LogManager.getLogger(DrillViewSqlBuilder.class.getName());

	// internal
	private static final String ROW_TIMESTAMP_ALIAS = "row_timestamp";
	private static final String SUBSELECT_ALIAS = "e";
	private static final String JSON_FIELD_ALIAS = "json";
	private static final int IDENTATION = 4;
	
	private DrillMetadata drillViews;

	public DrillViewSqlBuilder(DrillMetadata drillViews) {
		this.drillViews = drillViews;
	}

	public String generateDrillViewsFor(EventType eventType, EventStructure eventStructure) {

		log.debug("Generating create view statement for Drill from EventStructure from " + eventStructure.toString());

		String viewName = drillViews.viewNameFor(eventType);
		
		StringBuilder viewBuilder = new StringBuilder();

		generateDrillView(Config.getInstance().DRILL_VIEW_ALL_DATABASE, eventStructure, viewName, viewBuilder,
				Optional.empty());
		generateDrillView(Config.getInstance().DRILL_VIEW_DAY_DATABASE, eventStructure, viewName, viewBuilder,
				Optional.of("'-1' day"));
		generateDrillView(Config.getInstance().DRILL_VIEW_WEEK_DATABASE, eventStructure, viewName, viewBuilder,
				Optional.of("'-7' day"));

		return viewBuilder.toString();
	}

	private void generateDrillView(String drillDatabase, EventStructure eventStructure, String viewName,
			StringBuilder viewBuilder, Optional<String> timeLimit) {
		generateDrillViewStart(drillDatabase, viewBuilder, viewName);

		String fieldPrefix = SUBSELECT_ALIAS + "." + JSON_FIELD_ALIAS + ".";

		Node rootNode = eventStructure.getEventStructureTree().getRootNode();

		generateSelectColumns(rootNode, viewBuilder, fieldPrefix, "");

		generateDrillViewEnd(viewBuilder, eventStructure.getEventType().getName().toUpperCase(), timeLimit);
	}

	private void generateDrillViewStart(String drillDatabase, StringBuilder viewBuilder, String viewName) {
		viewBuilder.append("CREATE OR REPLACE VIEW ");
		viewBuilder.append(drillDatabase);
		viewBuilder.append(".`");
		viewBuilder.append(viewName);
		viewBuilder.append("` AS\n");
		viewBuilder.append("SELECT\n");
		viewBuilder.append(ident());
		viewBuilder.append("CONVERT_FROM(");
		viewBuilder.append(SUBSELECT_ALIAS);
		viewBuilder.append(".row_key, 'UTF8') as row_key,\n");
		viewBuilder.append(ident());
		viewBuilder.append("TO_TIMESTAMP(CAST(SUBSTR(CONVERT_FROM(");
		viewBuilder.append(SUBSELECT_ALIAS);
		viewBuilder.append(".row_key, 'UTF8'), STRPOS(CONVERT_FROM(");
		viewBuilder.append(SUBSELECT_ALIAS);
		viewBuilder.append(".row_key, 'UTF8'), '-') + 1, 10) AS BIGINT)) as ");
		viewBuilder.append(ROW_TIMESTAMP_ALIAS);
	}

	private void generateSelectColumns(Node node, StringBuilder viewBuilder, String fieldPrefix, String keyPrefix) {
		Set<Node> children = node.getChildren();
		for (Node child : children) {
			String nodeName = child.getName();
			if (child.hasChildren() && !child.hasArrayType()) {
				// recursion
				String newKeyPrefix = keyPrefix + nodeName + "_";
				String newFieldPrefix = fieldPrefix + "`" + nodeName + "`.";
				generateSelectColumns(child, viewBuilder, newFieldPrefix, newKeyPrefix);
			} else {
				viewBuilder.append(",\n");
				viewBuilder.append(ident());
				viewBuilder.append(fieldPrefix);
				viewBuilder.append("`");
				viewBuilder.append(nodeName);
				viewBuilder.append("` as `");
				viewBuilder.append(keyPrefix);
				viewBuilder.append(nodeName);
				viewBuilder.append("`");
			}
		}
	}

	private void generateDrillViewEnd(StringBuilder viewBuilder, String eventType, Optional<String> timeLimit) {
		viewBuilder.append("\nFROM (\n");
		viewBuilder.append(ident());
		viewBuilder.append("SELECT\n");
		viewBuilder.append(ident());
		viewBuilder.append(ident());
		viewBuilder.append("row_key,\n");
		viewBuilder.append(ident());
		viewBuilder.append(ident());
		viewBuilder.append("CONVERT_FROM(");
		viewBuilder.append(Config.getInstance().DRILL_VIEW_HBASE_TABLE);
		viewBuilder.append(".");
		viewBuilder.append(Config.getInstance().DRILL_VIEW_HBASE_COLUMN_FAMILY);
		viewBuilder.append(".");
		viewBuilder.append(Config.getInstance().DRILL_VIEW_HBASE_JSON_FIELD);
		viewBuilder.append(", 'JSON') AS ");
		viewBuilder.append(JSON_FIELD_ALIAS);
		viewBuilder.append("\n");
		viewBuilder.append(ident());
		viewBuilder.append("FROM\n");
		viewBuilder.append(ident());
		viewBuilder.append(ident());
		viewBuilder.append(Config.getInstance().DRILL_VIEW_HBASE_STORAGE_PLUGIN_NAME);
		viewBuilder.append(".");
		viewBuilder.append(Config.getInstance().DRILL_VIEW_HBASE_TABLE);
		viewBuilder.append("\n");
		viewBuilder.append(ident());
		viewBuilder.append("WHERE\n");
		viewBuilder.append(ident());
		viewBuilder.append(ident());
		viewBuilder.append("CONVERT_FROM(row_key, 'UTF8') BETWEEN '");
		viewBuilder.append(eventType);
		viewBuilder.append("-");
		generateDrillRowKeyStart(viewBuilder, timeLimit);
		viewBuilder.append(" AND '");
		viewBuilder.append(eventType);
		viewBuilder.append("-9'\n");
		viewBuilder.append(") ");
		viewBuilder.append(SUBSELECT_ALIAS);
		viewBuilder.append(";\n");
	}

	private void generateDrillRowKeyStart(StringBuilder viewBuilder, Optional<String> timeLimit) {
		String rowKeyStart;
		if (timeLimit == null || !timeLimit.isPresent()) {
			rowKeyStart = "0'";
		} else {
			rowKeyStart = "' || UNIX_TIMESTAMP(TO_CHAR(DATE_ADD(now(), interval " + timeLimit.get()
					+ "),'yyyy-MM-dd HH:mm:ss'))";
		}

		viewBuilder.append(rowKeyStart);
	}

	private static String ident() {
		StringBuilder r = new StringBuilder();
		for (int i = 0; i < IDENTATION; i++) {
			r.append(" ");
		}
		return r.toString();
	}

}
