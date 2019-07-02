package de.esailors.dataheart.drillviews.processor;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.esailors.dataheart.drillviews.data.EventStructure;
import de.esailors.dataheart.drillviews.data.Node;
import oadd.com.google.common.base.Optional;

public class HiveViewSqlBuilder {

	private static final Logger log = LogManager.getLogger(HiveViewSqlBuilder.class.getName());

	private static final String HIVE_HBASE_TABLE = "hbase_kafka_events";
	private static final String HIVE_HBASE_TABLE_ROWKEY_COLUMN = "rowkey";
	private static final String HIVE_HBASE_TABLE_JSON_COLUMN = "json";
	private static final String HIVE_HBASE_TABLE_ALIAS = "k";
	private static final String HIVE_ROOT_LATERAL_VIEW_ALIAS = "r";

	private static final int IDENTATION = 4;

	public HiveViewSqlBuilder() {
	}

	public String generateHiveViewsFor(String viewName, EventStructure eventStructure) {
		log.debug("Generating create view statement for Hive from EventStructure from " + eventStructure.toString());

		StringBuilder viewBuilder = new StringBuilder();
		Node node = eventStructure.getEventStructureTree().getRootNode();

		generateHiveViewFor(viewName, eventStructure, viewBuilder, node, Optional.absent());
		generateHiveViewFor(viewName + "_last_day", eventStructure, viewBuilder, node, Optional.of(86400));
		generateHiveViewFor(viewName + "_last_week", eventStructure, viewBuilder, node, Optional.of(604800));

		return viewBuilder.toString();
	}

	private void generateHiveViewFor(String viewName, EventStructure eventStructure, StringBuilder viewBuilder,
			Node node, Optional<Integer> timeLimit) {
		generateHiveViewStart(viewBuilder, viewName);
		generateHiveViewSelectColumns(viewBuilder, node, HIVE_ROOT_LATERAL_VIEW_ALIAS, true);
		generateHiveViewFromClause(viewBuilder);
		genreateLateralViews(viewBuilder, node, HIVE_ROOT_LATERAL_VIEW_ALIAS, HIVE_HBASE_TABLE_ALIAS,
				HIVE_HBASE_TABLE_JSON_COLUMN);
		generateHiveViewEnd(eventStructure, viewBuilder, timeLimit);
	}

	private void generateHiveViewSelectColumns(StringBuilder viewBuilder, Node node, String lateralViewAlias,
			boolean topLevel) {
		// need to make sure children are ordered in the same way
		List<Node> children = new ArrayList<>(node.getChildren());
		// TODO would be nice to have some columns at the beginning like site,
		// customerNumber and timestamp
		List<Node> nestedChildren = new ArrayList<>();
		for (Node child : children) {
			if (child.getChildren().isEmpty()) {
				viewBuilder.append(",\n");
				viewBuilder.append(ident());
				viewBuilder.append(lateralViewAlias);
				viewBuilder.append(".`");
				viewBuilder.append(child.getName());
				viewBuilder.append("`");
				if (!topLevel) {
					viewBuilder.append(" as ");
					viewBuilder.append(lateralViewAlias.substring(HIVE_ROOT_LATERAL_VIEW_ALIAS.length() + 1));
					viewBuilder.append("_");
					viewBuilder.append(child.getName());
				}
			} else {
				nestedChildren.add(child);
			}
		}

		for (Node nestedChild : nestedChildren) {
			String newLateralViewAlias = newLateralViewAlias(lateralViewAlias, nestedChild);
			generateHiveViewSelectColumns(viewBuilder, nestedChild, newLateralViewAlias, false);
		}
	}

	private void generateHiveViewFromClause(StringBuilder viewBuilder) {
		viewBuilder.append("\nFROM ");
		viewBuilder.append(HIVE_HBASE_TABLE);
		viewBuilder.append(" ");
		viewBuilder.append(HIVE_HBASE_TABLE_ALIAS);
		viewBuilder.append("\n");
	}

	private void genreateLateralViews(StringBuilder viewBuilder, Node node, String lateralViewAlias, String nodeAlias,
			String nodeColumn) {

		// need to make sure children are ordered in the same way
		List<Node> children = new ArrayList<>(node.getChildren());

		viewBuilder.append("LATERAL VIEW json_tuple(");
		viewBuilder.append(nodeAlias);
		viewBuilder.append(".`");
		viewBuilder.append(nodeColumn);
		viewBuilder.append("`");

		List<Node> nestedNodes = new ArrayList<>();
		for (Node child : children) {
			viewBuilder.append(", '");
			viewBuilder.append(child.getName());
			viewBuilder.append("'");
			if (!child.getChildren().isEmpty()) {
				nestedNodes.add(child);
			}
		}
		viewBuilder.append(") ");
		viewBuilder.append(lateralViewAlias);
		viewBuilder.append(" AS ");
		boolean first = true;
		for (Node child : children) {
			if (first) {
				first = false;
			} else {
				viewBuilder.append(", ");
			}
			viewBuilder.append("`");
			viewBuilder.append(child.getName());
			viewBuilder.append("`");
		}
		viewBuilder.append("\n");

		for (Node nestedNode : nestedNodes) {
			String newLateralViewAlias = newLateralViewAlias(lateralViewAlias, nestedNode);
			genreateLateralViews(viewBuilder, nestedNode, newLateralViewAlias, lateralViewAlias, nestedNode.getName());
		}
	}

	private String newLateralViewAlias(String lateralViewAlias, Node nestedNode) {
		// need to make sure aliases are unique
		return lateralViewAlias + "_" + nestedNode.getName();
	}

	private void generateHiveViewStart(StringBuilder viewBuilder, String viewName) {
		viewBuilder.append("CREATE OR REPLACE VIEW ");
		viewBuilder.append(viewName);
		viewBuilder.append(" AS \nSELECT\n");
		viewBuilder.append(ident());
		viewBuilder.append(HIVE_HBASE_TABLE_ALIAS);
		viewBuilder.append(".");
		viewBuilder.append(HIVE_HBASE_TABLE_ROWKEY_COLUMN);
	}

	private void generateHiveViewEnd(EventStructure eventStructure, StringBuilder viewBuilder,
			Optional<Integer> timeLimit) {
		viewBuilder.append("WHERE ");
		viewBuilder.append(HIVE_HBASE_TABLE_ROWKEY_COLUMN);
		viewBuilder.append(" > ");
		viewBuilder.append("'");
		viewBuilder.append(eventStructure.getEventType().getName().toUpperCase());
		generateHiveRowKeyStart(viewBuilder, timeLimit);
		viewBuilder.append(" AND ");
		viewBuilder.append(HIVE_HBASE_TABLE_ROWKEY_COLUMN);
		viewBuilder.append(" < ");
		viewBuilder.append("'");
		viewBuilder.append(eventStructure.getEventType().getName().toUpperCase());
		viewBuilder.append("-9';\n");
	}

	private void generateHiveRowKeyStart(StringBuilder viewBuilder, Optional<Integer> timeLimit) {
		String rowKeyStart;
		if (timeLimit == null || !timeLimit.isPresent()) {
			rowKeyStart = "-0'";
		} else {
			rowKeyStart = "-' || (unix_timestamp() - " + timeLimit.get() + ")";
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
