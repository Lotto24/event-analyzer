package de.esailors.dataheart.drillviews.jdbc.hive;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.esailors.dataheart.drillviews.conf.Config;
import de.esailors.dataheart.drillviews.data.EventStructure;
import de.esailors.dataheart.drillviews.data.EventType;
import de.esailors.dataheart.drillviews.data.Node;
import de.esailors.dataheart.drillviews.util.CollectionUtil;
import oadd.com.google.common.base.Optional;

public class HiveViewSqlBuilder {

	private static final Logger log = LogManager.getLogger(HiveViewSqlBuilder.class.getName());

	private static final String HIVE_HBASE_TABLE = "hbase_kafka_events";
	private static final String HIVE_HBASE_TABLE_ROWKEY_COLUMN = "rowkey";
	private static final String HIVE_HBASE_TABLE_JSON_COLUMN = "json";
	private static final String HIVE_HBASE_TABLE_ALIAS = "k";
	private static final String HIVE_ROOT_LATERAL_VIEW_ALIAS = "r";
	private static final String ARRAY_ITEM_ALIAS = "item";
	private static final String ARRAY_INDEX_ALIAS = "index";

	private static final int IDENTATION = 4;


	private HiveMetadata hiveViews;
	private HiveComplexTypeGenerator hiveComplexTypeGenerator; 

	public HiveViewSqlBuilder(HiveMetadata hiveViews) {
		this.hiveViews = hiveViews;
		this.hiveComplexTypeGenerator = new HiveComplexTypeGenerator();
	}

	public String generateHiveViewsFor(EventType eventType, EventStructure eventStructure) {
		log.debug("Generating create view statement for Hive from EventStructure from " + eventStructure.toString());

		StringBuilder viewBuilder = new StringBuilder();
		Node node = eventStructure.getEventStructureTree().getRootNode();
		
		String hiveType = hiveComplexTypeGenerator.generateComplexTypeFor(node);
		System.out.println(hiveType);

		String viewName = hiveViews.viewNameFor(eventType);
		generateHiveViewFor(Config.getInstance().HIVE_VIEW_ALL_DATABASE, viewName, eventStructure, viewBuilder, node, Optional.absent());
		generateHiveViewFor(Config.getInstance().HIVE_VIEW_DAY_DATABASE, viewName, eventStructure, viewBuilder, node, Optional.of(86400));
		generateHiveViewFor(Config.getInstance().HIVE_VIEW_WEEK_DATABASE, viewName, eventStructure, viewBuilder, node, Optional.of(604800));

		return viewBuilder.toString();
	}

	private void generateHiveViewFor(String database, String viewName, EventStructure eventStructure, StringBuilder viewBuilder,
			Node node, Optional<Integer> timeLimit) {
		generateHiveViewStart(viewBuilder, database, viewName);
		generateHiveViewSelectColumns(viewBuilder, node, HIVE_ROOT_LATERAL_VIEW_ALIAS, true);
		generateHiveViewFromClause(viewBuilder);
		genreateLateralViews(viewBuilder, node, HIVE_ROOT_LATERAL_VIEW_ALIAS, HIVE_HBASE_TABLE_ALIAS,
				HIVE_HBASE_TABLE_JSON_COLUMN);
		generateHiveViewEnd(eventStructure, viewBuilder, timeLimit);
	}

	private void generateHiveViewSelectColumns(StringBuilder viewBuilder, Node node, String lateralViewAlias,
			boolean topLevel) {
		// TODO would be nice to have some special columns at the beginning like site,
		// customerNumber and timestamp

		// TODO ^ maybe write some utility that orders like that and reuse for drill
		// views

		// TODO handle ARRAYs

		// need to make sure children are ordered in the same way
		Map<String, Node> childMap = node.getChildMap();
		List<Node> nestedChildren = new ArrayList<>();
		for (String childPath : CollectionUtil.toSortedList(childMap.keySet())) {
			Node child = childMap.get(childPath);
			if (child.hasArrayType()) {
				String arrayLateralViewAlias = arrayLateralViewAlias(child);
				
				// array index
				viewBuilder.append(",\n");
				viewBuilder.append(ident());
				viewBuilder.append(arrayLateralViewAlias);
				viewBuilder.append(".`");
				viewBuilder.append(ARRAY_INDEX_ALIAS);
				viewBuilder.append("` as `");
				viewBuilder.append(child.getName());
				viewBuilder.append("_array_index`");

				Map<String, Node> arrayChildMap = child.getChildMap();
				for (String arrayChildPath : CollectionUtil.toSortedList(arrayChildMap.keySet())) {
					Node arrayChild = arrayChildMap.get(arrayChildPath);
					viewBuilder.append(",\n");
					viewBuilder.append(ident());
					viewBuilder.append(arrayLateralViewAlias);
					viewBuilder.append(".`");
					viewBuilder.append(ARRAY_ITEM_ALIAS);
					viewBuilder.append("`['");
					viewBuilder.append(arrayChild.getName());
					viewBuilder.append("'] as `");
					viewBuilder.append(child.getName());
					viewBuilder.append("_");
					viewBuilder.append(arrayChild.getName());
					viewBuilder.append("`");
				}

			} else if (child.getChildren().isEmpty()) {
				viewBuilder.append(",\n");
				viewBuilder.append(ident());
				viewBuilder.append(lateralViewAlias);
				viewBuilder.append(".`");
				viewBuilder.append(child.getName());
				viewBuilder.append("`");
				if (!topLevel) {
					viewBuilder.append(" as `");
					viewBuilder.append(lateralViewAlias.substring(HIVE_ROOT_LATERAL_VIEW_ALIAS.length() + 1));
					viewBuilder.append("_");
					viewBuilder.append(child.getName());
					viewBuilder.append("`");
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

		if (node.getChildren().isEmpty() && !node.hasArrayType()) {
			throw new IllegalArgumentException(
					"Unable to generate lateral views for non-nested non-array node: " + node.getId());
		}
		if (node.hasArrayType()) {
			gernateLateralViewForArrayNode(viewBuilder, node, lateralViewAlias, nodeAlias, nodeColumn);
		} else {
			generateLaterViewForNestedNode(viewBuilder, node, lateralViewAlias, nodeAlias, nodeColumn);
		}
	}

	private void generateLaterViewForNestedNode(StringBuilder viewBuilder, Node node, String lateralViewAlias,
			String nodeAlias, String nodeColumn) {

		if (node.getChildren().isEmpty()) {
			throw new IllegalArgumentException(
					"Unable to generate nested lateral view for non-nested node: " + node.getId());
		}

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

	private void gernateLateralViewForArrayNode(StringBuilder viewBuilder, Node node, String lateralViewAlias,
			String nodeAlias, String nodeColumn) {
		if (!node.hasArrayType()) {
			throw new IllegalArgumentException("Unable to generate array lateral view non-array node: " + node.getId());
		}

		viewBuilder.append("LATERAL VIEW OUTER posexplode(from_json(");
		viewBuilder.append(nodeAlias);
		viewBuilder.append(".`");
		viewBuilder.append(nodeColumn);
		// TODO this is assuming the array contains json objects, could also be a
		// primitive
		viewBuilder.append("`, 'array<map<string,string>>')) ");
		viewBuilder.append(arrayLateralViewAlias(node));
		viewBuilder.append(" as ");
		viewBuilder.append(ARRAY_INDEX_ALIAS);
		viewBuilder.append(", ");
		viewBuilder.append(ARRAY_ITEM_ALIAS);
		viewBuilder.append("\n");
	}

	private String arrayLateralViewAlias(Node arrayNode) {
		return "arr_" + arrayNode.getName();
	}
	
	private String newLateralViewAlias(String lateralViewAlias, Node nestedNode) {
		// need to make sure aliases are unique
		return lateralViewAlias + "_" + nestedNode.getName();
	}

	private void generateHiveViewStart(StringBuilder viewBuilder, String database, String viewName) {
		viewBuilder.append("DROP VIEW IF EXISTS ");
		viewBuilder.append(database);
		viewBuilder.append(".");
		viewBuilder.append(viewName);
		viewBuilder.append(";\n");
		viewBuilder.append("CREATE VIEW ");
		viewBuilder.append(database);
		viewBuilder.append(".");
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
