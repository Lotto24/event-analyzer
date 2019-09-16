package de.esailors.dataheart.drillviews.jdbc.hive;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.esailors.dataheart.drillviews.conf.Config;
import de.esailors.dataheart.drillviews.data.EventStructure;
import de.esailors.dataheart.drillviews.data.EventType;
import de.esailors.dataheart.drillviews.data.Node;
import de.esailors.dataheart.drillviews.data.PrimitiveType;
import de.esailors.dataheart.drillviews.data.PrimitiveTypeDetector;
import de.esailors.dataheart.drillviews.util.CollectionUtil;

public class HiveViewSqlBuilder {

	private static final Logger log = LogManager.getLogger(HiveViewSqlBuilder.class.getName());

	private static final String HBASE_TABLE_ALIAS = "k";
	private static final String HBASE_TABLE_ROWKEY_COLUMN = "rowkey";
	private static final String HBASE_TABLE_JSON_COLUMN = "json";

	private static final String COMPLEX_TYPE_EVENT_ALIAS = "event";
	private static final String COMPLEX_TYPE_SUB_VIEW_ALIAS = "c";

	private static final String HIVE_ROOT_LATERAL_VIEW_ALIAS = "r";
	private static final String LATERAL_VIEW_ARRAY_ITEM_ALIAS = "item";
	private static final String LATERAL_VIEW_ARRAY_INDEX_ALIAS = "index";

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
		String viewName = hiveViews.viewNameFor(eventType);

		complexTypeView(Config.getInstance().HIVE_VIEW_ALL_DATABASE, viewName, eventStructure, viewBuilder, node,
				Optional.empty());
		complexTypeView(Config.getInstance().HIVE_VIEW_DAY_DATABASE, viewName, eventStructure, viewBuilder, node,
				Optional.of(86400));
		complexTypeView(Config.getInstance().HIVE_VIEW_WEEK_DATABASE, viewName, eventStructure, viewBuilder, node,
				Optional.of(604800));

//		lateralViewsView(Config.getInstance().HIVE_VIEW_ALL_DATABASE, viewName + "_lv", eventStructure, viewBuilder,
//				node, Optional.empty());
//		lateralViewsView(Config.getInstance().HIVE_VIEW_DAY_DATABASE, viewName + "_lv", eventStructure, viewBuilder,
//				node, Optional.of(86400));
//		lateralViewsView(Config.getInstance().HIVE_VIEW_WEEK_DATABASE, viewName + "_lv", eventStructure, viewBuilder,
//				node, Optional.of(604800));

		return viewBuilder.toString();
	}

	private void complexTypeView(String database, String viewName, EventStructure eventStructure,
			StringBuilder viewBuilder, Node node, Optional<Integer> timeLimit) {

		createView(viewBuilder, database, viewName);

		viewBuilder.append("SELECT\n");
		viewBuilder.append(ident());
		viewBuilder.append(COMPLEX_TYPE_SUB_VIEW_ALIAS);
		viewBuilder.append(".`");
		viewBuilder.append(HBASE_TABLE_ROWKEY_COLUMN);
		viewBuilder.append("`");

		String path = COMPLEX_TYPE_SUB_VIEW_ALIAS + ".`" + COMPLEX_TYPE_EVENT_ALIAS + "`";
		selectComplexTypeColumns(viewBuilder, node, path, "", true);

		viewBuilder.append("\nFROM (\n");

		selectStart(viewBuilder);
		selectComplexType(viewBuilder, node);
		fromClause(viewBuilder);
		whereClause(eventStructure, viewBuilder, timeLimit);

		viewBuilder.append("\n) ");
		viewBuilder.append(COMPLEX_TYPE_SUB_VIEW_ALIAS);

		endStatement(viewBuilder);
	}

	private void selectComplexTypeColumns(StringBuilder viewBuilder, Node node, String parentPath, String aliasPrefix, boolean topLevel) {
		Map<String, Node> childMap = node.getChildMap();
		for (String childPath : CollectionUtil.toSortedList(childMap.keySet())) {
			Node child = childMap.get(childPath);
			String hiveColumnName = hiveComplexTypeGenerator.hiveColumnNameFor(child);
			if (child.hasArrayType() || !child.hasChildren()) {
				viewBuilder.append(",\n");
				viewBuilder.append(ident());
				if(topLevel && hiveColumnName.equals(Config.getInstance().EVENT_FIELD_TIMESTAMP)) {
					// automatically cast toplevel timestamp field to timestamp type
					viewBuilder.append("CAST(");
				}
				viewBuilder.append(parentPath);
				viewBuilder.append(".`");
				viewBuilder.append(hiveColumnName);
				viewBuilder.append("`");
				if(topLevel && hiveColumnName.equals(Config.getInstance().EVENT_FIELD_TIMESTAMP)) {
					viewBuilder.append(" AS timestamp)");
				}
				viewBuilder.append(" as `");
				viewBuilder.append(aliasPrefix);
				viewBuilder.append(hiveColumnName);
				viewBuilder.append("`");
				
			} else {
				String newParentPath = parentPath + ".`" + hiveColumnName + "`";
				String newAliasPrefix = aliasPrefix + hiveColumnName + "_";
				selectComplexTypeColumns(viewBuilder, child, newParentPath, newAliasPrefix, false);
			}
		}
	}

	private void selectComplexType(StringBuilder viewBuilder, Node node) {
		String hiveType = hiveComplexTypeGenerator.generateComplexTypeFor(node);
		viewBuilder.append(",\n");
		viewBuilder.append(ident());
		viewBuilder.append("default.from_json(");
		viewBuilder.append(HBASE_TABLE_ALIAS);
		viewBuilder.append(".`");
		viewBuilder.append(HBASE_TABLE_JSON_COLUMN);
		viewBuilder.append("`, '");
		viewBuilder.append(hiveType);
		viewBuilder.append("') as `");
		viewBuilder.append(COMPLEX_TYPE_EVENT_ALIAS);
		viewBuilder.append("`");
	}

	private void lateralViewsView(String database, String viewName, EventStructure eventStructure,
			StringBuilder viewBuilder, Node node, Optional<Integer> timeLimit) {
		createView(viewBuilder, database, viewName);
		selectStart(viewBuilder);
		selectLateralViewColumns(viewBuilder, node, HIVE_ROOT_LATERAL_VIEW_ALIAS, true);
		fromClause(viewBuilder);
		lateralViews(viewBuilder, node, HIVE_ROOT_LATERAL_VIEW_ALIAS, HBASE_TABLE_ALIAS, HBASE_TABLE_JSON_COLUMN);
		whereClause(eventStructure, viewBuilder, timeLimit);
		endStatement(viewBuilder);
	}

	private void selectLateralViewColumns(StringBuilder viewBuilder, Node node, String lateralViewAlias,
			boolean topLevel) {

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
				viewBuilder.append(LATERAL_VIEW_ARRAY_INDEX_ALIAS);
				viewBuilder.append("` as `");
				viewBuilder.append(child.getId().replace(".", "_"));
				viewBuilder.append("_array_index`");

				Map<String, Node> arrayChildMap = child.getChildMap();
				for (String arrayChildPath : CollectionUtil.toSortedList(arrayChildMap.keySet())) {
					Node arrayChild = arrayChildMap.get(arrayChildPath);
					viewBuilder.append(",\n");
					viewBuilder.append(ident());
					viewBuilder.append(arrayLateralViewAlias);
					viewBuilder.append(".`");
					viewBuilder.append(LATERAL_VIEW_ARRAY_ITEM_ALIAS);
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
			selectLateralViewColumns(viewBuilder, nestedChild, newLateralViewAlias, false);
		}
	}

	private void fromClause(StringBuilder viewBuilder) {
		viewBuilder.append("\nFROM ");
		viewBuilder.append(Config.getInstance().HIVE_HBASE_TABLE);
		viewBuilder.append(" ");
		viewBuilder.append(HBASE_TABLE_ALIAS);
		viewBuilder.append("\n");
	}

	private void lateralViews(StringBuilder viewBuilder, Node node, String lateralViewAlias, String nodeAlias,
			String nodeColumn) {

		if (node.getChildren().isEmpty() && !node.hasArrayType()) {
			throw new IllegalArgumentException(
					"Unable to generate lateral views for non-nested non-array node: " + node.getId());
		}
		if (node.hasArrayType()) {
			lateralViewForArrayNode(viewBuilder, node, lateralViewAlias, nodeAlias, nodeColumn);
		} else {
			laterViewForNestedNode(viewBuilder, node, lateralViewAlias, nodeAlias, nodeColumn);
		}
	}

	private void laterViewForNestedNode(StringBuilder viewBuilder, Node node, String lateralViewAlias, String nodeAlias,
			String nodeColumn) {

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
			if (child.hasChildren() || child.hasArrayType()) {
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
			lateralViews(viewBuilder, nestedNode, newLateralViewAlias, lateralViewAlias, nestedNode.getName());
		}
	}

	private void lateralViewForArrayNode(StringBuilder viewBuilder, Node node, String lateralViewAlias,
			String nodeAlias, String nodeColumn) {
		if (!node.hasArrayType()) {
			throw new IllegalArgumentException("Unable to generate array lateral view non-array node: " + node.getId());
		}

		viewBuilder.append("LATERAL VIEW OUTER posexplode(default.from_json(");
		viewBuilder.append(nodeAlias);
		viewBuilder.append(".`");
		viewBuilder.append(nodeColumn);
		// for objects use map<string,string, otherwise use primitives>
		String hiveTypeForArrayItem;
		if (node.hasChildren()) {
			hiveTypeForArrayItem = "map<string,string>";
		} else {
			Optional<PrimitiveType> primitiveArrayItemTypeForNode = PrimitiveTypeDetector.getInstance().primitiveArrayItemTypeForNode(node);
			if(primitiveArrayItemTypeForNode.isPresent()) {
				hiveTypeForArrayItem = hiveComplexTypeGenerator.hiveTypeFor(primitiveArrayItemTypeForNode.get());
			} else {
				hiveTypeForArrayItem = "string";
			}
		}
		viewBuilder.append("`, 'array<" + hiveTypeForArrayItem + ">')) ");
		viewBuilder.append(arrayLateralViewAlias(node));
		viewBuilder.append(" as ");
		viewBuilder.append(LATERAL_VIEW_ARRAY_INDEX_ALIAS);
		viewBuilder.append(", ");
		viewBuilder.append(LATERAL_VIEW_ARRAY_ITEM_ALIAS);
		viewBuilder.append("\n");
	}

	private void createView(StringBuilder viewBuilder, String database, String viewName) {
		viewBuilder.append("DROP VIEW IF EXISTS ");
		viewBuilder.append(database);
		viewBuilder.append(".");
		viewBuilder.append(viewName);
		viewBuilder.append(";\n");
		viewBuilder.append("CREATE VIEW ");
		viewBuilder.append(database);
		viewBuilder.append(".");
		viewBuilder.append(viewName);
		viewBuilder.append(" AS \n");
	}

	private void selectStart(StringBuilder viewBuilder) {
		viewBuilder.append("SELECT\n");
		viewBuilder.append(ident());
		viewBuilder.append(HBASE_TABLE_ALIAS);
		viewBuilder.append(".`");
		viewBuilder.append(HBASE_TABLE_ROWKEY_COLUMN);
		viewBuilder.append("` as `");
		viewBuilder.append(HBASE_TABLE_ROWKEY_COLUMN);
		viewBuilder.append("`");
	}

	private void whereClause(EventStructure eventStructure, StringBuilder viewBuilder, Optional<Integer> timeLimit) {
		viewBuilder.append("WHERE ");
		viewBuilder.append(HBASE_TABLE_ROWKEY_COLUMN);
		viewBuilder.append(" >= ");
		viewBuilder.append("'");
		viewBuilder.append(eventStructure.getEventType().getName().toUpperCase());
		rowKeyStart(viewBuilder, timeLimit);
		viewBuilder.append(" AND ");
		viewBuilder.append(HBASE_TABLE_ROWKEY_COLUMN);
		viewBuilder.append(" < ");
		viewBuilder.append("'");
		viewBuilder.append(eventStructure.getEventType().getName().toUpperCase());
		viewBuilder.append("-9'");
	}

	private void endStatement(StringBuilder viewBuilder) {
		viewBuilder.append(";\n");
	}

	private void rowKeyStart(StringBuilder viewBuilder, Optional<Integer> timeLimit) {
		String rowKeyStart;
		if (timeLimit == null || !timeLimit.isPresent()) {
			rowKeyStart = "-0'";
		} else {
			rowKeyStart = "-' || (unix_timestamp() - " + timeLimit.get() + ")";
		}
		viewBuilder.append(rowKeyStart);
	}

	private String arrayLateralViewAlias(Node arrayNode) {
		return "arr_" + arrayNode.getId().replace(".", "_");
	}

	private String newLateralViewAlias(String lateralViewAlias, Node nestedNode) {
		// need to make sure aliases are unique
		return lateralViewAlias + "_" + nestedNode.getName();
	}

	private static String ident() {
		StringBuilder r = new StringBuilder();
		for (int i = 0; i < IDENTATION; i++) {
			r.append(" ");
		}
		return r.toString();
	}

}
