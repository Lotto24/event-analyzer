package de.esailors.dataheart.drillviews.processor;

import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Optional;

import de.esailors.dataheart.drillviews.conf.Config;
import de.esailors.dataheart.drillviews.data.EventStructure;
import de.esailors.dataheart.drillviews.data.EventType;
import de.esailors.dataheart.drillviews.data.Node;
import de.esailors.dataheart.drillviews.util.JsonUtil.JsonType;

public class DwhJobGenerator {

	private static final Logger log = LogManager.getLogger(DwhJobGenerator.class.getName());

	private static final String JOB_NAME_PLACEHOLDER = "__JOB_NAME__";
	private static final String EVENT_TYPE_PLACEHOLDER = "__EVENT_TYPE__";
	private static final String COLUMNS_PLACEHOLDER = "__COLUMNS__";
	private static final String TABLE_NAME_PLACEHOLDER = "__TABLE_NAME__";

	private static final String JOB_NAME_PREFIX = "DWHSRC_HBASE_LOADJSON_";
	private static final String FALLBACK_DATA_TYPE = "String";

	public String createDwhJob(EventType eventType) {

		Optional<EventStructure> mergedEventStructuredOption = eventType.getMergedEventStructured();
		if (!mergedEventStructuredOption.isPresent()) {
			throw new IllegalStateException(
					"Expect mergedEventStructure to exist to create a job out of for: " + eventType.toString());
		}
		EventStructure eventStructure = mergedEventStructuredOption.get();
		Set<Node> leafNodes = eventStructure.getEventStructureTree().getLeafNodes();

		String jobName = JOB_NAME_PREFIX + eventType.getName().toUpperCase();
		String columns = "";
		
		int index = 1; // 0 taken for _RUN_ID_ in template
		for (Node node : leafNodes) {
			Optional<String> jsonTableTypeForNode = dataTypeForNode(node);

			String columnName = node.getId().replaceAll("\\.", "_");
			String dataType = jsonTableTypeForNode.isPresent() ? jsonTableTypeForNode.get() : FALLBACK_DATA_TYPE;
			
			columns += "('" + jobName + "', 'JSONPARSER', " + index + ", 'Target Column', '" + columnName + "'),\n";
			columns += "('" + jobName + "', 'JSONPARSER', " + index + ", 'JsonPath', '$." + node.getId() + "'),\n";
			columns += "('" + jobName + "', 'JSONPARSER', " + index + ", 'Type', '" + dataType + "'),\n\n";

			index++;
		}

		// TODO no comma at the end!

		String job = FileUtil.loadFromResources(Config.getInstance().DWH_JOB_TEMPLATE_FILE);

		
		job = job.replace(JOB_NAME_PLACEHOLDER, jobName);
		job = job.replace(EVENT_TYPE_PLACEHOLDER, eventType.getName().toUpperCase());
		job = job.replace(TABLE_NAME_PLACEHOLDER, eventType.getName());
		job = job.replace(COLUMNS_PLACEHOLDER, columns);

		return job;
	}

	private Optional<String> dataTypeForNode(Node node) {
		Optional<String> jsonTypeForNodeOption = DwhTableGenerator.jsonTypeForNode(node);
		if (!jsonTypeForNodeOption.isPresent()) {
			log.warn("Unable to detect json type for: " + node.getId());
			return Optional.absent();
		}
		String jsonType = jsonTypeForNodeOption.get();
		switch (JsonType.valueOf(jsonType)) {
		case TEXTUAL: return Optional.of("String");
		case INTEGER: return Optional.of("Integer");
		case FLOAT: return Optional.of("Number");
		case BOOLEAN: return Optional.of("Boolean");
		default: {
			log.warn("Unsupported jsonType " + jsonType + " for node: " + node.getId());
			return Optional.absent();
		}
	}
	}

}
