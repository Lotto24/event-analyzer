package de.esailors.dataheart.drillviews.processor;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import de.esailors.dataheart.drillviews.conf.Config;
import de.esailors.dataheart.drillviews.data.Event;
import de.esailors.dataheart.drillviews.data.Topic;

public class Persister {

	// TODO might want to extract markdown specifics to utility class

	// TODO move the limit to config
	private static final int MAXIMUM_SAMPLES_TO_PERSIST = 10;

	private static final Logger log = LogManager.getLogger(Persister.class.getName());

	private Config config;

	private ObjectMapper jsonObjectMapper;
	private String formattedCurrentTime;

	public Persister(Config config) {
		this.config = config;
		this.jsonObjectMapper = new ObjectMapper();
		formattedCurrentTime = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date());
	}

	public void persistDrillView(Topic topic, String createStatement) {
		log.info("Writing drill view to disc for " + topic);
		FileWriterUtil.writeFile(config.OUTPUT_DRILL_DIRECTORY, topic.getName() + ".sql", createStatement);
	}

	public void persistEventSamples(Topic topic) {
		if (topic.getEvents().size() == 0) {
			log.debug("No events received to write samples for in: " + topic);
			return;
		}
		String eventSample = "";
		int cnt = 0;
		for (Event event : topic.getEvents()) {
//			eventSample += JsonPrettyPrinter.prettyPrintJsonString(event.getEventJson()) + "\n";
			eventSample += event.getEventJson().toString() + "\n";
			cnt++;
			if (cnt >= MAXIMUM_SAMPLES_TO_PERSIST) {
				break;
			}
		}
		FileWriterUtil.writeFile(config.OUTPUT_SAMPLES_DIRECTORY, topic.getName() + ".json", eventSample);

	}

	public void persistChangeLog(ChangeLog changeLog) {

		if (!changeLog.hasEntries()) {
			return;
		}
		// TODO more of a placeholder for now
		// changeSet should be part of README and update in descending chronological
		// order with nice markdown formatting
		String changeSetContent = "## " + formattedCurrentTime + " ChangeLog:\n\n";
		for (String message : changeLog.getMessages()) {
			changeSetContent += "* " + message + "\n";
		}

		String changeSetFile = "changelog_" + formattedCurrentTime + ".md";

		FileWriterUtil.writeFile(config.OUTPUT_CHANGELOGS_DIRECTORY, changeSetFile, changeSetContent);
	}

	public void persistTopicReport(Topic topic) {
		log.info("Writing topic report for: " + topic.getName());
		String reportContent = "# Topic report for: " + topic.getName() + "\n";
		if (topic.isConsistent()) {
			reportContent += "### Topic was consistent\n\n";
		} else {
			reportContent += "### Topic was **NOT** consistent!\n\n";
		}

		reportContent += "#### Analyzed events: " + topic.getEvents().size() + "\n\n";
		if (topic.getExampleEvent() != null) {
			reportContent += generateJsonInformation("Example Event", topic.getExampleEvent().getEventJson());
		} else {
			reportContent += "#### Example Event: **Not available**\n";
		}
		reportContent += generateTopicInformation(topic);

		if (!topic.getReportMessages().isEmpty()) {
			reportContent += "### Report messages:\n";
			for (String reportMessage : topic.getReportMessages()) {
				reportContent += "* " + reportMessage + "\n";
			}
			reportContent += "\n\n";
		}

		FileWriterUtil.writeFile(config.OUTPUT_TOPIC_DIRECTORY, topic.getName() + ".md", reportContent);
	}

	private String generateJsonInformation(String name, JsonNode json) {
		String jsonInformation = "#### " + name + ": \n";
		jsonInformation += "```javascript\n";
		jsonInformation += JsonPrettyPrinter.prettyPrintJsonString(json);
		jsonInformation += "\n```\n";
		return jsonInformation;
	}

	private String generateTopicInformation(Topic topic) {
		String topicInformation = "### Topic information:\n";
		topicInformation += generateSubTopicInformation("EventType", topic.getEventTypes());
		topicInformation += generateSubTopicInformation("Schema Versions", topic.getSchemaVersions());
		topicInformation += generateSubTopicInformation("Messages are avro", topic.getMessagesAreAvro());
		topicInformation += generateSubTopicInformation("Avro Schema Hashes", topic.getAvroSchemaHashes());

		topicInformation += "* Topic Partitions: " + topic.getPartitionCount() + "\n";

		if (topic.getMessageSchemas().size() == 1) {
			Schema schema = topic.getMessageSchemas().iterator().next();
			if (schema == null) {
				topicInformation += "#### Avro Schema: **NOT Avro**\n";
			} else {
				String avroSchemaJson = schema.toString();
				try {
					topicInformation += generateJsonInformation("Avro Schema",
							jsonObjectMapper.readTree(avroSchemaJson));
				} catch (IOException e) {
					throw new IllegalStateException("Unable to render avro schema", e);
				}
			}
		}

		return topicInformation;
	}

	private String generateSubTopicInformation(String type, Set<?> items) {
		String topicInformation = "* " + type + "\n";
		for (Object item : items) {
			topicInformation += "  * " + (item == null ? "null" : item.toString()) + "\n";
		}
		return topicInformation;
	}

	public String getFormattedCurrentTime() {
		return formattedCurrentTime;
	}

}
