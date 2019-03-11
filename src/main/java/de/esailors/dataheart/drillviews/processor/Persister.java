package de.esailors.dataheart.drillviews.processor;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import de.esailors.dataheart.drillviews.conf.Config;
import de.esailors.dataheart.drillviews.data.Event;
import de.esailors.dataheart.drillviews.data.Topic;

public class Persister {

	// TODO might want to extract markdown specifics to utility class

	private static final Logger log = LogManager.getLogger(Persister.class.getName());

	private Config config;

	private ObjectMapper jsonObjectMapper;
	private String formattedCurrentTime;

	public Persister(Config config) {
		this.config = config;
		this.jsonObjectMapper = new ObjectMapper();
		formattedCurrentTime = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date());

		initOutputDirectories();
	}

	private void initOutputDirectories() {
		ensureDirectoryExists(config.OUTPUT_DIRECTORY);
		ensureDirectoryExists(outputDirectoryPathFor(config.OUTPUT_DRILL_DIRECTORY));
		ensureDirectoryExists(outputDirectoryPathFor(config.OUTPUT_SAMPLES_DIRECTORY));
		ensureDirectoryExists(outputDirectoryPathFor(config.OUTPUT_TOPIC_DIRECTORY));
		ensureDirectoryExists(outputDirectoryPathFor(config.OUTPUT_CHANGELOGS_DIRECTORY));
	}

	private void ensureDirectoryExists(String directoryPath) {
		File outputDirectory = new File(directoryPath);
		if (!outputDirectory.exists()) {
			try {
				FileUtils.forceMkdir(outputDirectory);
			} catch (IOException e) {
				throw new IllegalStateException("Unable to create directory at " + outputDirectory.getAbsolutePath());
			}
		}
	}

	public void persistDrillView(Topic topic, String createStatement) {
		log.info("Writing drill view to disc for " + topic);
		FileWriterUtil.writeFile(outputDirectoryPathFor(config.OUTPUT_DRILL_DIRECTORY), fileNameForDrillView(topic),
				createStatement);
	}

	public String outputDirectoryPathFor(String subPath) {
		return config.OUTPUT_DIRECTORY + File.separator + subPath + File.separator;
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
			if (cnt >= config.OUTPUT_SAMPLES_COUNT) {
				break;
			}
		}
		FileWriterUtil.writeFile(outputDirectoryPathFor(config.OUTPUT_SAMPLES_DIRECTORY),
				fileNameForEventSamples(topic), eventSample);

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

		FileWriterUtil.writeFile(outputDirectoryPathFor(config.OUTPUT_CHANGELOGS_DIRECTORY), changeSetFile,
				changeSetContent);
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
			// add links to sample events and drill view
			reportContent += "#### Links:\n";
			reportContent += "* " + linkToEventSamples(topic) + "\n";
			reportContent += "* " + linkToDrillView(topic) + "\n";
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

		FileWriterUtil.writeFile(outputDirectoryPathFor(config.OUTPUT_TOPIC_DIRECTORY), fileNameForTopicReport(topic),
				reportContent);
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
		topicInformation += generateSubTopicInformation("EventType", eventTypeLinks(topic.getEventTypes()));
		topicInformation += generateSubTopicInformation("Avro Schemas", avroSchemaLinks(topic.getAvroSchemaHashes()));
		topicInformation += generateSubTopicInformation("Schema Versions", topic.getSchemaVersions());
		topicInformation += generateSubTopicInformation("Messages are avro", topic.getMessagesAreAvro());
		// TODO maybe persist avro schemas separately and link to them

		topicInformation += "* Topic Partitions: " + topic.getPartitionCount() + "\n";

		// TODO cleanup
//		// TODO hmm ok I'm not sure this actually works for topics with mixed avro and
//		// non-avro events
//		if (topic.getMessageSchemas().size() == 1) {
//			Schema schema = topic.getMessageSchemas().iterator().next();
//			if (schema == null) {
//				topicInformation += "#### Avro Schema: **NOT Avro**\n";
//			} else {
//				topicInformation += generateJsonInformation("Avro Schema", parseToJson(schema));
//
//			}
//		}

		return topicInformation;
	}

	private Set<?> avroSchemaLinks(Set<String> avroSchemaHashes) {
		Set<String> r = new HashSet<>();
		for (String schemaHash : avroSchemaHashes) {
			r.add(linkToAvroSchema(schemaHash));
		}
		return r;
	}

	private JsonNode parseToJson(Schema avroSchema) {
		return parseToJson(avroSchema.toString());
	}

	private JsonNode parseToJson(String jsonString) {
		try {
			return jsonObjectMapper.readTree(jsonString);
		} catch (IOException e) {
			throw new IllegalStateException("Unable to render string as JsonNode: " + jsonString, e);
		}
	}

	private Set<String> eventTypeLinks(Set<String> eventTypes) {
		Set<String> r = new HashSet<>();
		for (String eventType : eventTypes) {
			r.add(linkToEventTypeReport(eventType));
		}
		return r;
	}

	private String generateSubTopicInformation(String type, Set<?> items) {
		String topicInformation = "* " + type + "\n";
		if (items.size() > 0) {
			for (Object item : items) {
				topicInformation += "  * " + (item == null ? "null" : item.toString()) + "\n";
			}
		} else {
			topicInformation += "  * _none detected_\n";
		}
		return topicInformation;
	}

	public String getFormattedCurrentTime() {
		return formattedCurrentTime;
	}

	public void writeEventTypeReport(String eventType, List<Topic> topicList) {
		String eventTypeReportContent = "# EventType Report for " + eventType + "\n";
		eventTypeReportContent += "### Found EventType in the following topics:\n";
		for (Topic topic : topicList) {
			eventTypeReportContent += "* " + linkToTopicReport(topic) + "\n";
		}

		FileWriterUtil.writeFile(outputDirectoryPathFor(config.OUTPUT_EVENTTYPE_DIRECTORY),
				fileNameForEventTypeReport(eventType), eventTypeReportContent);
	}

	public void writeAvroSchema(String schemaHash, Schema schema) {
		String avroSchemaContent = JsonPrettyPrinter.prettyPrintJsonString(parseToJson(schema));

		FileWriterUtil.writeFile(outputDirectoryPathFor(config.OUTPUT_AVROSCHEMAS_DIRECTORY),
				fileNameForAvroSchema(schemaHash), avroSchemaContent);
	}
	
	public String fileNameForDrillView(Topic topic) {
		return topic.getName() + ".sql";
	}

	public String fileNameForTopicReport(Topic topic) {
		return topic.getName() + ".md";
	}

	public String fileNameForEventSamples(Topic topic) {
		return topic.getName() + ".json";
	}

	public String fileNameForEventTypeReport(String eventType) {
		return eventType + ".md";
	}

	public String fileNameForAvroSchema(String schemaHash) {
		return schemaHash + ".json";
	}

	private String linkToTopicReport(Topic topic) {
		return generateLink(topic.getName(), "../" + config.OUTPUT_TOPIC_DIRECTORY + fileNameForTopicReport(topic));
	}

	private String linkToEventTypeReport(String eventType) {
		return generateLink(eventType,
				"../" + config.OUTPUT_EVENTTYPE_DIRECTORY + fileNameForEventTypeReport(eventType));
	}

	private String linkToEventSamples(Topic topic) {
		return generateLink("Event sample", "../" + config.OUTPUT_SAMPLES_DIRECTORY + fileNameForEventSamples(topic));
	}

	private String linkToDrillView(Topic topic) {
		return generateLink("Drill view", "../" + config.OUTPUT_DRILL_DIRECTORY + fileNameForDrillView(topic));
	}

	private String linkToAvroSchema(String schemaHash) {
		return generateLink(schemaHash,
				"../" + config.OUTPUT_AVROSCHEMAS_DIRECTORY + fileNameForAvroSchema(schemaHash));
	}

	private String generateLink(String text, String reference) {
		return "[" + text + "](" + reference + ")";
	}

}
