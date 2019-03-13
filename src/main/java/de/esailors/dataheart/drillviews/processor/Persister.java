package de.esailors.dataheart.drillviews.processor;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Collection;
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

import com.google.common.base.Optional;

import de.esailors.dataheart.drillviews.conf.Config;
import de.esailors.dataheart.drillviews.data.Event;
import de.esailors.dataheart.drillviews.data.EventStructure;
import de.esailors.dataheart.drillviews.data.Topic;
import de.esailors.dataheart.drillviews.data.TreePlotter;

public class Persister {

	// TODO might want to extract markdown specifics to utility class
	// TODO clean output directory on init so we don't copy over old / inconsistend
	// leftover data

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
		ensureDirectoryExists(outputDirectoryPathFor(config.OUTPUT_EVENTSTRUCTURES_DIRECTORY));
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

	public String outputDirectoryPathFor(EventStructure eventStructure) {
		return outputDirectoryPathFor(config.OUTPUT_EVENTSTRUCTURES_DIRECTORY) + eventStructure.getStructureBaseName()
				+ File.separator;
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
		String sourcePath = outputDirectoryPathFor(config.OUTPUT_TOPIC_DIRECTORY);

		String reportContent = "# Topic report for: " + topic.getName() + "\n";
		if (topic.isConsistent()) {
			reportContent += "### Topic was consistent\n\n";
		} else {
			reportContent += "### Topic was **NOT** consistent!\n\n";
		}

		reportContent += "#### Analyzed events: " + topic.getEvents().size() + "\n\n";

		if (!topic.getReportMessages().isEmpty()) {
			reportContent += "### Report messages:\n";
			for (String reportMessage : topic.getReportMessages()) {
				reportContent += "* " + reportMessage + "\n";
			}
			reportContent += "\n\n";
		}

		reportContent += "### Merged Event Structure:\n";
		Optional<EventStructure> mergedEventStructured = topic.getMergedEventStructured();
		if (mergedEventStructured.isPresent()) {
			reportContent += "* " + linkToEventStructureReport(mergedEventStructured.get(), sourcePath) + "\n";
		} else {
			reportContent += "* _not available_\n";
		}

		reportContent += generateTopicInformation(topic, sourcePath);

		// not sure if I want to keep this now that we have event structures
		if (topic.getExampleEvent() != null) {
			// add links to sample events and drill view
			reportContent += "#### Links:\n";
			reportContent += "* " + linkToEventSamples(topic, sourcePath) + "\n";
			reportContent += "* " + linkToDrillView(topic, sourcePath) + "\n";
		}

		FileWriterUtil.writeFile(sourcePath, fileNameForTopicReport(topic), reportContent);
	}

	private String generateJsonInformation(String name, JsonNode json) {
		String jsonInformation = "#### " + name + ": \n";
		jsonInformation += "```javascript\n";
		jsonInformation += JsonPrettyPrinter.prettyPrintJsonString(json);
		jsonInformation += "\n```\n";
		return jsonInformation;
	}

	private String generateTopicInformation(Topic topic, String sourcePath) {
		String topicInformation = "### Topic information:\n";
		topicInformation += generateSubTopicInformation("Event Types",
				eventTypeLinks(topic.getEventTypes(), sourcePath));
		topicInformation += generateSubTopicInformation("Event Structures",
				eventStructureLinks(topic.getEventStructures(), sourcePath));
		topicInformation += generateSubTopicInformation("Avro Schemas",
				avroSchemaLinks(topic.getAvroSchemaHashes(), sourcePath));
		topicInformation += generateSubTopicInformation("Schema Versions", topic.getSchemaVersions());
		topicInformation += generateSubTopicInformation("Messages are avro", topic.getMessagesAreAvro());

		topicInformation += "* **" + topic.getPartitionCount() + "** Topic Partitions\n";

		return topicInformation;
	}

	private Set<?> avroSchemaLinks(Set<String> avroSchemaHashes, String sourcePath) {
		Set<String> r = new HashSet<>();
		for (String schemaHash : avroSchemaHashes) {
			if (schemaHash != null) {
				r.add(linkToAvroSchema(schemaHash, sourcePath));
			} else {
				r.add(null);
			}
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

	private Set<String> eventTypeLinks(Set<String> eventTypes, String sourcePath) {
		Set<String> r = new HashSet<>();
		for (String eventType : eventTypes) {
			r.add(linkToEventTypeReport(eventType, sourcePath));
		}
		return r;
	}

	private Set<String> eventStructureLinks(Set<EventStructure> eventStructures, String sourcePath) {
		Set<String> r = new HashSet<>();
		for (EventStructure eventStructure : eventStructures) {
			r.add(linkToEventStructureReport(eventStructure, sourcePath));
		}
		return r;
	}

	private String generateSubTopicInformation(String type, Set<?> items) {
		String topicInformation = "* **" + items.size() + "** " + type + "\n";
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

	public void persistEventTypeReport(String eventType, List<Topic> topicList) {
		String eventTypeReportContent = "# EventType Report for " + eventType + "\n";
		eventTypeReportContent += "### Found EventType in the following topics:\n";
		for (Topic topic : topicList) {
			eventTypeReportContent += "* "
					+ linkToTopicReport(topic, outputDirectoryPathFor(config.OUTPUT_EVENTTYPE_DIRECTORY)) + "\n";
		}

		FileWriterUtil.writeFile(outputDirectoryPathFor(config.OUTPUT_EVENTTYPE_DIRECTORY),
				fileNameForEventTypeReport(eventType), eventTypeReportContent);
	}

	public void persistAvroSchema(String schemaHash, Schema schema) {
		String avroSchemaContent = JsonPrettyPrinter.prettyPrintJsonString(parseToJson(schema));

		FileWriterUtil.writeFile(outputDirectoryPathFor(config.OUTPUT_AVROSCHEMAS_DIRECTORY),
				fileNameForAvroSchema(schemaHash), avroSchemaContent);
	}

	public void persistEventStructures(Topic topic) {
		for (EventStructure eventStructure : topic.getEventStructures()) {
			persistEventStructure(eventStructure);
		}
		Optional<EventStructure> mergedEventStructured = topic.getMergedEventStructured();
		if (mergedEventStructured.isPresent()) {
			persistEventStructure(mergedEventStructured.get());
		}
		;
	}

	public void persistEventStructure(EventStructure eventStructure) {
		// first write .dot and render as .png
		String sourcePath = outputDirectoryPathFor(eventStructure);
		TreePlotter.getInstance().plotTree(eventStructure.getEventStructureTree(), sourcePath,
				fileNameForEventStructureDot(eventStructure), sourcePath,
				fileNameForEventStructurePlot(eventStructure));

		// make an .md for this event structure
		String eventStructureContent = "# Event Structure for: " + eventStructure.toString() + "\n";

		eventStructureContent += "### Base Name: " + eventStructure.getStructureBaseName() + "\n";

		eventStructureContent += "### Plot\n";
		eventStructureContent += eventStructurePicture(eventStructure, sourcePath) + "\n";

		eventStructureContent += "### Source: " + eventStructure.getSource().getType() + "\n";
		switch (eventStructure.getSource().getType()) {
		case EVENT: {
			eventStructureContent += generateJsonInformation("Source event",
					eventStructure.getSource().getSourceEvent().get().getEventJson());
			break;
		}
		case AVRO_SCHEMA: {
			eventStructureContent += "* "
					+ linkToAvroSchema(eventStructure.getSource().getSourceSchemaHash().get(), sourcePath) + "\n";
			break;
		}
		case STRUCTURE_MERGE: {
			Collection<EventStructure> sourceStructures = eventStructure.getSource().getSourceStructures().get();
			for (EventStructure sourceStructure : sourceStructures) {
				eventStructureContent += "* " + linkToEventStructureReport(sourceStructure, sourcePath) + "\n";
			}

			break;
		}
		}

		FileWriterUtil.writeFile(sourcePath, fileNameForEventStructure(eventStructure), eventStructureContent);
	}

	private String eventStructurePicture(EventStructure eventStructure, String sourcePath) {
		return "!" + generateLink(eventStructure.toString(), sourcePath,
				outputDirectoryPathFor(eventStructure) + fileNameForEventStructurePlot(eventStructure));
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

	public String fileNameForEventStructure(EventStructure eventStructure) {
		return eventStructure.toString() + ".md";
	}

	public String fileNameForEventStructureDot(EventStructure eventStructure) {
		return eventStructure.toString() + ".dot";
	}

	public String fileNameForEventStructurePlot(EventStructure eventStructure) {
		return eventStructure.toString() + ".png";
	}

	private String linkToTopicReport(Topic topic, String sourcePath) {
		return generateLink(topic.getName(), sourcePath, outputDirectoryPathFor(config.OUTPUT_TOPIC_DIRECTORY) + fileNameForTopicReport(topic));
	}

	private String linkToEventTypeReport(String eventType, String sourcePath) {
		return generateLink(eventType, sourcePath,
				outputDirectoryPathFor(config.OUTPUT_EVENTTYPE_DIRECTORY) + fileNameForEventTypeReport(eventType));
	}

	private String linkToEventSamples(Topic topic, String sourcePath) {
		return generateLink("Event sample", sourcePath,
				outputDirectoryPathFor(config.OUTPUT_SAMPLES_DIRECTORY) + fileNameForEventSamples(topic));
	}

	private String linkToDrillView(Topic topic, String sourcePath) {
		return generateLink("Drill view", sourcePath, outputDirectoryPathFor(config.OUTPUT_DRILL_DIRECTORY) + fileNameForDrillView(topic));
	}

	private String linkToAvroSchema(String schemaHash, String sourcePath) {
		return generateLink(schemaHash, sourcePath,
				outputDirectoryPathFor(config.OUTPUT_AVROSCHEMAS_DIRECTORY) + fileNameForAvroSchema(schemaHash));
	}

	private String linkToEventStructureReport(EventStructure eventStructure, String sourcePath) {
		return generateLink(eventStructure.toString(), sourcePath,
				outputDirectoryPathFor(eventStructure) + fileNameForEventStructure(eventStructure));
	}

	private String generateLink(String text, String sourcePath, String targetPath) {
		return "[" + text + "](" + relativePathBetween(sourcePath, targetPath) + ")";
	}

	private String relativePathBetween(String sourcePath, String targetPath) {
		// inspired by
		// https://stackoverflow.com/questions/204784/how-to-construct-a-relative-path-in-java-from-two-absolute-paths-or-urls
		return Paths.get(sourcePath).relativize(Paths.get(targetPath)).toString();
	}

}
