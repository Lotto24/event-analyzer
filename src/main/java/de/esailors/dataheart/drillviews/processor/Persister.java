package de.esailors.dataheart.drillviews.processor;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
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
import de.esailors.dataheart.drillviews.data.EventType;
import de.esailors.dataheart.drillviews.data.Topic;
import de.esailors.dataheart.drillviews.data.TreePlotter;
import de.esailors.dataheart.drillviews.git.SystemUtil;

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
		wipeDirectory(config.OUTPUT_DIRECTORY);

		ensureDirectoryExists(config.OUTPUT_DIRECTORY);
		ensureDirectoryExists(outputDirectoryPathFor(config.OUTPUT_AVROSCHEMAS_DIRECTORY));
		ensureDirectoryExists(outputDirectoryPathFor(config.OUTPUT_DRILL_DIRECTORY));
		ensureDirectoryExists(outputDirectoryPathFor(config.OUTPUT_SAMPLES_DIRECTORY));
		ensureDirectoryExists(outputDirectoryPathFor(config.OUTPUT_TOPIC_DIRECTORY));
		ensureDirectoryExists(outputDirectoryPathFor(config.OUTPUT_CHANGELOGS_DIRECTORY));
		ensureDirectoryExists(outputDirectoryPathFor(config.OUTPUT_EVENTSTRUCTURES_DIRECTORY));
	}

	private void wipeDirectory(String directoryPath) {
		File directory = new File(directoryPath);
		log.info("Wiping directory: " + directory.getAbsolutePath());
		if (!directory.exists()) {
			return;
		}
		if (!directory.isDirectory()) {
			throw new IllegalArgumentException(
					"Got asked to wipe a path that exists but is not a directory: " + directoryPath);
		}

		// only wipe directories that are somewhere within working directory to avoi
		// absolute mayhem
		Optional<String> workingDirectoryOption = SystemUtil.getWorkingDirectory();
		if (!workingDirectoryOption.isPresent()) {
			throw new IllegalStateException("Unable to determine working directory, refusing to wipe directory");
		}
		String workingDirectoryPath = workingDirectoryOption.get();
		if (!isParentOf(workingDirectoryPath, directory.getAbsolutePath())) {
			throw new IllegalArgumentException("Was asked to wipe a directory (" + directory.getAbsolutePath()
					+ ") that is not part of current working directory: " + workingDirectoryPath);
		}

		try {
			FileUtils.forceDelete(directory);
		} catch (IOException e) {
			throw new IllegalStateException("Unable to wipe directory: " + directory.getAbsolutePath(), e);
		}
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

	public void persistDrillView(EventType eventType, String createStatement) {
		log.info("Writing drill view to disc for " + eventType);
		FileWriterUtil.writeFile(outputDirectoryPathFor(config.OUTPUT_DRILL_DIRECTORY), fileNameForDrillView(eventType),
				createStatement);
	}

	public String outputDirectoryPathFor(String subPath) {
		return config.OUTPUT_DIRECTORY + File.separator + subPath + File.separator;
	}

	public String outputDirectoryPathFor(EventStructure eventStructure) {
		return outputDirectoryPathFor(config.OUTPUT_EVENTSTRUCTURES_DIRECTORY) + eventStructure.getStructureBaseName()
				+ File.separator;
	}

	public void persistEventSamples(EventType eventType) {
		if (eventType.getEvents().size() == 0) {
			log.debug("No events received to write samples for in: " + eventType);
			return;
		}
		String eventSample = "";
		int cnt = 0;
		for (Event event : eventType.getEvents()) {
//			eventSample += JsonPrettyPrinter.prettyPrintJsonString(event.getEventJson()) + "\n";
			eventSample += event.getEventJson().toString() + "\n";
			cnt++;
			if (cnt >= config.OUTPUT_SAMPLES_COUNT) {
				break;
			}
		}
		FileWriterUtil.writeFile(outputDirectoryPathFor(config.OUTPUT_SAMPLES_DIRECTORY),
				fileNameForEventSamples(eventType), eventSample);

	}

	public void persistChanges(ChangeLog changeLog) {

		if (!changeLog.hasChanges()) {
			return;
		}
		// TODO more of a placeholder for now
		// changeSet should be part of README and update in descending chronological
		// order with nice markdown formatting
		String changeSetContent = "## " + formattedCurrentTime + " ChangeLog:\n\n";
		for (String message : changeLog.getChanges()) {
			changeSetContent += "* " + message + "\n";
		}

		String changeSetFile = "changelog_" + formattedCurrentTime + ".md";

		FileWriterUtil.writeFile(outputDirectoryPathFor(config.OUTPUT_CHANGELOGS_DIRECTORY), changeSetFile,
				changeSetContent);
	}
	
	public void persistWarnings(ChangeLog changeLog) {
		if (!changeLog.hasWarnings()) {
			return;
		}
		String changeSetContent = "## " + formattedCurrentTime + " Warnings:\n\n";
		for (String message : changeLog.getWarnings()) {
			changeSetContent += "* " + message + "\n";
		}

		String changeSetFile = "warnings_" + formattedCurrentTime + ".md";

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
		reportContent += "#### Invalid events: " + topic.getInvalidEvents().size() + "\n\n";

		if (!topic.getReportMessages().isEmpty()) {
			reportContent += "### Report messages:\n";
			for (String reportMessage : topic.getReportMessages()) {
				reportContent += "* " + reportMessage + "\n";
			}
			reportContent += "\n\n";
		}

		reportContent += "### Topic information:\n";
		reportContent += generateSubInformation("Event Types",
				eventTypeLinksByName(topic.getEventTypeNames().keySet(), sourcePath));

		reportContent += "* **" + topic.getPartitionCount() + "** Topic Partitions\n";

		FileWriterUtil.writeFile(sourcePath, fileNameForTopicReport(topic), reportContent);
	}

	private String generateJsonInformation(String name, JsonNode json) {
		String jsonInformation = "#### " + name + ": \n";
		jsonInformation += "```javascript\n";
		jsonInformation += JsonPrettyPrinter.prettyPrintJsonString(json);
		jsonInformation += "\n```\n";
		return jsonInformation;
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

	private Set<String> eventTypeLinks(Set<EventType> eventTypes, String sourcePath) {
		Set<String> r = new HashSet<>();
		for (EventType eventType : eventTypes) {
			r.add(linkToEventTypeReport(eventType, sourcePath));
		}
		return r;
	}

	private Set<String> eventTypeLinksByName(Set<String> eventTypeNames, String sourcePath) {
		Set<String> r = new HashSet<>();
		for (String eventTypeName : eventTypeNames) {
			r.add(linkToEventTypeReportByName(eventTypeName, sourcePath));
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

	private String generateSubInformation(String type, Set<?> items) {
		// TODO this is purely markdown
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

	public void persistEventTypeReport(EventType eventType) {
		String sourcePath = outputDirectoryPathFor(config.OUTPUT_EVENTTYPE_DIRECTORY);

		String reportContent = "# EventType Report for " + eventType.getName() + "\n";

		// consistency
		if (eventType.isConsistent()) {
			reportContent += "### EventType was consistent\n\n";
		} else {
			reportContent += "### EventType was **NOT** consistent!\n\n";
		}

		reportContent += "#### Analyzed events: " + eventType.getEvents().size() + "\n\n";

		// source topics
		reportContent += "### Found in the following topics:\n";
		for (Topic topic : eventType.getSourceTopics()) {
			reportContent += "* " + linkToTopicReport(topic, outputDirectoryPathFor(config.OUTPUT_EVENTTYPE_DIRECTORY))
					+ "\n";
		}

		// report messages
		if (!eventType.getReportMessages().isEmpty()) {
			reportContent += "### Report messages:\n";
			for (String reportMessage : eventType.getReportMessages()) {
				reportContent += "* " + reportMessage + "\n";
			}
			reportContent += "\n\n";
		}

		reportContent += "### Merged Event Structure:\n";
		Optional<EventStructure> mergedEventStructured = eventType.getMergedEventStructured();
		if (mergedEventStructured.isPresent()) {
			reportContent += "* " + linkToEventStructureReport(mergedEventStructured.get(), sourcePath) + "\n";
		} else {
			reportContent += "* _not available_\n";
		}

		reportContent += generateEventTypeInformation(eventType, sourcePath);

		// links
		reportContent += "#### Links:\n";
		reportContent += "* " + linkToEventSamples(eventType, sourcePath) + "\n";
		reportContent += "* " + linkToDrillView(eventType, sourcePath) + "\n";

		// write report to disk
		FileWriterUtil.writeFile(sourcePath, fileNameForEventTypeReport(eventType), reportContent);
	}

	private String generateEventTypeInformation(EventType eventType, String sourcePath) {
		String eventTypeInformation = "### EventType information:\n";
		eventTypeInformation += generateSubInformation("Event Structures",
				eventStructureLinks(eventType.getEventStructures(), sourcePath));
		eventTypeInformation += generateSubInformation("Avro Schemas",
				avroSchemaLinks(eventType.getAvroSchemaHashes(), sourcePath));
		eventTypeInformation += generateSubInformation("Schema Versions", eventType.getSchemaVersions());
		eventTypeInformation += generateSubInformation("Messages are avro", eventType.getMessagesAreAvro());

		return eventTypeInformation;
	}

	public void persistAvroSchema(String schemaHash, Schema schema) {
		String avroSchemaContent = JsonPrettyPrinter.prettyPrintJsonString(parseToJson(schema));

		FileWriterUtil.writeFile(outputDirectoryPathFor(config.OUTPUT_AVROSCHEMAS_DIRECTORY),
				fileNameForAvroSchema(schemaHash), avroSchemaContent);
	}

	public void persistEventStructures(EventType eventType) {
		for (EventStructure eventStructure : eventType.getEventStructures()) {
			persistEventStructure(eventStructure);
		}
		Optional<EventStructure> mergedEventStructured = eventType.getMergedEventStructured();
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

	public String fileNameForDrillView(EventType eventType) {
		return eventType.getName() + ".sql";
	}

	public String fileNameForTopicReport(Topic topic) {
		return topic.getName() + ".md";
	}

	public String fileNameForEventSamples(EventType eventType) {
		return eventType.getName() + ".json";
	}

	public String fileNameForEventTypeReport(EventType eventType) {
		return fileNameForEventTypeReportByName(eventType.getName());
	}

	public String fileNameForEventTypeReportByName(String eventTypeName) {
		return eventTypeName + ".md";
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
		return generateLink(topic.getName(), sourcePath,
				outputDirectoryPathFor(config.OUTPUT_TOPIC_DIRECTORY) + fileNameForTopicReport(topic));
	}

	private String linkToEventTypeReport(EventType eventType, String sourcePath) {
		return linkToEventTypeReportByName(eventType.getName(), sourcePath);
	}

	private String linkToEventTypeReportByName(String eventTypeName, String sourcePath) {
		return generateLink(eventTypeName, sourcePath, outputDirectoryPathFor(config.OUTPUT_EVENTTYPE_DIRECTORY)
				+ fileNameForEventTypeReportByName(eventTypeName));
	}

	private String linkToEventSamples(EventType eventType, String sourcePath) {
		return generateLink("Event sample", sourcePath,
				outputDirectoryPathFor(config.OUTPUT_SAMPLES_DIRECTORY) + fileNameForEventSamples(eventType));
	}

	private String linkToDrillView(EventType eventType, String sourcePath) {
		return generateLink("Drill view", sourcePath,
				outputDirectoryPathFor(config.OUTPUT_DRILL_DIRECTORY) + fileNameForDrillView(eventType));
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

	private boolean isParentOf(String parentPath, String childPath) {
		// inspired by
		// https://stackoverflow.com/questions/4746671/how-to-check-if-a-given-path-is-possible-child-of-another-path
		Path parent = Paths.get(parentPath).toAbsolutePath();
		Path child = Paths.get(childPath).toAbsolutePath();
		boolean r = child.startsWith(parent);
		log.info("Checking if " + parent.toString() + " is parent of " + child.toString() + ": " + r);
		return r;
	}

}
