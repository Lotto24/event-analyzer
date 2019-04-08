package de.esailors.dataheart.drillviews.processor;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.base.Optional;

import de.esailors.dataheart.drillviews.conf.Config;
import de.esailors.dataheart.drillviews.data.AvroSchema;
import de.esailors.dataheart.drillviews.data.Event;
import de.esailors.dataheart.drillviews.data.EventStructure;
import de.esailors.dataheart.drillviews.data.EventType;
import de.esailors.dataheart.drillviews.data.Topic;
import de.esailors.dataheart.drillviews.util.GitRepository;
import de.esailors.dataheart.drillviews.util.JsonUtil;
import de.esailors.dataheart.drillviews.util.SystemUtil;

public class Persister {

	private static final Logger log = LogManager.getLogger(Persister.class.getName());

	private static final String README_FILE = "README.md";
	private static final String README_TEMPLATE_FILE = "README.md.template";

	private Optional<GitRepository> gitRepositoryOption;
	private ObjectMapper jsonObjectMapper;
	private String formattedCurrentTime;

	public Persister(Optional<GitRepository> gitRepositoryOption) {
		this.gitRepositoryOption = gitRepositoryOption;
		this.jsonObjectMapper = new ObjectMapper();
		formattedCurrentTime = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date());

		initOutputDirectories();
	}

	private void initOutputDirectories() {
		wipeDirectory(Config.getInstance().OUTPUT_DIRECTORY);

		ensureDirectoryExists(Config.getInstance().OUTPUT_DIRECTORY);
		ensureDirectoryExists(outputDirectoryPathFor(Config.getInstance().OUTPUT_AVROSCHEMAS_DIRECTORY));
		ensureDirectoryExists(outputDirectoryPathFor(Config.getInstance().OUTPUT_DRILL_DIRECTORY));
		ensureDirectoryExists(outputDirectoryPathFor(Config.getInstance().OUTPUT_SAMPLES_DIRECTORY));
		ensureDirectoryExists(outputDirectoryPathFor(Config.getInstance().OUTPUT_TOPIC_DIRECTORY));
		ensureDirectoryExists(outputDirectoryPathFor(Config.getInstance().OUTPUT_CHANGELOGS_DIRECTORY));
		ensureDirectoryExists(outputDirectoryPathFor(Config.getInstance().OUTPUT_EVENTSTRUCTURES_DIRECTORY));
		ensureDirectoryExists(outputDirectoryPathFor(Config.getInstance().OUTPUT_EVENTTYPE_DIRECTORY));
	}

	private void wipeDirectory(String directoryPath) {
		File directory = new File(directoryPath);
		log.debug("Wiping directory: " + directory.getAbsolutePath());
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
		FileWriterUtil.writeFile(outputDirectoryPathFor(Config.getInstance().OUTPUT_DRILL_DIRECTORY),
				fileNameForDrillView(eventType), createStatement);
	}

	public String outputDirectoryPathFor(EventStructure eventStructure) {
		return outputDirectoryPathFor(Config.getInstance().OUTPUT_EVENTSTRUCTURES_DIRECTORY)
				+ eventStructure.getEventType().getName() + File.separator;
	}

	public String outputDirectoryPathFor(AvroSchema avroSchema) {
		return outputDirectoryPathFor(Config.getInstance().OUTPUT_AVROSCHEMAS_DIRECTORY) + avroSchema.getName()
				+ File.separator;
	}

	public String outputDirectoryPathForReadme() {
		return outputDirectoryPathFor("");
	}

	public String outputDirectoryPathFor(String subPath) {
		return Config.getInstance().OUTPUT_DIRECTORY + File.separator + subPath
				+ (subPath.isEmpty() ? "" : File.separator);
	}

	public void persistEventSamples(EventType eventType) {
		if (eventType.getEvents().size() == 0) {
			log.debug("No events received to write samples for in: " + eventType);
			return;
		}
		String eventSample = "";
		int cnt = 0;
		for (Event event : eventType.getEvents()) {
			eventSample += event.getEventJson().toString() + "\n";
			cnt++;
			if (cnt >= Config.getInstance().OUTPUT_SAMPLES_COUNT) {
				break;
			}
		}
		FileWriterUtil.writeFile(outputDirectoryPathFor(Config.getInstance().OUTPUT_SAMPLES_DIRECTORY),
				fileNameForEventSamples(eventType), eventSample);
	}

	public void persistChanges(ChangeLog changeLog) {

		if (!changeLog.hasChanges()) {
			return;
		}
		String changeSetContent = "## " + formattedCurrentTime + " ChangeLog:\n\n";
		for (String message : changeLog.getChanges()) {
			changeSetContent += "* " + message + "\n";
		}

		String changeSetFile = "changelog_" + formattedCurrentTime + ".md";

		FileWriterUtil.writeFile(outputDirectoryPathFor(Config.getInstance().OUTPUT_CHANGELOGS_DIRECTORY),
				changeSetFile, changeSetContent);
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

		FileWriterUtil.writeFile(outputDirectoryPathFor(Config.getInstance().OUTPUT_CHANGELOGS_DIRECTORY),
				changeSetFile, changeSetContent);
	}

	public void persistTopicReport(Topic topic) {
		log.info("Writing topic report for: " + topic.getName());
		String sourcePath = outputDirectoryPathFor(Config.getInstance().OUTPUT_TOPIC_DIRECTORY);

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

		reportContent += lastUpdateMarker();

		FileWriterUtil.writeFile(sourcePath, fileNameForTopicReport(topic), reportContent);
	}

	private String generateJsonInformation(String name, JsonNode json) {
		String jsonInformation = "#### " + name + ": \n";
		jsonInformation += "```javascript\n";
		jsonInformation += JsonUtil.prettyPrintJsonString(json);
		jsonInformation += "\n```\n";
		return jsonInformation;
	}

	private Set<?> avroSchemaLinks(Collection<AvroSchema> avroSchemas, String sourcePath) {
		Set<String> r = new HashSet<>();
		for (AvroSchema avroSchema : avroSchemas) {
			if (avroSchema != null) {
				r.add(linkToAvroSchema(avroSchema, sourcePath));
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
		String topicInformation = "* **" + items.size() + "** " + type + "\n";
		if (items.size() > 0) {
			for (Object item : items) {
				topicInformation += "  * ";
				if(item == null) {
					topicInformation += "";
				} else if (item instanceof Optional) {
					Optional<?> option = (Optional<?>)item;
					if(option.isPresent()) {
						topicInformation += option.get();
					} else {
						topicInformation += "ABSENT";
					}
				} else {
					topicInformation += item.toString();
				}
				topicInformation += "\n";
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
		String sourcePath = outputDirectoryPathFor(Config.getInstance().OUTPUT_EVENTTYPE_DIRECTORY);

		String reportContent = "# EventType Report: " + eventType.getName() + "\n";

		reportContent += lastUpdateMarker();

		reportContent += "#### Analyzed Events: " + eventType.getEvents().size() + "\n\n";
		Optional<Long> drillViewCountOption = eventType.getDrillViewCountOption();
		reportContent += "#### Drill View Count: " + (drillViewCountOption.isPresent() ? drillViewCountOption.get() : "UNKNOWN") + "\n\n";

		// consistency
		if (eventType.isConsistent()) {
			reportContent += "#### EventType was consistent\n\n";
		} else {
			reportContent += "### EventType was **NOT** consistent!\n\n";
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

		// source topics
		reportContent += "### Found in the following topics:\n";
		for (Topic topic : eventType.getSourceTopics()) {
			reportContent += "* "
					+ linkToTopicReport(topic, outputDirectoryPathFor(Config.getInstance().OUTPUT_EVENTTYPE_DIRECTORY))
					+ "\n";
		}

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
				avroSchemaLinks(eventType.getAvroSchemas().values(), sourcePath));
		eventTypeInformation += generateSubInformation("Schema Versions", eventType.getSchemaVersions());
		eventTypeInformation += generateSubInformation("Messages are avro", eventType.getMessagesAreAvro());
		eventTypeInformation += generateSubInformation("Timestamp Types", eventType.getTimestampTypes());

		return eventTypeInformation;
	}

	public void persistAvroSchema(AvroSchema avroSchema) {
		String sourcePath = outputDirectoryPathFor(avroSchema);

		// avro schema report
		String reportContent = "# Avro Schema: " + avroSchema.getSchema().getName() + "\n";

		reportContent += "### Full name:\n";
		reportContent += "* " + avroSchema.getFullSchemaName() + "\n";

		reportContent += "### Schema Version:\n";
		Optional<String> schemaVersionOption = avroSchema.getSchemaVersion();
		reportContent += "* " + (schemaVersionOption.isPresent() ? schemaVersionOption.get() : "UNKNOWN_SCHEMA_VERSION")
				+ "\n";

		// event type
		reportContent += "### Event Type:\n";
		reportContent += "* " + linkToEventTypeReport(avroSchema.getEventType(), sourcePath) + "\n";

		// event structure
		persistEventStructure(avroSchema.getEventStructure());
		reportContent += "### Event Structure:\n";
		reportContent += "* " + linkToEventStructureReport(avroSchema.getEventStructure(), sourcePath) + "\n";

		// avro schema json
		String avroSchemaJson = JsonUtil.prettyPrintJsonString(parseToJson(avroSchema.getSchema()));
		String avroSchemaJsonFile = fileNameForAvroSchemaJson(avroSchema);
		FileWriterUtil.writeFile(sourcePath, avroSchemaJsonFile, avroSchemaJson);
		reportContent += "### Schema JSON:\n";
		reportContent += "* " + linkToAvroSchemaJson(avroSchema, sourcePath) + "\n";

		reportContent += lastUpdateMarker();

		// write to disk
		FileWriterUtil.writeFile(sourcePath, fileNameForAvroSchemaReport(avroSchema), reportContent);
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

		plotTree(eventStructure);

		// make an .md for this event structure
		String eventStructureContent = "# Event Structure: " + eventStructure.toString() + "\n";

		eventStructureContent += "### Event Type:\n";
		eventStructureContent += "* " + linkToEventTypeReport(eventStructure.getEventType(), sourcePath) + "\n";

		eventStructureContent += "### Plot\n";
		eventStructureContent += eventStructurePlot(eventStructure, sourcePath) + "\n";

		eventStructureContent += "### Source: " + eventStructure.getSource().getType() + "\n";
		switch (eventStructure.getSource().getType()) {
		case EVENT: {
			eventStructureContent += generateJsonInformation("Source event",
					eventStructure.getSource().getSourceEvent().get().getEventJson());
			break;
		}
		case AVRO: {
			eventStructureContent += "* "
					+ linkToAvroSchema(eventStructure.getSource().getSourceSchema().get(), sourcePath) + "\n";
			break;
		}
		case MERGE: {
			Collection<EventStructure> sourceStructures = eventStructure.getSource().getSourceStructures().get();
			for (EventStructure sourceStructure : sourceStructures) {
				eventStructureContent += "* " + linkToEventStructureReport(sourceStructure, sourcePath) + "\n";
			}

			break;
		}
		}

		eventStructureContent += lastUpdateMarker();

		FileWriterUtil.writeFile(sourcePath, fileNameForEventStructure(eventStructure), eventStructureContent);
	}

	private void plotTree(EventStructure eventStructure) {

		String dotFileName = fileNameForEventStructureDot(eventStructure);

		String dotContent = eventStructure.getEventStructureTree().toDot();
		if (gitRepositoryOption.isPresent()) {
			// check if .dot changed from git repository, if not don't render it again
			String dotFileSubPath = Config.getInstance().OUTPUT_EVENTSTRUCTURES_DIRECTORY + File.separator
					+ eventStructure.getEventType().getName() + File.separator + dotFileName;
			Optional<String> existingDotContentOption = gitRepositoryOption.get().loadFile(dotFileSubPath);
			if (existingDotContentOption.isPresent()) {
				if (existingDotContentOption.get().equals(dotContent)) {
					log.debug("Event structure plot did not change, skipping rendering for " + eventStructure);
					return;
				}
				log.info("Event structure plot has changed, rerendering " + eventStructure);
			} else {
				log.debug("Event structure not found in local git repository");
			}
		} else {
			log.debug("Local git repository disabled, unable to check if event structure has changed");
		}

		doPlotTree(eventStructure, dotContent, dotFileName, fileNameForEventStructurePlot(eventStructure));
	}

	private void doPlotTree(EventStructure eventStructure, String dotContent, String dotFileName, String pngFileName) {

		String dotFileFolder = outputDirectoryPathFor(eventStructure);
		String plotFileFolder = outputDirectoryPathFor(eventStructure);

		FileWriterUtil.writeFile(dotFileFolder, dotFileName, dotContent);
		String renderDotCommand = "dot -Tpng " + dotFileFolder + File.separator + dotFileName + " -o" + plotFileFolder
				+ File.separator + pngFileName;

		SystemUtil.executeCommand(renderDotCommand);
	}

	public void updateReadme(Map<String, EventType> eventTypes) {
		log.info("Updating readme");

		String sourcePath = outputDirectoryPathForReadme();

		// add an index to all EventTypes to README.md for ease of navigation
		File readmeTemplate = FileWriterUtil.getFileFromResources(README_TEMPLATE_FILE);
		if (!readmeTemplate.exists()) {
			throw new IllegalStateException(
					"Missing readme template in resources: " + readmeTemplate.getAbsolutePath());
		}
		String readmeContent;
		try {
			readmeContent = FileUtils.readFileToString(readmeTemplate);
		} catch (IOException e) {
			throw new IllegalStateException(
					"Unable to read readme template from resources at: " + readmeTemplate.getAbsolutePath());
		}

		List<String> eventTypesIndex = new ArrayList<>(eventTypes.keySet());

		// we should also link to event type reports that are already existing but not
		// part of the current run
		if (gitRepositoryOption.isPresent()) {
			List<String> reportsFromRepository = gitRepositoryOption.get()
					.listFiles(Config.getInstance().OUTPUT_EVENTTYPE_DIRECTORY);
			for (String reportFromRepository : reportsFromRepository) {
				// extract event type name from filename (remove .md extension), and unify with
				// eventTypesIndex
				log.debug("Found existing event type report in repository: " + reportFromRepository);
				String eventTypeName = reportFromRepository.substring(0, reportFromRepository.lastIndexOf("."));
				log.debug("Extracted eventTypeName: " + eventTypeName);
				if (!eventTypesIndex.contains(eventTypeName)) {
					log.debug("Adding older event type report that was not part of current run to readme index: "
							+ eventTypeName);
					eventTypesIndex.add(eventTypeName);
				}
			}
		}
		Collections.sort(eventTypesIndex);

		readmeContent += "### Event Types\n";
		for (String eventTypeName : eventTypesIndex) {
			readmeContent += "* " + linkToEventTypeReportByName(eventTypeName, sourcePath) + "\n";
		}

		readmeContent += lastUpdateMarker();

		// write to disk
		FileWriterUtil.writeFile(sourcePath, fileNameForReadme(), readmeContent);
	}

	private String lastUpdateMarker() {
		return "#### Last Update: " + formattedCurrentTime + "\n";
	}

	private String eventStructurePlot(EventStructure eventStructure, String sourcePath) {
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

	public String fileNameForAvroSchemaJson(AvroSchema avroSchema) {
		return avroSchema.getSchemaHash() + ".json";
	}

	public String fileNameForAvroSchemaReport(AvroSchema avroSchema) {
		return avroSchema.getSchemaHash() + ".md";
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

	public String fileNameForReadme() {
		return README_FILE;
	}

	private String linkToTopicReport(Topic topic, String sourcePath) {
		return generateLink(topic.getName(), sourcePath,
				outputDirectoryPathFor(Config.getInstance().OUTPUT_TOPIC_DIRECTORY) + fileNameForTopicReport(topic));
	}

	private String linkToEventTypeReport(EventType eventType, String sourcePath) {
		return linkToEventTypeReportByName(eventType.getName(), sourcePath);
	}

	private String linkToEventTypeReportByName(String eventTypeName, String sourcePath) {
		return generateLink(eventTypeName, sourcePath,
				outputDirectoryPathFor(Config.getInstance().OUTPUT_EVENTTYPE_DIRECTORY)
						+ fileNameForEventTypeReportByName(eventTypeName));
	}

	private String linkToEventSamples(EventType eventType, String sourcePath) {
		return generateLink("Event sample", sourcePath,
				outputDirectoryPathFor(Config.getInstance().OUTPUT_SAMPLES_DIRECTORY)
						+ fileNameForEventSamples(eventType));
	}

	private String linkToDrillView(EventType eventType, String sourcePath) {
		return generateLink("Drill view", sourcePath,
				outputDirectoryPathFor(Config.getInstance().OUTPUT_DRILL_DIRECTORY) + fileNameForDrillView(eventType));
	}

	private String linkToAvroSchema(AvroSchema avroSchema, String sourcePath) {
		return generateLink(avroSchema.getName(), sourcePath,
				outputDirectoryPathFor(avroSchema) + fileNameForAvroSchemaReport(avroSchema));
	}

	private String linkToAvroSchemaJson(AvroSchema avroSchema, String sourcePath) {
		return generateLink(avroSchema.getName(), sourcePath,
				outputDirectoryPathFor(avroSchema) + fileNameForAvroSchemaJson(avroSchema));
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
		log.debug("Checking if " + parent.toString() + " is parent of " + child.toString() + ": " + r);
		return r;
	}

}
