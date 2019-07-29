package de.esailors.dataheart.drillviews.processor;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.Optional;

import de.esailors.dataheart.drillviews.conf.Config;
import de.esailors.dataheart.drillviews.data.AvroSchema;
import de.esailors.dataheart.drillviews.data.Event;
import de.esailors.dataheart.drillviews.data.EventStructure;
import de.esailors.dataheart.drillviews.data.EventStructureSource;
import de.esailors.dataheart.drillviews.data.EventType;
import de.esailors.dataheart.drillviews.data.Topic;
import de.esailors.dataheart.drillviews.util.GitRepository;
import de.esailors.dataheart.drillviews.util.JsonUtil;
import de.esailors.dataheart.drillviews.util.SystemUtil;

public class Persister {

	private static final Logger log = LogManager.getLogger(Persister.class.getName());

	private static final String README_TEMPLATE_FILE = "README.md.template";

	private Optional<GitRepository> gitRepositoryOption;
	private PersisterPaths persisterPaths;
	private ObjectMapper jsonObjectMapper;
	private String formattedCurrentTime;

	public Persister(Optional<GitRepository> gitRepositoryOption, PersisterPaths persisterPaths) {
		this.gitRepositoryOption = gitRepositoryOption;
		this.persisterPaths = persisterPaths;
		this.jsonObjectMapper = new ObjectMapper();
		formattedCurrentTime = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date());

		persisterPaths.initOutputDirectories();
	}

	public void persistDrillView(EventType eventType, String createStatement) {
		log.debug("Writing drill view to disc for " + eventType);
		FileUtil.writeFile(persisterPaths.outputDirectoryPathFor(Config.getInstance().OUTPUT_DRILL_DIRECTORY),
				persisterPaths.fileNameForDrillView(eventType), createStatement);
	}

	public void persistHiveView(EventType eventType, String createStatement) {
		log.debug("Writing hive view to disc for " + eventType);
		FileUtil.writeFile(persisterPaths.outputDirectoryPathFor(Config.getInstance().OUTPUT_HIVE_DIRECTORY),
				persisterPaths.fileNameForHiveView(eventType), createStatement);
	}

	public void persistDwhTable(EventType eventType, String ddl) {
		log.debug("Writing DWH table to disc for " + eventType);
		FileUtil.writeFile(persisterPaths.outputDirectoryPathFor(Config.getInstance().OUTPUT_DWH_TABLES_DIRECTORY),
				persisterPaths.fileNameForDwhTable(eventType), ddl);
	}
	
	public void persistDwhJob(EventType eventType, String job) {
		log.debug("Writing DWH job to disc for " + eventType);
		FileUtil.writeFile(persisterPaths.outputDirectoryPathFor(Config.getInstance().OUTPUT_DWH_JOBS_DIRECTORY),
				persisterPaths.fileNameForDwhJob(eventType), job);
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
		FileUtil.writeFile(persisterPaths.outputDirectoryPathFor(Config.getInstance().OUTPUT_SAMPLES_DIRECTORY),
				persisterPaths.fileNameForEventSamples(eventType), eventSample);
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

		FileUtil.writeFile(persisterPaths.outputDirectoryPathFor(Config.getInstance().OUTPUT_CHANGELOGS_DIRECTORY),
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

		FileUtil.writeFile(persisterPaths.outputDirectoryPathFor(Config.getInstance().OUTPUT_CHANGELOGS_DIRECTORY),
				changeSetFile, changeSetContent);
	}

	public void persistTopicReport(Topic topic) {
		log.debug("Writing topic report for: " + topic.getName());
		String sourcePath = persisterPaths.outputDirectoryPathFor(Config.getInstance().OUTPUT_TOPIC_DIRECTORY);

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

		FileUtil.writeFile(sourcePath, persisterPaths.fileNameForTopicReport(topic), reportContent);
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
		String subInformation = "* **" + items.size() + "** " + type + "\n";
		if (items.size() > 0) {
			for (Object item : items) {
				subInformation += "  * ";
				if (item == null) {
					subInformation += "";
				} else if (item instanceof Optional) {
					Optional<?> option = (Optional<?>) item;
					if (option.isPresent()) {
						subInformation += option.get();
					} else {
						subInformation += "ABSENT";
					}
				} else {
					subInformation += item.toString();
				}
				subInformation += "\n";
			}
		} else {
			subInformation += "  * _none detected_\n";
		}
		return subInformation;
	}

	public String getFormattedCurrentTime() {
		return formattedCurrentTime;
	}

	public void persistEventTypeReport(EventType eventType) {
		String sourcePath = persisterPaths.outputDirectoryPathFor(Config.getInstance().OUTPUT_EVENTTYPE_DIRECTORY);

		String reportContent = "# EventType Report: " + eventType.getName() + "\n";

		reportContent += lastUpdateMarker();
		
		// consistency
		if (eventType.isConsistent()) {
			reportContent += "#### EventType was consistent\n\n";
		} else {
			reportContent += "### EventType was **NOT** consistent!\n\n";
		}

		reportContent += "#### Analyzed Events: " + eventType.getEvents().size() + "\n\n";
		Optional<Long> drillViewCountOption = eventType.getDrillViewCountOption();
		reportContent += "#### Drill View Count: "
				+ (drillViewCountOption.isPresent() ? drillViewCountOption.get() : "UNKNOWN") + "\n\n";
		
		Optional<Long> hiveViewCountOption = eventType.getHiveViewCountOption();
		reportContent += "#### Hive View Count: "
				+ (hiveViewCountOption.isPresent() ? hiveViewCountOption.get() : "UNKNOWN") + "\n\n";

		// report messages
		if (!eventType.getReportMessages().isEmpty()) {
			reportContent += "### Report messages:\n";
			for (String reportMessage : eventType.getReportMessages()) {
				reportContent += "* " + reportMessage + "\n";
			}
			reportContent += "\n\n";
		}

		reportContent += "### Event Structures:\n";
		Optional<EventStructure> mergedEventStructureOption = eventType.getMergedEventStructured();
		if (mergedEventStructureOption.isPresent()) {
			EventStructure mergedEventStructure = mergedEventStructureOption.get();
			reportContent += "* " + linkToEventStructureReport(mergedEventStructure, sourcePath) + "\n";
			for (String sourceStructureName : mergedEventStructure.getSourceStructureNames()) {
				reportContent += "* " + linkToEventStructureReport(sourceStructureName, eventType, sourcePath) + "\n";
			}
		} else {
			reportContent += "* _MERGED EVENT STRUCTURE NOT AVAILABLE!_\n";
			reportContent += generateSubInformation("Event Structures",
					eventStructureLinks(eventType.getEventStructures(), sourcePath));
		}

		reportContent += generateEventTypeInformation(eventType, sourcePath);

		// source topics
		reportContent += "### Found in the following topics:\n";
		for (Topic topic : eventType.getSourceTopics()) {
			reportContent += "* "
					+ linkToTopicReport(topic,
							persisterPaths.outputDirectoryPathFor(Config.getInstance().OUTPUT_EVENTTYPE_DIRECTORY))
					+ "\n";
		}

		// links
		// TODO only link to things where the file exists
		reportContent += "#### Links:\n";
		reportContent += "* " + linkToEventSamples(eventType, sourcePath) + "\n";
		reportContent += "* " + linkToDrillView(eventType, sourcePath) + "\n";
		reportContent += "* " + linkToHiveView(eventType, sourcePath) + "\n";
		reportContent += "* " + linkToDwhTable(eventType, sourcePath) + "\n";
		reportContent += "* " + linkToDwhJob(eventType, sourcePath) + "\n";

		// write report to disk
		FileUtil.writeFile(sourcePath, persisterPaths.fileNameForEventTypeReport(eventType), reportContent);
	}

	private String generateEventTypeInformation(EventType eventType, String sourcePath) {
		String eventTypeInformation = "### EventType information:\n";
		eventTypeInformation += generateSubInformation("Avro Schemas",
				avroSchemaLinks(eventType.getAvroSchemas().values(), sourcePath));
		eventTypeInformation += generateSubInformation("Schema Versions", eventType.getSchemaVersions());
		eventTypeInformation += generateSubInformation("Messages are avro", eventType.getMessagesAreAvro());
		eventTypeInformation += generateSubInformation("Timestamp Types", eventType.getTimestampTypes());

		return eventTypeInformation;
	}

	public void persistAvroSchema(AvroSchema avroSchema) {
		String sourcePath = persisterPaths.outputDirectoryPathFor(avroSchema);

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
		String avroSchemaJsonFile = persisterPaths.fileNameForAvroSchemaJson(avroSchema);
		FileUtil.writeFile(sourcePath, avroSchemaJsonFile, avroSchemaJson);
		reportContent += "### Schema JSON:\n";
		reportContent += "* " + linkToAvroSchemaJson(avroSchema, sourcePath) + "\n";

		reportContent += lastUpdateMarker();

		// write to disk
		FileUtil.writeFile(sourcePath, persisterPaths.fileNameForAvroSchemaReport(avroSchema), reportContent);
	}

	public void persistEventStructures(EventType eventType) {
		for (EventStructure eventStructure : eventType.getEventStructures()) {
			if (eventStructure.getSource().getType().equals(EventStructureSource.Type.DESERIALIZED)) {
				continue;
			}
			persistEventStructure(eventStructure);
		}
		Optional<EventStructure> mergedEventStructureOption = eventType.getMergedEventStructured();
		if (mergedEventStructureOption.isPresent()) {
			persistEventStructure(mergedEventStructureOption.get());
			serializeMergedEventStructureTreeToDisk(mergedEventStructureOption.get());
		}
		;
	}

	private void serializeMergedEventStructureTreeToDisk(EventStructure mergedEventStructure) {

		String fileName = persisterPaths.fileNameForEventStructureSerialization(mergedEventStructure);
		String filePath = persisterPaths.outputDirectoryPathFor(mergedEventStructure) + File.separator + fileName;

		log.debug("Serializing merged EventStructure tree for " + mergedEventStructure.getEventType().getName() + " to "
				+ filePath);

		try (FileOutputStream file = new FileOutputStream(filePath);
				ObjectOutputStream objectOutputStream = new ObjectOutputStream(file)) {
			objectOutputStream.writeObject(mergedEventStructure.getEventStructureTree());
		} catch (IOException e) {
			log.error("Error while serializing merged EventStructure tree for "
					+ mergedEventStructure.getEventType().getName(), e);
			throw new IllegalStateException("Error while serializing merged EventStructure tree for "
					+ mergedEventStructure.getEventType().getName(), e);
		}
	}

	public void persistEventStructure(EventStructure eventStructure) {
		// first write .dot and render as .png
		String sourcePath = persisterPaths.outputDirectoryPathFor(eventStructure);

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
			Set<String> propertySourceStructureNames = eventStructure.getSourceStructureNames();
			for (String propertySourceStructureName : propertySourceStructureNames) {
				eventStructureContent += "* " + linkToEventStructureReport(propertySourceStructureName,
						eventStructure.getEventType(), sourcePath) + "\n";
			}

			break;
		}
		default: {
			throw new IllegalArgumentException(
					"Unexpected EventStructureSource.Type: " + eventStructure.getSource().getType());
		}
		}

		eventStructureContent += lastUpdateMarker();

		FileUtil.writeFile(sourcePath, persisterPaths.fileNameForEventStructure(eventStructure), eventStructureContent);
	}

	private void plotTree(EventStructure eventStructure) {
		String dotFileName = persisterPaths.fileNameForEventStructureDot(eventStructure);
		String dotContent = new Dotter(eventStructure).generateDot();
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
				log.info("Event structure not found in local git repository, starting to render " + eventStructure);
			}
		} else {
			log.debug("Local git repository disabled, unable to check if event structure has changed");
		}

		doPlotTree(eventStructure, dotContent, dotFileName,
				persisterPaths.fileNameForEventStructurePlot(eventStructure));
	}

	private void doPlotTree(EventStructure eventStructure, String dotContent, String dotFileName, String pngFileName) {

		String dotFileFolder = persisterPaths.outputDirectoryPathFor(eventStructure);
		String plotFileFolder = persisterPaths.outputDirectoryPathFor(eventStructure);

		FileUtil.writeFile(dotFileFolder, dotFileName, dotContent);
		String renderDotCommand = "dot -Tpng " + dotFileFolder + File.separator + dotFileName + " -o" + plotFileFolder
				+ File.separator + pngFileName;

		SystemUtil.executeCommand(renderDotCommand);
	}

	public void updateReadme(Map<String, EventType> eventTypes) {
		log.info("Updating readme");

		String sourcePath = persisterPaths.outputDirectoryPathForReadme();

		// add an index to all EventTypes to README.md for ease of navigation

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

		String readmeContent = FileUtil.loadFromResources(README_TEMPLATE_FILE);
		readmeContent += "### Event Types\n";
		for (String eventTypeName : eventTypesIndex) {
			readmeContent += "* " + linkToEventTypeReportByName(eventTypeName, sourcePath) + "\n";
		}

		readmeContent += lastUpdateMarker();

		// write to disk
		FileUtil.writeFile(sourcePath, persisterPaths.fileNameForReadme(), readmeContent);
	}

	private String lastUpdateMarker() {
		return "#### Last Update: " + formattedCurrentTime + "\n";
	}

	private String eventStructurePlot(EventStructure eventStructure, String sourcePath) {
		return "!" + generateLink(eventStructure.toString(), sourcePath,
				persisterPaths.outputDirectoryPathFor(eventStructure)
						+ persisterPaths.fileNameForEventStructurePlot(eventStructure));
	}

	private String linkToTopicReport(Topic topic, String sourcePath) {
		return generateLink(topic.getName(), sourcePath,
				persisterPaths.outputDirectoryPathFor(Config.getInstance().OUTPUT_TOPIC_DIRECTORY)
						+ persisterPaths.fileNameForTopicReport(topic));
	}

	private String linkToEventTypeReport(EventType eventType, String sourcePath) {
		return linkToEventTypeReportByName(eventType.getName(), sourcePath);
	}

	private String linkToEventTypeReportByName(String eventTypeName, String sourcePath) {
		return generateLink(eventTypeName, sourcePath,
				persisterPaths.outputDirectoryPathFor(Config.getInstance().OUTPUT_EVENTTYPE_DIRECTORY)
						+ persisterPaths.fileNameForEventTypeReportByName(eventTypeName));
	}

	private String linkToEventSamples(EventType eventType, String sourcePath) {
		return generateLink("Event sample", sourcePath,
				persisterPaths.outputDirectoryPathFor(Config.getInstance().OUTPUT_SAMPLES_DIRECTORY)
						+ persisterPaths.fileNameForEventSamples(eventType));
	}

	private String linkToDrillView(EventType eventType, String sourcePath) {
		return generateLink("Drill view", sourcePath,
				persisterPaths.outputDirectoryPathFor(Config.getInstance().OUTPUT_DRILL_DIRECTORY)
						+ persisterPaths.fileNameForDrillView(eventType));
	}

	private String linkToHiveView(EventType eventType, String sourcePath) {
		return generateLink("Hive view", sourcePath,
				persisterPaths.outputDirectoryPathFor(Config.getInstance().OUTPUT_HIVE_DIRECTORY)
						+ persisterPaths.fileNameForHiveView(eventType));
	}
	
	private String linkToDwhTable(EventType eventType, String sourcePath) {
		return generateLink("DWH table", sourcePath,
				persisterPaths.outputDirectoryPathFor(Config.getInstance().OUTPUT_DWH_TABLES_DIRECTORY)
						+ persisterPaths.fileNameForDwhTable(eventType));
	}
	
	private String linkToDwhJob(EventType eventType, String sourcePath) {
		return generateLink("DWH job", sourcePath,
				persisterPaths.outputDirectoryPathFor(Config.getInstance().OUTPUT_DWH_JOBS_DIRECTORY)
						+ persisterPaths.fileNameForDwhJob(eventType));
	}

	private String linkToAvroSchema(AvroSchema avroSchema, String sourcePath) {
		return generateLink(avroSchema.getName(), sourcePath, persisterPaths.outputDirectoryPathFor(avroSchema)
				+ persisterPaths.fileNameForAvroSchemaReport(avroSchema));
	}

	private String linkToAvroSchemaJson(AvroSchema avroSchema, String sourcePath) {
		return generateLink(avroSchema.getName(), sourcePath, persisterPaths.outputDirectoryPathFor(avroSchema)
				+ persisterPaths.fileNameForAvroSchemaJson(avroSchema));
	}

	private String linkToEventStructureReport(EventStructure eventStructure, String sourcePath) {
		return generateLink(eventStructure.toString(), sourcePath, persisterPaths.outputDirectoryPathFor(eventStructure)
				+ persisterPaths.fileNameForEventStructure(eventStructure));
	}

	private String linkToEventStructureReport(String eventStructureName, EventType eventType, String sourcePath) {
		return generateLink(eventStructureName, sourcePath, persisterPaths.outputDirectoryPathFor(eventType)
				+ persisterPaths.fileNameForEventStructure(eventStructureName));
	}

	private String generateLink(String text, String sourcePath, String targetPath) {
		return "[" + text + "](" + persisterPaths.relativePathBetween(sourcePath, targetPath) + ")";
	}

	public void addOutputToGitRepository() {
		if (!gitRepositoryOption.isPresent()) {
			log.info("Git disabled, not adding output to repository");
			return;
		}

		log.info("Adding process output to local git repository");

		GitRepository gitRepository = gitRepositoryOption.get();

		// push everything that is written to disk also to git
		gitRepository
				.addToRepository(persisterPaths.outputDirectoryPathFor(Config.getInstance().OUTPUT_DRILL_DIRECTORY));
		gitRepository
				.addToRepository(persisterPaths.outputDirectoryPathFor(Config.getInstance().OUTPUT_HIVE_DIRECTORY));
		gitRepository
				.addToRepository(persisterPaths.outputDirectoryPathFor(Config.getInstance().OUTPUT_SAMPLES_DIRECTORY));
		gitRepository
				.addToRepository(persisterPaths.outputDirectoryPathFor(Config.getInstance().OUTPUT_TOPIC_DIRECTORY));
		gitRepository.addToRepository(
				persisterPaths.outputDirectoryPathFor(Config.getInstance().OUTPUT_EVENTTYPE_DIRECTORY));
		gitRepository.addToRepository(
				persisterPaths.outputDirectoryPathFor(Config.getInstance().OUTPUT_AVROSCHEMAS_DIRECTORY));
		gitRepository.addToRepository(
				persisterPaths.outputDirectoryPathFor(Config.getInstance().OUTPUT_EVENTSTRUCTURES_DIRECTORY));
		gitRepository.addToRepository(
				persisterPaths.outputDirectoryPathFor(Config.getInstance().OUTPUT_CHANGELOGS_DIRECTORY));
		gitRepository.addToRepository(
				persisterPaths.outputDirectoryPathFor(Config.getInstance().OUTPUT_DWH_TABLES_DIRECTORY));
		gitRepository
				.addToRepository(persisterPaths.outputDirectoryPathFor(Config.getInstance().OUTPUT_DWH_JOBS_DIRECTORY));
		gitRepository
				.addToRepository(persisterPaths.outputDirectoryPathForReadme() + persisterPaths.fileNameForReadme());

		gitRepository.commitAndPush(getFormattedCurrentTime());
	}

}
