package de.esailors.dataheart.drillviews.processor;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Optional;

import de.esailors.dataheart.drillviews.conf.Config;
import de.esailors.dataheart.drillviews.data.AvroSchema;
import de.esailors.dataheart.drillviews.data.Event;
import de.esailors.dataheart.drillviews.data.EventStructure;
import de.esailors.dataheart.drillviews.data.EventType;
import de.esailors.dataheart.drillviews.data.Topic;
import de.esailors.dataheart.drillviews.data.Tree;
import de.esailors.dataheart.drillviews.jdbc.drill.DrillConnection;
import de.esailors.dataheart.drillviews.jdbc.drill.DrillViewSqlBuilder;
import de.esailors.dataheart.drillviews.jdbc.drill.DrillMetadata;
import de.esailors.dataheart.drillviews.jdbc.hive.HiveConnection;
import de.esailors.dataheart.drillviews.jdbc.hive.HiveViewSqlBuilder;
import de.esailors.dataheart.drillviews.jdbc.hive.HiveMetadata;
import de.esailors.dataheart.drillviews.util.CollectionUtil;
import de.esailors.dataheart.drillviews.util.GitRepository;

public class Processor {

	private static final Logger log = LogManager.getLogger(Processor.class.getName());

	private HiveConnection hiveConnection;
	private HiveMetadata hiveViews;
	private DrillConnection drillConnection;
	private DrillMetadata drillViews;
	private DrillViewSqlBuilder drillViewSqlBuilder;
	private HiveViewSqlBuilder hiveViewSqlBuilder;
	private DwhGenerator dwhGenerator;
	private PersisterPaths persisterPaths;
	private Persister persister;
	private Optional<GitRepository> gitRepositoryOption;
	private ChangeLog changeLog;

	private Map<String, EventType> eventTypes = new HashMap<>();
	private Map<String, AvroSchema> avroSchemas = new HashMap<>();

	public Processor(Optional<GitRepository> gitRepositoryOption) {
		this.gitRepositoryOption = gitRepositoryOption;
		this.persisterPaths = new PersisterPaths();
		this.persister = new Persister(gitRepositoryOption, persisterPaths);
		this.changeLog = new ChangeLog();
		if (Config.getInstance().DRILL_ENABLED) {
			this.drillConnection = new DrillConnection();
			this.drillViews = new DrillMetadata(drillConnection);
			this.drillViewSqlBuilder = new DrillViewSqlBuilder(drillViews);
		}
		if (Config.getInstance().HIVE_ENABLED) {
			this.hiveConnection = new HiveConnection();
			this.hiveViews = new HiveMetadata(hiveConnection);
			this.hiveViewSqlBuilder = new HiveViewSqlBuilder(hiveViews);
		}
		if (Config.getInstance().DWH_TABLE_GENERATION_ENABLED || Config.getInstance().DWH_JOB_GENERATION_ENABLED) {
			this.dwhGenerator = new DwhGenerator();
		}
	}

	public void process(Set<Topic> topics) {

		// initially I thought each topic has exactly one event type in it, but sadly
		// this is not the case. so first we process all topics, extract the eventTypes
		// out of them and then go to the EventType level
		for (Topic topic : topics) {
			process(topic);
		}
		for (EventType eventType : CollectionUtil.toSortedList(eventTypes.values())) {
			log.info("Processing " + eventType);
			markEventTypeInconsistencies(eventType);
			loadSerializedEventStructure(eventType);
			mergeEventStructures(eventType);
			updateAvroSchemaMap(eventType);
			if (Config.getInstance().DRILL_ENABLED) {
				createDrillViews(eventType);
				runCountOnDrillView(eventType);
			}
			if (Config.getInstance().HIVE_ENABLED) {
				createHiveViews(eventType);
				runCountOnHiveView(eventType);
			}
			if (Config.getInstance().DWH_TABLE_GENERATION_ENABLED) {
				createDwhTable(eventType);
			}
			if (Config.getInstance().DWH_JOB_GENERATION_ENABLED) {
				createDwhJob(eventType);
			}
			writeEventSamples(eventType);
			writeEventStructures(eventType);
			writeEventTypeReport(eventType);
		}

		writeAvroSchemas();
		writeChangeLog();

		updateReadme();

		persister.addOutputToGitRepository();
	}

	private void mergeEventStructures(EventType eventType) {
		eventType.buildMergedEventStructure();
	}

	private void loadSerializedEventStructure(EventType eventType) {
		if (gitRepositoryOption.isPresent()) {
			// TODO really hacky, reuse persister.outputPathFor() in GitRepo and remove
			// prefix again or something
			String repositoryPath = gitRepositoryOption.get()
					.filePathInRepository(Config.getInstance().OUTPUT_EVENTSTRUCTURES_DIRECTORY + eventType.getName()
							+ File.separator + persisterPaths.fileNameForEventStructureSerialization(eventType));
			File fileInRepository = new File(repositoryPath);
			if (!fileInRepository.exists()) {
				log.info("Did not find an existing serialized tree for " + eventType.getName() + " in repository at "
						+ repositoryPath);
			} else {
				Tree deserializedTree = deserializeTreeFrom(fileInRepository);
				eventType.setDeserializedEventStructure(new EventStructure(eventType, deserializedTree));
			}
		}
	}

	private Tree deserializeTreeFrom(File file) {
		try (FileInputStream fileInputStream = new FileInputStream(file);
				ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream)) {

			Tree read = (Tree) objectInputStream.readObject();

			log.info("Deserialized tree for: " + read.getRootNode().getName());

			return read;
		} catch (IOException | ClassNotFoundException e) {
			throw new IllegalStateException("Error while deserializing from " + file.getAbsolutePath(), e);
		}
	}

	private void writeEventTypeReport(EventType eventType) {
		persister.persistEventTypeReport(eventType);
	}

	private void updateReadme() {
		persister.updateReadme(eventTypes);
	}

	private void runCountOnDrillView(EventType eventType) {
		// run count on newly created view for sanity checking and report / statistics
		Optional<Long> drillViewCountOption = drillViews.runDayCount(eventType);
		if (drillViewCountOption.isPresent()) {
			long drillViewCount = drillViewCountOption.get();
			log.info("Count in day drill view for " + eventType + ": " + drillViewCount);
			eventType.setDrillViewCount(drillViewCount);
		} else {
			changeLog.addWarning("Unable to determine count via drill view of " + eventType);
		}
	}

	private void runCountOnHiveView(EventType eventType) {
		// run count on newly created view for sanity checking and report / statistics
		Optional<Long> hiveViewCountOption = hiveViews.runDayCount(eventType);
		if (hiveViewCountOption.isPresent()) {
			long hiveViewCount = hiveViewCountOption.get();
			log.info("Count in day hive view for " + eventType + ": " + hiveViewCount);
			eventType.setHiveViewCount(hiveViewCount);
		} else {
			changeLog.addWarning("Unable to determine count via drill view of " + eventType);
		}
	}

	private void writeAvroSchemas() {
		for (String schemaHash : avroSchemas.keySet()) {
			persister.persistAvroSchema(avroSchemas.get(schemaHash));
		}

	}

	private void writeChangeLog() {
		if (!changeLog.hasWarnings()) {
			log.info("No major warnings detected");
		} else {
			persister.persistWarnings(changeLog);
		}

		if (!changeLog.hasChanges()) {
			log.info("No major changes detected");
		} else {
			persister.persistChanges(changeLog);
		}
	}

	public void process(Topic topic) {

		log.info("Processing " + topic.getEvents().size() + " events for " + topic);

		markTopicInconsistencies(topic);
		updateEventTypeList(topic);
		writeTopicReport(topic);
	}

	private void writeEventStructures(EventType eventType) {
		persister.persistEventStructures(eventType);
	}

	private void updateAvroSchemaMap(EventType eventType) {

		for (Entry<String, AvroSchema> schemaEntry : eventType.getAvroSchemas().entrySet()) {
			String schemaHash = schemaEntry.getKey();
			if (schemaHash != null) {
				if (avroSchemas.get(schemaHash) != null
						&& !avroSchemas.get(schemaHash).equals(schemaEntry.getValue())) {
					changeLog.addChange("Found two different Avro Schemas for the same schema hash (" + schemaHash
							+ ") in " + eventType);
				}

				avroSchemas.put(schemaHash, schemaEntry.getValue());
			}
		}
	}

	private void updateEventTypeList(Topic topic) {
		Map<String, Set<Event>> eventTypeNameMap = topic.getEventTypeNames();
		for (Entry<String, Set<Event>> eventTypeEntry : eventTypeNameMap.entrySet()) {
			String eventTypeName = eventTypeEntry.getKey();
			Set<Event> eventTypeEvents = eventTypeEntry.getValue();
			EventType existingEventType = eventTypes.get(eventTypeName);
			if (existingEventType == null) {
				// create new EventType
				log.info("EventType detected: " + eventTypeName);
				EventType eventType = new EventType(eventTypeName, topic, eventTypeEvents);
				eventTypes.put(eventTypeName, eventType);
			} else {
				// update existing EventType
				existingEventType.addSourceTopic(topic);
				existingEventType.addEvents(eventTypeEvents);
			}
		}
	}

	private void writeTopicReport(Topic topic) {
		persister.persistTopicReport(topic);
	}

	private void writeEventSamples(EventType eventType) {
		if (gitRepositoryOption.isPresent()) {
			// check local git repository if we already have enough example events
			Optional<String> existingSamplesOption = gitRepositoryOption.get()
					.loadFile(Config.getInstance().OUTPUT_SAMPLES_DIRECTORY + File.separator
							+ persisterPaths.fileNameForEventSamples(eventType));
			if (existingSamplesOption.isPresent()) {
				String existingSamples = existingSamplesOption.get();
				int existingSampleCount = StringUtils.countMatches(existingSamples, "\n");
				if (existingSampleCount >= Config.getInstance().OUTPUT_SAMPLES_COUNT) {
					log.info("Already have enough event samples, skipping persisting samples for " + eventType);
					return;
				}
			}

		}
		persister.persistEventSamples(eventType);
	}

	private void createDrillViews(EventType eventType) {
		// compare and align generated views with those from drill
		log.info("Preparing Drill view for " + eventType);

		Optional<String> currentViewFromRepository;
		if (gitRepositoryOption.isPresent()) {
			currentViewFromRepository = gitRepositoryOption.get().loadFile(Config.getInstance().OUTPUT_DRILL_DIRECTORY
					+ File.separator + persisterPaths.fileNameForDrillView(eventType));
			if (currentViewFromRepository.isPresent()) {
				log.debug("Found a view in local git repository");
			} else {
				log.debug("No view found in local git repository");
			}
		} else {
			log.warn("Local git repository not enabled, unable to check if view changed");
			currentViewFromRepository = Optional.absent();
		}

		// generate drill views and execute them
		Optional<EventStructure> mergedEventStructuredOption = eventType.getMergedEventStructured();
		if (!mergedEventStructuredOption.isPresent()) {
			throw new IllegalStateException(
					"Topic does not provide a merged event structure even though it had an example event");
		}
		String viewFromCurrentRun = drillViewSqlBuilder.generateDrillViewsFor(eventType,
				mergedEventStructuredOption.get());

		if (drillViews.doesViewExist(eventType)) {
			log.debug("Drill view for " + eventType + " already exists");
			// check if it's the same view and don't execute if it is
			// compare with view from local git repository
			if (currentViewFromRepository.isPresent()) {
				if (currentViewFromRepository.get().equals(viewFromCurrentRun)) {
					log.info(
							"Drill view from repository is the same as the view from current run, skipping further processing");
					return;
				} else {
					changeLog.addChange("Drill view changed for " + eventType);
				}
			}

		} else {
			log.debug("No Drill view exists yet for " + eventType);
			changeLog.addChange("Generating new Drill view for: " + eventType);
		}

		// execute create statement on Drill
		try {
			drillConnection.executeSqlStatements(viewFromCurrentRun);
		} catch (SQLException e) {
			throw new IllegalStateException("Error while executing create view SQL statement on Drill", e);
		}

		// write drill views to disk
		persister.persistDrillView(eventType, viewFromCurrentRun);
	}

	private void createHiveViews(EventType eventType) {
		// TODO this is basically the same as createDrillViews -> unify
		// compare and align generated views with those from hive
		log.info("Preparing Hive view for " + eventType);

		Optional<String> currentViewFromRepository;
		if (gitRepositoryOption.isPresent()) {
			currentViewFromRepository = gitRepositoryOption.get().loadFile(Config.getInstance().OUTPUT_HIVE_DIRECTORY
					+ File.separator + persisterPaths.fileNameForHiveView(eventType));
			if (currentViewFromRepository.isPresent()) {
				log.debug("Found a view in local git repository");
			} else {
				log.debug("No view found in local git repository");
			}
		} else {
			log.warn("Local git repository not enabled, unable to check if view changed");
			currentViewFromRepository = Optional.absent();
		}

		// generate views and execute them
		Optional<EventStructure> mergedEventStructuredOption = eventType.getMergedEventStructured();
		if (!mergedEventStructuredOption.isPresent()) {
			throw new IllegalStateException(
					"Topic does not provide a merged event structure even though it had an example event");
		}
		String viewFromCurrentRun = hiveViewSqlBuilder.generateHiveViewsFor(eventType,
				mergedEventStructuredOption.get());

		// TODO hacky test
		System.out.println(viewFromCurrentRun);

		if (hiveViews.doesViewExist(eventType)) {
			log.debug("Hive view for " + eventType + " already exists");
			// check if it's the same view and don't execute if it is
			// compare with view from local git repository
			if (currentViewFromRepository.isPresent()) {
				if (currentViewFromRepository.get().equals(viewFromCurrentRun)) {
					log.info(
							"Hive view from repository is the same as the view from current run, skipping further processing");
					return;
				} else {
					changeLog.addChange("Hive view changed for " + eventType);
				}
			}
		} else {
			log.debug("No Hive view exists yet for " + eventType);
			changeLog.addChange("Generating new Hive view for: " + eventType);
		}

		// execute create statement on Hive
		try {
			hiveConnection.executeSqlStatements(viewFromCurrentRun);
		} catch (SQLException e) {
			// TODO either treat properly, log and add to changeset or throw Exception
			// log.error("Error while executing create view SQL statement on Hive", e);
			throw new IllegalStateException("Error while executing create view SQL statement on Hive", e);
		}

		// write hive views to disk
		persister.persistHiveView(eventType, viewFromCurrentRun);
	}

	private void createDwhTable(EventType eventType) {
		log.info("Creating DWH table for " + eventType);
		String ddl = dwhGenerator.createDwhTable(eventType);
		persister.persistDwhTable(eventType, ddl);
	}

	private void createDwhJob(EventType eventType) {
		log.info("Creating DWH job for " + eventType);
		String job = dwhGenerator.createDwhJob(eventType);
		persister.persistDwhJob(eventType, job);
	}

	private void markTopicInconsistencies(Topic topic) {

		topic.markInconsistencies();

		if (topic.getEvents().size() == 0) {
			changeLog.addWarning("No events received for " + topic);
		} else {
			if (topic.isConsistent()) {
				log.debug("Consistency checks passed: " + topic);
			} else {
				changeLog.addWarning("Inconsistencies detected in " + topic);
			}
		}
	}

	private void markEventTypeInconsistencies(EventType eventType) {
		eventType.markInconsistencies();

		if (eventType.getEvents().size() == 0) {
			changeLog.addWarning("No events received for " + eventType);
		} else {
			if (eventType.isConsistent()) {
				log.info("Consistency checks passed: " + eventType);
			} else {
				changeLog.addWarning("Inconsistencies detected in " + eventType);
			}
		}
	}

}
