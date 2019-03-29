package de.esailors.dataheart.drillviews.processor;

import java.io.File;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Optional;

import de.esailors.dataheart.drillviews.conf.Config;
import de.esailors.dataheart.drillviews.data.AvroSchema;
import de.esailors.dataheart.drillviews.data.Event;
import de.esailors.dataheart.drillviews.data.EventStructure;
import de.esailors.dataheart.drillviews.data.EventType;
import de.esailors.dataheart.drillviews.data.Topic;
import de.esailors.dataheart.drillviews.drill.DrillConnection;
import de.esailors.dataheart.drillviews.drill.DrillViews;
import de.esailors.dataheart.drillviews.util.GitRepository;

public class Processor {

	private static final Logger log = LogManager.getLogger(Processor.class.getName());

	private DrillConnection drillConnection;
	private DrillViews drillViews;
	private CreateViewSqlBuilder createViewSqlBuilder;
	private Persister persister;
	private Optional<GitRepository> gitRepositoryOption;
	private ChangeLog changeLog;

	private Map<String, EventType> eventTypes = new HashMap<>();
	private Map<String, AvroSchema> avroSchemas = new HashMap<>();

	public Processor(Optional<GitRepository> gitRepositoryOption) {
		this.gitRepositoryOption = gitRepositoryOption;
		this.drillConnection = new DrillConnection();
		this.createViewSqlBuilder = new CreateViewSqlBuilder();
		this.drillViews = new DrillViews(drillConnection);
		this.persister = new Persister();
		this.changeLog = new ChangeLog();
	}

	public void process(Set<Topic> topics) {

		// initially I thought each topic has exactly one event type in it, but sadly
		// this is not the case. so first we process all topics, extract the eventTypes
		// out of them and then go to the EventType level
		for (Topic topic : topics) {
			process(topic);
		}
		for (EventType eventType : eventTypes.values()) {
			log.info("Processing " + eventType);
			markEventTypeInconsistencies(eventType);
			updateAvroSchemaMap(eventType);
			createDrillViews(eventType);
			// TODO run count on newly created view for sanity checking and report
//			runCountOnDrillView(eventType);
			writeEventSamples(eventType);
			writeEventStructures(eventType);
		}

		// TODO fields report? maybe overkill but sounds cool

		writeEventTypeReport();
		writeAvroSchemas();
		writeChangeLog();
		
		// TODO add an index to all EventTypes to README.md for ease of navigation

		addOutputToGitRepository();
	}

	private void runCountOnDrillView(EventType eventType) {
		// TODO STOPPED HERE
		long count = drillViews.runDayCount(eventType);
		log.info("Count in day view for " + eventType + ": " + count);
	}

	private void addOutputToGitRepository() {
		if(!gitRepositoryOption.isPresent()) {
			log.info("Git disabled, not adding output to repository");
			return;
		}
		
		log.info("Adding process output to local git repository");
		
		GitRepository gitRepository = gitRepositoryOption.get();

		// push everything that is written to disk also to git
		gitRepository.addToRepository(persister.outputDirectoryPathFor(Config.getInstance().OUTPUT_DRILL_DIRECTORY));
		gitRepository.addToRepository(persister.outputDirectoryPathFor(Config.getInstance().OUTPUT_SAMPLES_DIRECTORY));
		gitRepository.addToRepository(persister.outputDirectoryPathFor(Config.getInstance().OUTPUT_TOPIC_DIRECTORY));
		gitRepository.addToRepository(persister.outputDirectoryPathFor(Config.getInstance().OUTPUT_EVENTTYPE_DIRECTORY));
		gitRepository.addToRepository(persister.outputDirectoryPathFor(Config.getInstance().OUTPUT_AVROSCHEMAS_DIRECTORY));
		gitRepository.addToRepository(persister.outputDirectoryPathFor(Config.getInstance().OUTPUT_EVENTSTRUCTURES_DIRECTORY));
		gitRepository.addToRepository(persister.outputDirectoryPathFor(Config.getInstance().OUTPUT_CHANGELOGS_DIRECTORY));

		// TODO output any kind of statistics / report? to git / readme + changelog

		gitRepository.commitAndPush(persister.getFormattedCurrentTime());
	}

	private void writeAvroSchemas() {
		// TODO check if it actually changes before persisting
		for (String schemaHash : avroSchemas.keySet()) {
			persister.persistAvroSchema(avroSchemas.get(schemaHash));
		}

	}

	private void writeEventTypeReport() {
		// TODO check if it actually changes before persisting

		for (EventType eventType : eventTypes.values()) {
			persister.persistEventTypeReport(eventType);
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
			// TODO add last n changelogs to README.md in reverse chronological order
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
				log.info("New eventType detected: " + eventTypeName);
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
		// TODO check local git repository if report even changed
		persister.persistTopicReport(topic);
	}

	private void writeEventSamples(EventType eventType) {
		// TODO check local git repository if we already have enough example events (for
		// this schemaVersion) and don't persist more if we already do
		persister.persistEventSamples(eventType);
	}

	private void createDrillViews(EventType eventType) {
		// TODO compare and align generated views with those from drill
		log.info("Preparing Drill view for " + eventType);

		Optional<String> currentViewFromRepository;
		if(gitRepositoryOption.isPresent()) {
			currentViewFromRepository = gitRepositoryOption.get().loadFileFromRepository(Config.getInstance().OUTPUT_DRILL_DIRECTORY + File.separator + persister.fileNameForDrillView(eventType));
			if (currentViewFromRepository.isPresent()) {
				log.debug("Found a view in local git repository");
			} else {
				log.info("No view found in local git repository");
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
		String viewName = drillViews.viewNameFor(eventType);
		String viewFromCurrentRun = createViewSqlBuilder.generateDrillViewsFor(viewName, mergedEventStructuredOption.get());

		if (drillViews.doesViewExist(viewName)) {
			log.debug("Drill view for " + eventType + " already exists");
			// check if it's the same view and don't execute if it is
			// compare with view from local git repository
			if (currentViewFromRepository.isPresent()) {
				if (currentViewFromRepository.get().equals(viewFromCurrentRun)) {
					log.info(
							"View from repository is the same as the view from current run, skipping further processing");
					return;
				} else {
					changeLog.addChange("Drill view changed for " + eventType);
					// TODO use git diff to see changes
				}
			}

		} else {
			log.debug("No Drill view exists yet for " + eventType);
			changeLog.addChange("Genearting new Drill view for: " + eventType);
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
		eventType.buildMergedEventStructure();

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
