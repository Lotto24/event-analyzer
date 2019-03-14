package de.esailors.dataheart.drillviews.processor;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Optional;

import de.esailors.dataheart.drillviews.conf.Config;
import de.esailors.dataheart.drillviews.data.EventStructure;
import de.esailors.dataheart.drillviews.data.Topic;
import de.esailors.dataheart.drillviews.drill.DrillConnection;
import de.esailors.dataheart.drillviews.drill.DrillViews;
import de.esailors.dataheart.drillviews.git.GitUtil;

public class Processor {

	private static final Logger log = LogManager.getLogger(Processor.class.getName());

	private Config config;

	private DrillConnection drillConnection;
	private DrillViews drillViews;
	private CreateViewSqlBuilder createViewSqlBuilder;
	private Persister persister;
	private GitUtil gitUtil;
	private ChangeLog changeLog;

	private Map<String, List<Topic>> eventTypeMap = new HashMap<>();
	private Map<String, Schema> avroSchemas = new HashMap<>();

	public Processor(Config config, GitUtil gitUtil) {
		this.config = config;
		this.gitUtil = gitUtil;
		this.drillConnection = new DrillConnection(config);
		this.createViewSqlBuilder = new CreateViewSqlBuilder(config);
		this.drillViews = new DrillViews(config, drillConnection);
		this.persister = new Persister(config);
		this.changeLog = new ChangeLog(config);
	}

	public void process(Set<Topic> topics) {

		for (Topic topic : topics) {
			process(topic);
		}
		writeEventTypeReport();
		writeAvroSchemas();

		log.info("Adding process output to local git repository");

		// push everything that is written to disk also to git
		gitUtil.addToRepository(persister.outputDirectoryPathFor(config.OUTPUT_DRILL_DIRECTORY));
		gitUtil.addToRepository(persister.outputDirectoryPathFor(config.OUTPUT_SAMPLES_DIRECTORY));
		gitUtil.addToRepository(persister.outputDirectoryPathFor(config.OUTPUT_TOPIC_DIRECTORY));
		gitUtil.addToRepository(persister.outputDirectoryPathFor(config.OUTPUT_EVENTTYPE_DIRECTORY));
		gitUtil.addToRepository(persister.outputDirectoryPathFor(config.OUTPUT_AVROSCHEMAS_DIRECTORY));
		gitUtil.addToRepository(persister.outputDirectoryPathFor(config.OUTPUT_EVENTSTRUCTURES_DIRECTORY));
		// do a kind of "EventType Report" that maps from EventType to topic (at the
		// moment we only have topic)

		// TODO output any kind of statistics / report? to git / readme + changelog
		if (writeChangeLog()) {
			// write entries to changeSet for "major" events like a new topic / view
			gitUtil.addToRepository(persister.outputDirectoryPathFor(config.OUTPUT_CHANGELOGS_DIRECTORY));
		}

		gitUtil.commitAndPush(persister.getFormattedCurrentTime());
	}

	private void writeAvroSchemas() {
		// TODO check if it actually changes before persisting
		for(String schemaHash : avroSchemas.keySet()) {
			persister.persistAvroSchema(schemaHash, avroSchemas.get(schemaHash));
		}
		
	}

	private void writeEventTypeReport() {
		// TODO check if it actually changes before persisting
		for (String eventType : eventTypeMap.keySet()) {
			List<Topic> topicList = eventTypeMap.get(eventType);
			if (topicList.size() > 1) {
				changeLog.addMessage("Found eventType " + eventType + " in " + topicList.size() + " topics");
			}
			persister.persistEventTypeReport(eventType, topicList);
		}
	}

	private boolean writeChangeLog() {
		if (!changeLog.hasEntries()) {
			log.info("No major changes detected, not writing a changelog for this run");
			return false;
		}
		// TODO add last n changelogs to README.md in reverse chronological order
		persister.persistChangeLog(changeLog);
		return true;
	}

	public void process(Topic topic) {

		log.info("Processing " + topic.getEvents().size() + " events for " + topic);

		markTopicInconsistencies(topic);
		updateEventTypeList(topic);
		updateAvroSchemaMap(topic);
		createDrillViews(topic);
		writeEventSamples(topic);
		writeEventStructures(topic);
		writeTopicReport(topic);
	}

	private void writeEventStructures(Topic topic) {
		persister.persistEventStructures(topic);
	}

	private void updateAvroSchemaMap(Topic topic) {
		
		for(Entry<String, Schema> schemaEntry : topic.getAvroSchemas().entrySet()) {
			String schemaHash = schemaEntry.getKey();
			if(schemaHash != null) {
				if(avroSchemas.get(schemaHash) != null && !avroSchemas.get(schemaHash).equals(schemaEntry.getValue())) {
					changeLog.addMessage("Found two different Avro Schemas for the same schema hash (" + schemaHash + ") in " + topic);
				}
				
				avroSchemas.put(schemaHash, schemaEntry.getValue());
			}
		}
	}

	private void updateEventTypeList(Topic topic) {
		for (String eventType : topic.getEventTypes()) {
			if (eventTypeMap.get(eventType) == null) {
				eventTypeMap.put(eventType, new ArrayList<>());
			}
			eventTypeMap.get(eventType).add(topic);
		}
	}

	private void writeTopicReport(Topic topic) {
		// TODO check local git repository if report even changed
		persister.persistTopicReport(topic);
	}

	private void writeEventSamples(Topic topic) {
		// TODO check local git repository if we already have enough example events (for
		// this schemaVersion) and don't persist more if we already do
		persister.persistEventSamples(topic);
	}

	private void createDrillViews(Topic topic) {
		// TODO compare and align generated views with those from drill
		log.info("Preparing Drill view for " + topic);

		Optional<String> currentViewFromRepository = loadDrillViewFromRepository(topic);
		if (currentViewFromRepository.isPresent()) {
			log.info("Found a view in local git repository");
		} else {
			log.info("No view found in local git repository");
		}

		// generate drill views and execute them
		if (topic.getExampleEvent() == null) {
			log.warn("Did not find an event to create a Drill view. Skipping drill view processing for " + topic);
			return;
		}

		log.info("Creating Drill view create SQL for " + topic);
		// TODO handle invalid events without eventType
		Optional<EventStructure> mergedEventStructuredOption = topic.getMergedEventStructured();
		if(!mergedEventStructuredOption.isPresent()) {
			throw new IllegalStateException("Topic does not provide a merged event structure even though it had an example event");
		}
		String viewFromCurrentRun = createViewSqlBuilder.generateDrillViewsFor(mergedEventStructuredOption.get());

		if (drillViews.doesViewExist(topic.getName())) {
			log.debug("Drill view for " + topic + " already exists");
			// check if it's the same view and don't execute if it is
			// compare with view from local git repository
			if (currentViewFromRepository.isPresent()) {
				if (currentViewFromRepository.get().equals(viewFromCurrentRun)) {
					log.info(
							"View from repository is the same as the view from current run, skipping further processing");
					return;
				} else {
					changeLog.addMessage("Drill view changed for " + topic);
					// TODO use git diff to see changes
				}
			}

		} else {
			log.info("No Drill view exists yet for " + topic);
			changeLog.addMessage("Genearting new Drill view for: " + topic);
		}

		// execute create statement on Drill
		try {
			drillConnection.executeSqlStatements(viewFromCurrentRun);
			// TODO run count on newly created view for sanity checking and report
		} catch (SQLException e) {
			throw new IllegalStateException("Error while executing create view SQL statement on Drill", e);
		}

		// write drill views to disk
		persister.persistDrillView(topic, viewFromCurrentRun);
	}

	private Optional<String> loadDrillViewFromRepository(Topic topic) {

		log.info("Loading drill view from local repository for " + topic);

		File drillViewFile = new File(config.GIT_LOCAL_REPOSITORY_PATH + File.separator + config.OUTPUT_DRILL_DIRECTORY
				+ File.separator + persister.fileNameForDrillView(topic));
		if (!drillViewFile.exists() || !drillViewFile.canRead()) {
			log.debug("Unable to load drill view from git repository as file either doesn't exist or can't be read at "
					+ drillViewFile.getAbsolutePath());
			return Optional.absent();
		} else {
			try {
				return Optional.of(FileUtils.readFileToString(drillViewFile));
			} catch (IOException e) {
				log.warn("Unable to read drill view from local git repository even though the file exists at: "
						+ drillViewFile.getAbsolutePath(), e);
				return Optional.absent();
			}
		}
	}

	private void markTopicInconsistencies(Topic topic) {

		topic.markInconsistencies();

		if (topic.getEvents().size() == 0) {
			changeLog.addMessage("No events received for " + topic);
		} else {
			if (topic.isConsistent()) {
				log.info("Consistency checks passed: " + topic);
			} else {
				changeLog.addMessage("Inconsistencies detected in " + topic);
			}
		}
	}

}
