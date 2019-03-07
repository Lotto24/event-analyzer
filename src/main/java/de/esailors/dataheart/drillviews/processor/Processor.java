package de.esailors.dataheart.drillviews.processor;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.JsonNode;

import de.esailors.dataheart.drillviews.conf.Config;
import de.esailors.dataheart.drillviews.data.Event;
import de.esailors.dataheart.drillviews.data.Topic;
import de.esailors.dataheart.drillviews.drill.DrillConnection;
import de.esailors.dataheart.drillviews.drill.DrillViews;
import de.esailors.dataheart.drillviews.git.GitUtil;

public class Processor {

	private static final Logger log = LogManager.getLogger(Processor.class.getName());

	private Config config;

	private DrillConnection drillConnection;
	private DrillViews drillViews;
	private Persister persister;
	private GitUtil gitUtil;
	private ChangeLog changeLog;

	public Processor(Config config) {
		this.config = config;
		this.drillConnection = new DrillConnection(config);
		this.drillViews = new DrillViews(config, drillConnection);
		this.persister = new Persister(config);
		this.gitUtil = new GitUtil(config);
		this.changeLog = new ChangeLog(config);
	}

	public void process(Set<Topic> topics) {

		for (Topic topic : topics) {
			process(topic);
		}

		// push everything that is written to disk also to git
		gitUtil.addToRepository(config.OUTPUT_DRILL_DIRECTORY);
		gitUtil.addToRepository(config.OUTPUT_SAMPLES_DIRECTORY);

		// TODO output any kind of statistics / report? to git / readme + changelog
		if (writeChangeLog()) {
			gitUtil.addToRepository(config.OUTPUT_CHANGELOGS_DIRECTORY);
		}

		gitUtil.commitAndPush();
	}

	private boolean writeChangeLog() {
		if (!changeLog.hasEntries()) {
			log.info("No major changes detected, not writing a changelog for this run");
			return false;
		}
		persister.persistChangeLog(changeLog);
		return true;
	}

	public void process(Topic topic) {

		// TODO write entries to changeSet for "major" events like a new topic / view
		// being detected

		log.info("Processing " + topic.getEvents().size() + " events for " + topic);
		if (topic.getEvents().size() == 0) {
			changeLog.addMessage("No events received for " + topic);
		} else {
			markTopicInconsistencies(topic);
		}

		createDrillViews(topic);
		writeEventSamples(topic);
		writeTopicReport(topic);
	}

	private void writeTopicReport(Topic topic) {
		// TODO STOPPED HERE
	}

	private void writeEventSamples(Topic topic) {
		persister.persistEventSamples(topic);
	}

	private void createDrillViews(Topic topic) {
		// TODO compare and align generated views with those from drill

		// generate drill views and execute them
		if (topic.getExampleEvent() == null) {
			log.warn("Did not find an event to create a Drill view for " + topic);
		} else {

			if (drillViews.doesViewExist(topic.getTopicName())) {
				log.debug("Drill view for " + topic + " already exists");
				// TODO check if it's the same view and don't execute if it is
			} else {
				log.info("No Drill view exists yet for " + topic);
				changeLog.addMessage("Genearting new Drill view for: " + topic);
			}

			log.info("Creating Drill views for " + topic);
			String createStatement = CreateViewSQLBuilder.generateDrillViewsFor(topic.getExampleEvent());
			// write drill views to disk
			persister.persistDrillView(topic, createStatement);
			try {
				drillConnection.executeSqlStatements(createStatement);
			} catch (SQLException e) {
//				log.error("Error while executing create view statement", e);
				throw new IllegalStateException("Error while executing create view statement on Drill", e);
			}
		}
	}

	private void markTopicInconsistencies(Topic topic) {
		// TODO move this to Topic?

		// check consistency within topics
		// - each topic only has avro / json - if avro always the same schema?
		// - JSON structure doesnt change (always the same fields?)
		// - always the same event Type in each topic

		if (topic.getEvents().size() < 2) {
			log.warn("Can't properly check event consistency because I did not get enough events for " + topic);
		}

		// idea: gather values in Sets, if they have more than 1 entry afterwards
		// something is fishy
		Set<Boolean> messagesAreAvro = new HashSet<>();
		Set<Schema> messageSchemas = new HashSet<>();
		Set<String> eventTypes = new HashSet<>();
		Set<String> schemaVersions = new HashSet<>();

		Event firstEvent = null;
		Iterator<Event> iterator = topic.getEvents().iterator();
		while (iterator.hasNext()) {
			Event event = iterator.next();

			messagesAreAvro.add(event.isAvroMessage());
			messageSchemas.add(event.getSchema());
			eventTypes.add(getEventTypeFromEvent(event));
			schemaVersions.add(getSchemaVersionFromEvent(event));

			if (firstEvent == null) {
				// first one we see, use this to compare to all others
				firstEvent = event;
				topic.setExampleEvent(event);
//			} else {
//				if (event.isAvroMessage() != firstEvent.isAvroMessage()) {
//					log.warn("Mixed Avro and plain JSON within the same topic: " + topic);
//				}
			}

		}

		// TODO mark these findings either in Processor or in Topic
		if (messagesAreAvro.size() > 1) {
			log.warn("Mixed Avro and plain JSON within the same topic: " + topic);
		}
		if (messageSchemas.size() > 1) {
			log.warn("Mixed Avro schemas within the same topic: " + topic);
		}
		if (eventTypes.size() > 1) {
			log.warn("Mixed EventTypes within the same topic: " + topic);
		}
		if (schemaVersions.size() > 1) {
			log.warn("Mixed Versions within the same topic: " + topic);
		}

		topic.setMessagesAreAvro(messagesAreAvro);
		topic.setMessageSchemas(messageSchemas);
		topic.setEventTypes(eventTypes);
		topic.setSchemaVersions(schemaVersions);

		if (topic.isConsistent()) {
			log.info("Consistency checks passed: " + topic);
		} else {
			String message = "Inconsistencies detected in " + topic;
			log.warn(message);
			changeLog.addMessage(message);
		}
	}

	private String getEventTypeFromEvent(Event event) {
		// TODO doesn't really belong here, move somewhere else
		return getFieldFromEvent(event, config.EVENT_FIELD_EVENT_TYPE);
	}

	private String getSchemaVersionFromEvent(Event event) {
		// TODO doesn't really belong here, move somewhere else
		return getFieldFromEvent(event, config.EVENT_FIELD_VERSION);
	}

	private String getFieldFromEvent(Event event, String field) {
		JsonNode jsonNode = event.getEventJson().get(field);
		if (jsonNode == null) {
			log.warn("Field not found for '" + field + "' in: " + event);
			return null;
		}
		return jsonNode.asText();
	}

}
