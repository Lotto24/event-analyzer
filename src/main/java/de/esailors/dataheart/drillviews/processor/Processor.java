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
import de.esailors.dataheart.drillviews.generator.CreateViewSQLBuilder;

public class Processor {

	private static final Logger log = LogManager.getLogger(Processor.class.getName());

	private Config config;

	private DrillConnection drillConnection;
	private DrillViews drillViews;

	public Processor(Config config) {
		this.config = config;
		this.drillConnection = new DrillConnection(config);
		this.drillViews = new DrillViews(config, drillConnection);
	}

	public void process(Set<Topic> topics) {

		for (Topic topic : topics) {
			process(topic);
		}

	}

	public void process(Topic topic) {

		log.info("Processing " + topic.getEvents().size() + " events for " + topic);

		markTopicInconsistencies(topic);

		createDrillViews(topic);

		// TODO output any kind of statistics / report? (either to disk or mail /
		// whatever)

		// TODO write drill views to disk
		// TODO write topic info to disk?
		// TODO write event samples to disk
//		writeSamplesToDisk();

		// TODO push everything that is written to disk also to git?

	}

	private void createDrillViews(Topic topic) {
		// TODO compare and align generated views with those from drill

		// TODO generate drill views and execute them
		if (topic.getExampleEvent() == null) {
			log.warn("Did not find an event to create a Drill view for " + topic);
		} else {
			String createStatement = CreateViewSQLBuilder.generateDrillViewsFor(topic.getExampleEvent());
			log.info("Creating Drill views for " + topic);
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

		Event firstEvent = null;
		Iterator<Event> iterator = topic.getEvents().iterator();
		while (iterator.hasNext()) {
			Event event = iterator.next();

			messagesAreAvro.add(event.isAvroMessage());
			messageSchemas.add(event.getSchema());
			eventTypes.add(getEventTypeFromEvent(event));

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

		topic.setMessagesAreAvro(messagesAreAvro);
		topic.setMessageSchemas(messageSchemas);
		topic.setEventTypes(eventTypes);

		if (topic.isConsistent()) {
			log.info("Consistency checks passed: " + topic);
		} else {
			log.warn("Inconsistencies detected in " + topic);
		}
	}

	private String getEventTypeFromEvent(Event event) {
		// TODO doesn't really belong here, move somewhere else
		JsonNode jsonNode = event.getEventJson().get(config.EVENT_FIELD_EVENT_TYPE);
		if (jsonNode == null || !jsonNode.isTextual()) {
			log.warn("No field for event type '" + config.EVENT_FIELD_EVENT_TYPE + "' found in: " + event);
			return null;
		}
		return jsonNode.getTextValue();
	}

}
