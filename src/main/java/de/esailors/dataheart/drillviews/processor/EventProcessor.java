package de.esailors.dataheart.drillviews.processor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.esailors.dataheart.drillviews.conf.Config;
import de.esailors.dataheart.drillviews.generator.CreateViewSQLBuilder;

public class EventProcessor {

	private static final Logger log = LogManager.getLogger(EventProcessor.class.getName());

	private Config config;
	private EventFactory eventFactory;
	private Map<String, List<Event>> events = new HashMap<>();

	public EventProcessor(Config config) {
		this.config = config;

		initEventFactory();
	}

	private void initEventFactory() {
		this.eventFactory = new EventFactory(config);
	}

	public void registerRecords(ConsumerRecords<byte[], byte[]> consumedRecords) {
		log.debug("Received records to process: " + consumedRecords.count());
		if (consumedRecords.count() == 0) {
			log.warn("Did not receive any event");
			// TODO handle this
		}
		for (ConsumerRecord<byte[], byte[]> record : consumedRecords) {
			registerRecord(record);
		}
	}

	private void registerRecord(ConsumerRecord<byte[], byte[]> record) {

		Event event = eventFactory.buildEvent(record);
		registerEvent(event);
	}

	private void registerEvent(Event event) {
		List<Event> eventList;
		if (events.get(event.getTopic()) == null) {
			eventList = new ArrayList<>();
			events.put(event.getTopic(), eventList);
		}
		events.get(event.getTopic()).add(event);
	}

	public void processRecordsFor(String topic) {

		log.info("Processing events for " + topic);

		if (events.get(topic).size() > 0) {
			// TODO handle the other events as well, also write samples
			// TODO THIS IS A PLACEHOLDER
			String createStatement = CreateViewSQLBuilder
					.generateDrillViewForJson(events.get(topic).get(0).getEventJson(), topic);
//			log.info(createStatement);

			// TODO execute statement on drill connection
			// TODO STOPPED HERE
		}
	}

}
