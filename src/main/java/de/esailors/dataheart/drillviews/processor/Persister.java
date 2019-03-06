package de.esailors.dataheart.drillviews.processor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.esailors.dataheart.drillviews.conf.Config;
import de.esailors.dataheart.drillviews.data.Event;
import de.esailors.dataheart.drillviews.data.Topic;

public class Persister {

	private static final Logger log = LogManager.getLogger(Persister.class.getName());

	private Config config;

	public Persister(Config config) {
		this.config = config;
	}

	public void persistDrillView(Topic topic, String createStatement) {
		log.info("Writing drill view to disc for " + topic);
		FileWriterUtil.writeFile(config.OUTPUT_DRILL_DIRECTORY, topic.getTopicName() + ".sql", createStatement);
	}

	public void persistEventSamples(Topic topic) {
		String eventSample = "";
		int cnt = 0;
		for (Event event : topic.getEvents()) {
			eventSample += JsonPrettyPrinter.prettyPrintJsonString(event.getEventJson());
			cnt++;
			if (cnt >= 10) {
				// TODO move the limit to config?
				break;
			}
		}
		FileWriterUtil.writeFile(config.OUTPUT_SAMPLES_DIRECTORY, topic.getTopicName() + ".json", eventSample);

	}

}
