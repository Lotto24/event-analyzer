package de.esailors.dataheart.drillviews;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.esailors.dataheart.drillviews.conf.Config;
import de.esailors.dataheart.drillviews.data.Topic;
import de.esailors.dataheart.drillviews.kafka.KafkaEventFetcher;
import de.esailors.dataheart.drillviews.processor.Processor;

public class Main {

	private static final Logger log = LogManager.getLogger(Main.class.getName());

	private static final String DEFAULT_CONFIG_PATH = "conf/config.properties";

	public static void main(String[] args) throws FileNotFoundException, IOException {

		log.info("Starting DrillViewGenerator");

		String configPath = DEFAULT_CONFIG_PATH;
		if (args.length > 0) {
			configPath = args[0];
			log.debug("Using config path from command line argument: " + configPath);
		}

		// load config
		Config config = new Config(configPath);

		// fetch messages from all Topics and parse to Event
		Set<Topic> topics = new KafkaEventFetcher(config).fetchEvents();

		// align existing Drill views with fetched events
		new Processor(config).process(topics);

		log.info("DrillViewGenerator finished successfully");

	}

}
