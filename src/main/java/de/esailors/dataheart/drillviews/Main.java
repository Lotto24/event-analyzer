package de.esailors.dataheart.drillviews;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.esailors.dataheart.drillviews.conf.Config;
import de.esailors.dataheart.drillviews.kafka.EventFetcher;

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

		Config config = new Config(configPath);
		
		EventFetcher kafkaConnection = new EventFetcher(config);
		try {
			kafkaConnection.processTopicList();
		} catch (InterruptedException e) {
			log.error("Error while consuming from Kafka", e);
		}
		
		
//		DrillViewGenerator drillViewGenerator = new DrillViewGenerator(config);

		log.info("DrillViewGenerator finished successfully");

	}

}
