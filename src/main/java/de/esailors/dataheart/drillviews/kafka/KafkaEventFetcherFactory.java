package de.esailors.dataheart.drillviews.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KafkaEventFetcherFactory implements ThreadFactory {
	
	private static final Logger log = LogManager.getLogger(KafkaEventFetcherFactory.class.getName());

	private List<KafkaEventFetcher> createdInstances = new ArrayList<>();
	private boolean open = true;

	@Override
	public Thread newThread(Runnable r) {
		if(!open) {
			throw new IllegalStateException("Got asked to create a thread but I'm closed already");
		}
		int createdThreads = createdInstances.size();
		log.info("Creating " + createdThreads + "th KafkaEventFetcher thread for");
		KafkaEventFetcher kafkaEventFetcher = new KafkaEventFetcher(r, "fetcher-" + createdThreads);
		createdInstances.add(kafkaEventFetcher);
		return kafkaEventFetcher;
	}
	
	public void close() {
		if(!open || createdInstances == null) {
			log.debug("Already closed");
			return;
		}
		log.info("Closing " + createdInstances.size() + " kafka event fetchers");
		for(KafkaEventFetcher instance : createdInstances) {
			instance.close();
		}
		createdInstances.clear();
		createdInstances = null;
		open = false;
	}

}
