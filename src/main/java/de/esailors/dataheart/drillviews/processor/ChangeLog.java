package de.esailors.dataheart.drillviews.processor;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.esailors.dataheart.drillviews.conf.Config;

public class ChangeLog {

	private static final Logger log = LogManager.getLogger(ChangeLog.class.getName());

	private Config config;

	private List<String> messages = new ArrayList<>();

	public ChangeLog(Config config) {
		this.config = config;
	}

	public void addMessage(String message) {
		log.info("New ChangeSet entry: " + message);
		messages.add(message);
	}

	public List<String> getMessages() {
		return messages;
	}

	public boolean hasEntries() {
		return !messages.isEmpty();
	}

}
