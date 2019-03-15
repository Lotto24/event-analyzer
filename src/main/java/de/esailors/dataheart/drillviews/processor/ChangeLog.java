package de.esailors.dataheart.drillviews.processor;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.esailors.dataheart.drillviews.conf.Config;

public class ChangeLog {

	private static final Logger log = LogManager.getLogger(ChangeLog.class.getName());

	 // separate warnings (no events found) from changes (new view)
	private List<String> changes = new ArrayList<>();
	private List<String> warnings = new ArrayList<>();

	public ChangeLog() {
	}

	public void addChange(String change) {
		log.info("New Change: " + change);
		changes.add(change);
	}

	public List<String> getChanges() {
		return changes;
	}

	public boolean hasChanges() {
		return !changes.isEmpty();
	}

	public void addWarning(String warning) {
		log.warn("New Warning: " + warning);
		warnings.add(warning);
	}
	
	public List<String> getWarnings() {
		return warnings;
	}

	public boolean hasWarnings() {
		return !warnings.isEmpty();
	}
}
