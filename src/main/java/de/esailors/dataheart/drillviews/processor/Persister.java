package de.esailors.dataheart.drillviews.processor;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.esailors.dataheart.drillviews.conf.Config;
import de.esailors.dataheart.drillviews.data.Event;
import de.esailors.dataheart.drillviews.data.Topic;

public class Persister {

	// TODO move the limit to config?
	private static final int MAXIMUM_SAMPLES_TO_PERSIST = 10;

	private static final Logger log = LogManager.getLogger(Persister.class.getName());

	private Config config;

	// TODO set this globally for whole run, maybe in Persister or something
	private String formattedCurrentTime;

	public Persister(Config config) {
		this.config = config;

		formattedCurrentTime = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date());
	}

	public void persistDrillView(Topic topic, String createStatement) {
		log.info("Writing drill view to disc for " + topic);
		FileWriterUtil.writeFile(config.OUTPUT_DRILL_DIRECTORY, topic.getName() + ".sql", createStatement);
	}

	public void persistEventSamples(Topic topic) {
		String eventSample = "";
		int cnt = 0;
		for (Event event : topic.getEvents()) {
			eventSample += JsonPrettyPrinter.prettyPrintJsonString(event.getEventJson());
			cnt++;
			if (cnt >= MAXIMUM_SAMPLES_TO_PERSIST) {
				break;
			}
		}
		FileWriterUtil.writeFile(config.OUTPUT_SAMPLES_DIRECTORY, topic.getName() + ".json", eventSample);

	}

	public void persistChangeLog(ChangeLog changeLog) {

		if (!changeLog.hasEntries()) {
			return;
		}
		// TODO more of a placeholder for now
		// changeSet should be part of README and update in descending chronological
		// order with nice markdown formatting
		String changeSetContent = "## " + formattedCurrentTime + " ChangeLog:\n\n";
		for (String message : changeLog.getMessages()) {
			changeSetContent += "* " + message + "\n";
		}

		String changeSetFile = "changelog_" + formattedCurrentTime + ".md";

		FileWriterUtil.writeFile(config.OUTPUT_CHANGELOGS_DIRECTORY, changeSetFile, changeSetContent);
	}

	public void persistTopicReport(Topic topic) {
		log.info("Writing topic report for: " + topic.getName());
		String reportContent = "# Topic report for: " + topic.getName() + "\n";
		
		if(topic.isConsistent()) {
			reportContent += "## Topic was consistent\n\n";
		} else {
			reportContent += "## Topic was **NOT** consistent!\n\n";
		}
		if(!topic.getReportMessages().isEmpty()) {
			reportContent += "### Report messages:\n";
			for(String reportMessage : topic.getReportMessages()) {
				reportContent += "* " + reportMessage + "\n";
			}
			reportContent += "\n\n";
		}
		
		reportContent += "### Topic information:\n\n";
		reportContent += "* EventType: " + String.join(", ", topic.getEventTypes()) + "\n";
		reportContent += "* Schema Versions: " + String.join(", ", topic.getSchemaVersions()) + "\n";
		reportContent += "* Is Avro: " + String.join(", ", toStringCollection(topic.getMessagesAreAvro())) + "\n";
		reportContent += "* Avro Schema Hashes: " + String.join(", ", toStringCollection(topic.getAvroSchemaHashes())) + "\n";
		reportContent += "* Avro Schemas: " + String.join(", ", toStringCollection(topic.getMessageSchemas())) + "\n";
		
		FileWriterUtil.writeFile(config.OUTPUT_TOPIC_DIRECTORY, topic.getName() + ".md", reportContent);
	}

	private Collection<String> toStringCollection(Collection<?> collection) {
		// doesn't really belong here and there's probably a better way of joining the Boolean Set
		List<String> r = new ArrayList<>();
		collection.forEach((item) -> r.add((item == null ? "null" : item.toString())));
		return r;
	}

}
