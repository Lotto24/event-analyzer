package de.esailors.dataheart.drillviews.processor;

import java.sql.SQLException;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.esailors.dataheart.drillviews.conf.Config;
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

	public Processor(Config config, GitUtil gitUtil) {
		this.config = config;
		this.gitUtil = gitUtil;
		this.drillConnection = new DrillConnection(config);
		this.drillViews = new DrillViews(config, drillConnection);
		this.persister = new Persister(config);
		this.changeLog = new ChangeLog(config);
	}

	public void process(Set<Topic> topics) {

		for (Topic topic : topics) {
			process(topic);
		}

		log.info("Adding process output to local git repository");

		// push everything that is written to disk also to git
		gitUtil.addToRepository(config.OUTPUT_DRILL_DIRECTORY);
		gitUtil.addToRepository(config.OUTPUT_SAMPLES_DIRECTORY);
		gitUtil.addToRepository(config.OUTPUT_TOPIC_DIRECTORY);

		// TODO output any kind of statistics / report? to git / readme + changelog
		if (writeChangeLog()) {
			// write entries to changeSet for "major" events like a new topic / view
			gitUtil.addToRepository(config.OUTPUT_CHANGELOGS_DIRECTORY);
		}

		gitUtil.commitAndPush(persister.getFormattedCurrentTime());
	}

	private boolean writeChangeLog() {
		if (!changeLog.hasEntries()) {
			log.info("No major changes detected, not writing a changelog for this run");
			return false;
		}
		// TODO add last n changelogs to README.md in reverse chronological order
		persister.persistChangeLog(changeLog);
		return true;
	}

	public void process(Topic topic) {

		log.info("Processing " + topic.getEvents().size() + " events for " + topic);

		markTopicInconsistencies(topic);
		createDrillViews(topic);
		writeEventSamples(topic);
		writeTopicReport(topic);
	}

	private void writeTopicReport(Topic topic) {
		// TODO check local git repository if report even changed
		persister.persistTopicReport(topic);
	}

	private void writeEventSamples(Topic topic) {
		// TODO check local git repository if we already have enough example events (for
		// this schemaVersion) and don't persist more if we already do
		persister.persistEventSamples(topic);
	}

	private void createDrillViews(Topic topic) {
		// TODO compare and align generated views with those from drill

		// generate drill views and execute them
		if (topic.getExampleEvent() == null) {
			log.warn("Did not find an event to create a Drill view for " + topic);
		} else {

			if (drillViews.doesViewExist(topic.getName())) {
				log.debug("Drill view for " + topic + " already exists");
				// TODO check if it's the same view and don't execute if it is
				// TODO fetch view from local git repository and compare
			} else {
				log.info("No Drill view exists yet for " + topic);
				changeLog.addMessage("Genearting new Drill view for: " + topic);
			}

			log.info("Creating Drill views for " + topic);
			String createStatement = CreateViewSQLBuilder.generateDrillViewsFor(topic.getExampleEvent());
			// TODO check if view actually changed
			// write drill views to disk
			persister.persistDrillView(topic, createStatement);
			try {
				drillConnection.executeSqlStatements(createStatement);
				// TODO run count on newly created view for sanity checking and report
			} catch (SQLException e) {
//				log.error("Error while executing create view statement", e);
				throw new IllegalStateException("Error while executing create view statement on Drill", e);
			}
		}
	}

	private void markTopicInconsistencies(Topic topic) {

		topic.markInconsistencies();

		if (topic.getEvents().size() == 0) {
			changeLog.addMessage("No events received for " + topic);
		} else {
			if (topic.isConsistent()) {
				log.info("Consistency checks passed: " + topic);
			} else {
				changeLog.addMessage("Inconsistencies detected in " + topic);
			}
		}
	}

}
