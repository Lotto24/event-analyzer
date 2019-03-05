package de.esailors.dataheart.drillviews.processor;

import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.esailors.dataheart.drillviews.conf.Config;
import de.esailors.dataheart.drillviews.data.Topic;
import de.esailors.dataheart.drillviews.drill.DrillViews;

public class Processor {
	
	private static final Logger log = LogManager.getLogger(Processor.class.getName());

	private DrillViews drillViews;

	public Processor(Config config) {
		this.drillViews = new DrillViews(config);
	}
	
	public void process(Set<Topic> topics) {
		
		for(Topic topic : topics) {
			process(topic);
		}
		
	}
	
	public void process(Topic topic) {

		log.info("Processing " + topic.getEvents().size() + " events for " + topic);

		// TODO check consistency within topics
		// - each topic only has avro / json 
		// - JSON structure doesnt change (always the same fields?)
		// - always the same event Type in each topic
//		checkTopicConsistency(topic);
		
		
		// TODO generate drill views and execute them
//			String createStatement = CreateViewSQLBuilder
//					.generateDrillViewForJson(events.get(topic).get(0).getEventJson(), topic.getTopicName());
		// TODO generate drill views from avro

		
		// TODO output any kind of statistics / report? (either to disk or mail / whatever)
		
		// TODO write drill views to disk
		// TODO write topic info to disk?
		// TODO write event samples to disk
//		writeSamplesToDisk();
		
		// TODO push everything that is written to disk also to git?
		
	}

}
