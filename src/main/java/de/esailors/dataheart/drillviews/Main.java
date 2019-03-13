package de.esailors.dataheart.drillviews;

import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.esailors.dataheart.drillviews.conf.Config;
import de.esailors.dataheart.drillviews.data.Topic;
import de.esailors.dataheart.drillviews.git.GitUtil;
import de.esailors.dataheart.drillviews.kafka.KafkaEventFetcher;
import de.esailors.dataheart.drillviews.processor.Processor;

public class Main {

	private static final Logger log = LogManager.getLogger(Main.class.getName());

	private static final String DEFAULT_CONFIG_PATH = "conf/config.properties";

	public static void main(String[] args) {
		
//		Path sourcePath = Paths.get("structures/");
//		Path targetPath = Paths.get("schemas/target.file");
//		Path relative = sourcePath.relativize(targetPath);
//		System.out.println(relative.toString());
		
//		TreeFactory treeFactory = new TreeFactory();
//		TreePlotter treePlotter = new TreePlotter();
//		
//		String exampleEventType = "payment_payin_triggered";
//		
//		String exampleEventJsonString = FileUtils.readFileToString(new File("out/event_samples/" + exampleEventType + ".json"));
//		Tree jsonTree = treeFactory.buildTreeFromJsonString(exampleEventJsonString, exampleEventType);
//		treePlotter.plotTree(jsonTree);
//
//		
//		String schemaHash = "adc8b1f993297331b327a698b4b48319";
////		String schemaHash = "ded437dab71f948158ff066aa28010e2";
//		String schemaPath = "out/avro_schemas/" + schemaHash + ".json";
//		String avroSchemaJson = FileUtils.readFileToString(new File(schemaPath));
//		
//		Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);
////		System.out.println(avroSchema);
//		
//		
//		Tree schemaTree = treeFactory.buildTreeFromAvroSchema(avroSchema);
//		treePlotter.plotTree(schemaTree);
//		
//		if(1==1) throw new IllegalStateException("testing");
		
		log.info("Starting DrillViewGenerator");

		// load configuration
		String configPath = DEFAULT_CONFIG_PATH;
		if (args.length > 0) {
			configPath = args[0];
			log.debug("Using config path from command line argument: " + configPath);
		}
		Config config = new Config(configPath);

		// inititalize local git repository
		GitUtil gitUtil = new GitUtil(config);
		
//		 fetch messages from all Topics and parse to Event
		Set<Topic> topics = new KafkaEventFetcher(config).fetchEvents();

		// process the fetched messages
		// align existing Drill views with fetched events
		// write report, views and sample data 
		// publish to git for others to see
		new Processor(config, gitUtil).process(topics);
		

		log.info("DrillViewGenerator finished successfully");

	}


}
