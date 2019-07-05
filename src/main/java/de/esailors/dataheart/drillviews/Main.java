package de.esailors.dataheart.drillviews;

import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;

import com.google.common.base.Optional;

import de.esailors.dataheart.drillviews.conf.Config;
import de.esailors.dataheart.drillviews.data.Topic;
import de.esailors.dataheart.drillviews.kafka.KafkaEventFetcher;
import de.esailors.dataheart.drillviews.processor.Processor;
import de.esailors.dataheart.drillviews.util.GitRepository;

/**
 *
 * @author andre.mis
 */
public class Main {

	private static final Logger log = LogManager.getLogger(Main.class.getName());

	private static final String DEFAULT_CONFIG_PATH = "conf/config.properties";

	public static void main(String[] args) {

//		String file = "/home/andre.mis/git/event-analyzer/analysis_repository/event_structures/AgeVerificationResult/AgeVerificationResult.tree";
//		FileInputStream fileInputStream;
//		try {
//			fileInputStream = new FileInputStream(file);
//			ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
//			Tree read = (Tree) objectInputStream.readObject();
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (ClassNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//
//		if (1 == 1)
//			throw new IllegalStateException("testing");

		// Some ideas for possible improvements:
		// - fields report, maybe overkill but could be very useful
		// - using (git) diff to see changes between versions of drill views
		// - add last n changelogs to README.md in reverse chronological order
		// - a check to see if different event structures are "compatible"
		// - add more statistics or a report for each run
		// - link avro schemas to official eSailors/kafka-events repo
		// - on live we find both 'AgeVerification' and 'ageVerification' -> unify
		// - refactor inconsistency checking to separate class
		// - check for more kinds of event invalidity and add cause enum
		// - extract markdown specifics to separate class and
		// - refactor persister to separate report generation from path handling
		// - for nodes that are Avro MAPS we need to select the whole blob in views
		// - persist event samples separately for each event structure
		// - persist event structure plots somewhere more suitable than git
		// - when pulling from git only fetch the branch we actually work on

		initLog4j();

		log.info("Starting Event Analyzer");

		// load configuration
		String configPath = DEFAULT_CONFIG_PATH;
		if (args.length > 0) {
			configPath = args[0];
			log.debug("Using config path from command line argument: " + configPath);
		}
		Config.load(configPath);

		// inititalize local git repository
		Optional<GitRepository> gitRepositoryOption;
		if (Config.getInstance().GIT_ENABLED) {
			gitRepositoryOption = Optional.of(new GitRepository());
		} else {
			log.warn("Local git repository for analysis output is not enabled");
			gitRepositoryOption = Optional.absent();
		}

//		 fetch messages from all Topics and parse to Event
		Set<Topic> topics = new KafkaEventFetcher().fetchEvents();

		// process the fetched messages
		// align existing Drill views with fetched events
		// write report, views and sample data
		// publish to git for others to see
		new Processor(gitRepositoryOption).process(topics);

		log.info("DrillViewGenerator finished successfully");

	}

	private static void initLog4j() {
		// inspired by
		// https://stackoverflow.com/questions/30120330/log4j2-unable-to-register-shutdown-hook-because-jvm-is-shutting-down

		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				log.info("Shutting down");
				// Shut down everything (e.g. threads) that you need to.

				// then shut down log4j
				if (LogManager.getContext() instanceof LoggerContext) {
					log.debug("Shutting down log4j2");
					Configurator.shutdown((LoggerContext) LogManager.getContext());
				} else
					log.warn("Unable to shutdown log4j2");

				// logger not usable anymore
				System.out.println("Done");
			}
		});
	}

}
