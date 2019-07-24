package de.esailors.dataheart.drillviews;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;

import com.google.common.base.Optional;

import de.esailors.dataheart.drillviews.conf.Config;
import de.esailors.dataheart.drillviews.data.Topic;
import de.esailors.dataheart.drillviews.kafka.KafkaEventFetcher;
import de.esailors.dataheart.drillviews.kafka.KafkaEventFetcherFactory;
import de.esailors.dataheart.drillviews.kafka.KafkaTopicsExplorer;
import de.esailors.dataheart.drillviews.processor.Processor;
import de.esailors.dataheart.drillviews.util.GitRepository;

/**
 *
 * @author andre.mis
 */
public class Main {

	private static final Logger log = LogManager.getLogger(Main.class.getName());

	private static final String DEFAULT_CONFIG_PATH = "conf/config.properties";

	public static void main2(String[] args) {

		initLog4j();
		initConfig(args);
		
		System.out.println("Creating service");
		ExecutorService executorService = Executors.newFixedThreadPool(2, new KafkaEventFetcherFactory());
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}

		for (int i = 0; i < 10; i++) {
			AtomicInteger counter = new AtomicInteger(i);
			System.out.println("Submitting task " + i);
			executorService.submit(createTask(counter));
		}
		System.out.println("Done submitting: " + executorService.isTerminated() + " " + executorService.isShutdown());
		executorService.shutdown();
		try {
			executorService.awaitTermination(10, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}

	private static ThreadFactory createThreadFactory() {
		return new ThreadFactory() {

			private int threadCount = 0;
			
			@Override
			public Thread newThread(Runnable r) {
				System.out.println("Creating thread");
				Thread t = new Thread(r);
				t.setName("CustomThread-" + threadCount);
				threadCount++;
				return t;
			}
			
		};
	}

	private static Runnable createTask(AtomicInteger counter) {
		return new Runnable() {

			@Override
			public void run() {
				Thread currentThread = Thread.currentThread();
				String threadName = currentThread.getName() + " " + currentThread.getId();
				System.out.println(threadName + " running with " + currentThread.getClass());
				KafkaEventFetcher myThread = (KafkaEventFetcher) currentThread;
//				myThread.doStuff(threadName, counter);
				int current;
				while((current = counter.getAndIncrement()) < 10) {
					System.out.println(threadName + ": " + current);
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				System.out.println(threadName + " done");
			}

		};
	}

	public static void main(String[] args) {

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
		// - use some config library instead of the homebrewn one

		initLog4j();
		initConfig(args);
		log.info("Starting Event Analyzer");

		// inititalize local git repository
		Optional<GitRepository> gitRepositoryOption;
		if (Config.getInstance().GIT_ENABLED) {
			gitRepositoryOption = Optional.of(new GitRepository());
		} else {
			log.warn("Local git repository for analysis output is not enabled");
			gitRepositoryOption = Optional.absent();
		}

//		 fetch messages from all Topics and parse to Event
		KafkaTopicsExplorer kafkaExplorer = new KafkaTopicsExplorer();
		Set<Topic> topics = kafkaExplorer.fetchEvents();
		kafkaExplorer.close();

		// process the fetched messages
		// align existing Drill views with fetched events
		// write report, views and sample data
		// publish to git for others to see
		new Processor(gitRepositoryOption).process(topics);

		log.info("DrillViewGenerator finished successfully");

	}

	private static void initConfig(String[] args) {
		String configPath = DEFAULT_CONFIG_PATH;
		if (args.length > 0) {
			configPath = args[0];
			log.debug("Using config path from command line argument: " + configPath);
		}
		Config.load(configPath);
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
