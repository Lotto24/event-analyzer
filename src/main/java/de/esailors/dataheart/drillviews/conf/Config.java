package de.esailors.dataheart.drillviews.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Config {

	private static final Logger log = LogManager.getLogger(Config.class.getName());

	/* CONFIG KEYS */

	// drill config

	public static final String DRILL_JDBC_URL_KEY = "drill.jdbc.url";

	public static final String DRILL_VIEW_ALL_DATABASE_KEY = "drill.view.all.database";
	public static final String DRILL_VIEW_DAY_DATABASE_KEY = "drill.view.day.database";
	public static final String DRILL_VIEW_WEEK_DATABASE_KEY = "drill.view.week.database";

	// processor config

	public static final String CONSUL_HOST_KEY = "consul.host";
	public static final String CONSUL_PORT_KEY = "consul.port";

	public static final String EVENT_FIELD_EVENT_TYPE_KEY = "events.event_type_field";
	public static final String PROCESSOR_SCHEMA_HASH_LENGTH_KEY = "events.schema_hash_length";

	/* kafka config keys */

	public static final String MAX_RUNTIME_MS_KEY = "kafka.max.runtime";
	public static final String MAX_RECORDS_TO_CONSUME_KEY = "kafka.max.records";
	public static final String KAFKA_POLLS_BEFORE_GIVING_UP_KEY = "kafka.max.polls_without_records";

	public static final String KAFKA_CONSUMER_BOOTSTRAP_SERVERS_KEY = "kafka.consumer.bootstrapserver";
	public static final String KAFKA_CONSUMER_GROUP_ID_KEY = "kafka.consumer.group_id";

	public static final String KAFKA_CONSUMER_POLL_TIMEOUT_KEY = "kafka.consumer.poll.timeout";
	public static final String KAFKA_CONSUMER_POLL_MIN_DELAY_KEY = "kafka.consumer.poll.min_delay";

	public static final String KAFKA_CONSUMER_AUTO_OFFSET_RESET_KEY = "kafka.consumer.auto_offset_reset";
	public static final String KAFKA_CONSUMER_ENABLE_AUTO_COMMIT_KEY = "kafka.consumer.enable_autocommit";
	public static final String KAFKA_CONSUMER_MAX_POLL_RECORDS_KEY = "kafka.consumer.max_poll_records";
	public static final String KAFKA_CONSUMER_FETCH_MIN_BYTES_KEY = "kafka.consumer.fetch_min_bytes";
	public static final String KAFKA_CONSUMER_FETCH_MAX_WAIT_MS_KEY = "kafka.consumer.fetch_max_wait";

	public static final String KAFKA_RESET_CONSUMER_OFFSETS_KEY = "kafka.consumer.reset_offsets";
	public static final String KAFKA_ASSIGN_TO_ALL_TOPICS_KEY = "kafka.topics.assign_to_all";

	/* CONFIG DEFAULT VALUES */

	// drill settings

	public String DRILL_JDBC_URL = "jdbc:drill:zk=hdp-master-01.t24.stagec.sg-cloud.co.uk:2181,hdp-master-02.t24.stagec.sg-cloud.co.uk:2181,hdp-master-03.t24.stagec.sg-cloud.co.uk:2181/drill/drillbits1";

	public String DRILL_VIEW_ALL_DATABASE = "drill.json_events";
	public String DRILL_VIEW_DAY_DATABASE = "drill.json_events_last_day";
	public String DRILL_VIEW_WEEK_DATABASE = "drill.json_events_last_week";

	// processor settings

	public String CONSUL_HOST = "app-01-dwh.test.t24.eu-west-1.sg-cloud.co.uk";
	public int CONSUL_PORT = 8500;

	public int PROCESSOR_SCHEMA_HASH_LENGTH = 32;
	public String EVENT_FIELD_EVENT_TYPE = "eventType";

	// kafka connection settings

	public String KAFKA_CONSUMER_BOOTSTRAP_SERVERS = "app-01-dwh.test.t24.eu-west-1.sg-cloud.co.uk:9092";
	public String KAFKA_CONSUMER_GROUP_ID = "DataHeartHBaseConnector";

	public int KAFKA_CONSUMER_POLL_TIMEOUT = 1000;

	// kafka consumer config

	public String KAFKA_CONSUMER_AUTO_OFFSET_RESET = "earliest";
	public boolean KAFKA_CONSUMER_ENABLE_AUTO_COMMIT = false;
	public int KAFKA_CONSUMER_MAX_POLL_RECORDS = 1;

	public boolean KAFKA_RESET_CONSUMER_OFFSETS = false;

	// config internal fields

	private String configFileName;
	private Properties configProperties;

	public Config(String configFileName) {
		this.configFileName = configFileName;

		try {
			loadConfig();
		} catch (IOException e) {
			throw new IllegalStateException("Unable to load config from: " + configFileName, e);
		}
	}

	public void loadConfig() throws FileNotFoundException, IOException {
		log.info("Loading config file from: " + configFileName);

		File configFile = new File(configFileName);
		if (!configFile.exists()) {
			throw new IllegalArgumentException("Unable to load config file, file does not exist: " + configFileName);
		}

		FileInputStream fileInputStream = new FileInputStream(configFile);
		configProperties = new Properties();
		configProperties.load(fileInputStream);
		fileInputStream.close();

		loadConfigValues();
	}

	private void loadConfigValues() {
		loadConsulSettings();
		loadDrillSettings();
		loadKafkaSettings();
		loadProcessorSettings();
	}

	private void loadProcessorSettings() {
		PROCESSOR_SCHEMA_HASH_LENGTH = loadConfigInteger(PROCESSOR_SCHEMA_HASH_LENGTH_KEY);
		EVENT_FIELD_EVENT_TYPE = loadConfigString(EVENT_FIELD_EVENT_TYPE_KEY);
	}

	private void loadConsulSettings() {
		CONSUL_HOST = loadConfigString(CONSUL_HOST_KEY);
		CONSUL_PORT = loadConfigInteger(CONSUL_PORT_KEY);
	}

	private void loadDrillSettings() {
		DRILL_JDBC_URL = loadConfigString(DRILL_JDBC_URL_KEY);
		DRILL_VIEW_ALL_DATABASE = loadConfigString(DRILL_VIEW_ALL_DATABASE_KEY);
		DRILL_VIEW_DAY_DATABASE = loadConfigString(DRILL_VIEW_DAY_DATABASE_KEY);
		DRILL_VIEW_WEEK_DATABASE = loadConfigString(DRILL_VIEW_WEEK_DATABASE_KEY);
	}

	private void loadKafkaSettings() {
		KAFKA_CONSUMER_BOOTSTRAP_SERVERS = loadConfigString(KAFKA_CONSUMER_BOOTSTRAP_SERVERS_KEY);
		KAFKA_CONSUMER_GROUP_ID = loadConfigString(KAFKA_CONSUMER_GROUP_ID_KEY);

		KAFKA_CONSUMER_POLL_TIMEOUT = loadConfigInteger(KAFKA_CONSUMER_POLL_TIMEOUT_KEY);

		KAFKA_CONSUMER_AUTO_OFFSET_RESET = loadConfigString(KAFKA_CONSUMER_AUTO_OFFSET_RESET_KEY);
		KAFKA_CONSUMER_ENABLE_AUTO_COMMIT = loadConfigBoolean(KAFKA_CONSUMER_ENABLE_AUTO_COMMIT_KEY);
		KAFKA_CONSUMER_MAX_POLL_RECORDS = loadConfigInteger(KAFKA_CONSUMER_MAX_POLL_RECORDS_KEY);
	}

	private boolean loadConfigBoolean(String configKey) {
		String configString = loadConfigString(configKey);
		if (!configString.equals(new Boolean(true).toString()) && !configString.equals(new Boolean(false).toString())) {
			throw new IllegalArgumentException(
					"Unable to parse boolean value from config: " + configKey + " was " + configString);
		}
		return Boolean.valueOf(configString);
	}

	private int loadConfigInteger(String configKey) {
		String configString = loadConfigString(configKey);

		try {
			return Integer.parseInt(configString);
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException(
					"Unable to parse integer value from config: " + configKey + " was " + configString);
		}

	}

	private String loadConfigString(String configKey) {
		String configValue = configProperties.getProperty(configKey);
		log.debug(" * " + configKey + ": " + configValue);

		if (configValue == null) {
			throw new IllegalArgumentException("Unable to load config value, key is not defined: " + configKey);
		}

		return configValue.trim();
	}
}
