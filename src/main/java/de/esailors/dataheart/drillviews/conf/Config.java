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
	private static final String DRILL_ENABLED_KEY = "drill.enabled";
	private static final String DRILL_JDBC_URL_KEY = "drill.jdbc.url";
	private static final String DRILL_JDBC_USER_KEY = "drill.jdbc.user";
	private static final String DRILL_JDBC_PASSWORD_KEY = "drill.jdbc.password";
	private static final String DRILL_VIEW_HBASE_TABLE_KEY = "drill.view.hbase.table";
	private static final String DRILL_VIEW_HBASE_COLUMN_FAMILY_KEY = "drill.view.hbase.column_family";
	private static final String DRILL_VIEW_HBASE_JSON_FIELD_KEY = "drill.view.hbase.json_field";
	private static final String DRILL_VIEW_HBASE_STORAGE_PLUGIN_NAME_KEY = "drill.view.hbase.storage_plugin";
	private static final String DRILL_VIEW_ALL_DATABASE_KEY = "drill.view.all.database";
	private static final String DRILL_VIEW_DAY_DATABASE_KEY = "drill.view.day.database";
	private static final String DRILL_VIEW_WEEK_DATABASE_KEY = "drill.view.week.database";

	// hive config
	private static final String HIVE_ENABLED_KEY = "hive.enabled";
	private static final String HIVE_JDBC_URL_KEY = "hive.jdbc.url";
	private static final String HIVE_JDBC_USER_KEY = "hive.jdbc.user";
	private static final String HIVE_JDBC_PASSWORD_KEY = "hive.jdbc.password";
	private static final String HIVE_HBASE_TABLE_KEY = "hive.hbase.table";
	private static final String HIVE_VIEW_ALL_DATABASE_KEY = "hive.database.all";
	private static final String HIVE_VIEW_DAY_DATABASE_KEY = "hive.database.day";
	private static final String HIVE_VIEW_WEEK_DATABASE_KEY = "hive.database.week";
	
	// processor config
	private static final String CONSUL_HOST_KEY = "consul.host";
	private static final String CONSUL_PORT_KEY = "consul.port";

	private static final String EVENT_FIELD_ID_KEY = "events.field.id";
	private static final String EVENT_FIELD_TIMESTAMP_KEY = "events.field.timestamp";
	private static final String EVENT_FIELD_EVENT_TYPE_KEY = "events.field.event_type";
	private static final String EVENT_FIELD_VERSION_KEY = "events.field.version";

	private static final String PROCESSOR_SCHEMA_HASH_LENGTH_KEY = "events.schema_hash_length";

	// persister config

	private static final String OUTPUT_DIRECTORY_KEY = "persister.out.directory";
	private static final String OUTPUT_DRILL_DIRECTORY_KEY = "persister.out.drill.directory";
	private static final String OUTPUT_HIVE_DIRECTORY_KEY = "persister.out.hive.directory";
	private static final String OUTPUT_CHANGELOGS_DIRECTORY_KEY = "persister.out.changelogs.directory";
	private static final String OUTPUT_TOPIC_DIRECTORY_KEY = "persister.out.topics.directory";
	private static final String OUTPUT_EVENTTYPE_DIRECTORY_KEY = "persister.out.eventtypes.directory";
	private static final String OUTPUT_AVROSCHEMAS_DIRECTORY_KEY = "persister.out.avroschemas.directory";
	private static final String OUTPUT_EVENTSTRUCTURES_DIRECTORY_KEY = "persister.out.eventstructures.directory";
	private static final String OUTPUT_SAMPLES_DIRECTORY_KEY = "persister.out.samples.directory";
	private static final String OUTPUT_SAMPLES_COUNT_KEY = "persister.out.samples.count";
	private static final String OUTPUT_DWH_TABLES_DIRECTORY_KEY = "persister.out.dwh.tables";
	private static final String OUTPUT_DWH_JOBS_DIRECTORY_KEY = "persister.out.dwh.jobs";

	/* kafka config keys */

	private static final String KAFKA_CONSUMER_BOOTSTRAP_SERVERS_KEY = "kafka.consumer.bootstrapserver";
	private static final String KAFKA_CONSUMER_GROUP_ID_KEY = "kafka.consumer.group_id";
	private static final String KAFKA_CONSUMER_POLL_TIMEOUT_KEY = "kafka.consumer.poll.timeout";
	private static final String KAFKA_CONSUMER_AUTO_OFFSET_RESET_KEY = "kafka.consumer.auto_offset_reset";
	private static final String KAFKA_CONSUMER_ENABLE_AUTO_COMMIT_KEY = "kafka.consumer.enable_autocommit";
	private static final String KAFKA_CONSUMER_MAX_POLL_RECORDS_KEY = "kafka.consumer.max_poll_records";
	private static final String KAFKA_POLL_PARTITIONS_INDIVIDUALLY_KEY = "kafka.poll.partitions.individually";
	private static final String KAFKA_FETCHER_THREADS_KEY = "kafka.fetcher.threads";

	// git settings

	private static final String GIT_ENABLED_KEY = "git.enabled";
	private static final String GIT_AUTHENTICATION_METHOD_KEY = "git.authentication.method";
	private static final String GIT_AUTHENTICATION_USER_KEY = "git.authentication.user";
	private static final String GIT_AUTHENTICATION_PASSWORD_KEY = "git.authentication.password";
	private static final String GIT_AUTHENTICATION_SSH_KEY_PATH_KEY = "git.authentication.sshkey.path";
	
	private static final String GIT_LOCAL_REPOSITORY_PATH_KEY = "git.local.repository.path";
	private static final String GIT_REPOSITORY_URI_KEY = "git.repository.uri";
	private static final String GIT_BRANCH_KEY = "git.branch";
	private static final String GIT_REMOTE_NAME_KEY = "git.remote";
	private static final String GIT_AUTHOR_KEY = "git.author";
	private static final String GIT_EMAIL_DEFAULT_USER_KEY = "git.email.default.user";
	private static final String GIT_EMAIL_DEFAULT_HOST_KEY = "git.email.default.host";

	// dwh settings
	
	private static final String DWH_TABLE_GENERATION_ENABLED_KEY = "dwh.table.enabled";
	private static final String DWH_TABLE_TEMPLATE_FILE_KEY = "dwh.table.template";
	private static final String DWH_TABLE_SCHEMA_KEY = "dwh.table.schema";
	
	private static final String DWH_JOB_GENERATION_ENABLED_KEY = "dwh.job.enabled";
	private static final String DWH_JOB_TEMPLATE_FILE_KEY = "dwh.job.template";
	
	/* CONFIG DEFAULT VALUES */

	// drill settings
	public boolean DRILL_ENABLED = false;
	public String DRILL_JDBC_URL = "jdbc:drill:zk=hdp-master-01.t24.stagec.sg-cloud.co.uk:2181,hdp-master-02.t24.stagec.sg-cloud.co.uk:2181,hdp-master-03.t24.stagec.sg-cloud.co.uk:2181/drill/drillbits1";
	public String DRILL_JDBC_USER = "event-analyzer";
	public String DRILL_JDBC_PASSWORD = "";
	public String DRILL_VIEW_HBASE_TABLE = "kafka_events";
	public String DRILL_VIEW_HBASE_COLUMN_FAMILY = "d";
	public String DRILL_VIEW_HBASE_JSON_FIELD = "json";
	public String DRILL_VIEW_HBASE_STORAGE_PLUGIN_NAME = "hbase";
	public String DRILL_VIEW_ALL_DATABASE = "drill.json_events";
	public String DRILL_VIEW_DAY_DATABASE = "drill.json_events_last_day";
	public String DRILL_VIEW_WEEK_DATABASE = "drill.json_events_last_week";

	// hive settings
	public boolean HIVE_ENABLED = false;
	public String HIVE_JDBC_URL = "jdbc:hive2://hdp-master-01.t24.stagec.sg-cloud.co.uk:2181,hdp-master-02.t24.stagec.sg-cloud.co.uk:2181,hdp-master-03.t24.stagec.sg-cloud.co.uk:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2-hive2";
	public String HIVE_JDBC_USER = "event-analyzer";
	public String HIVE_JDBC_PASSWORD = "";
	public String HIVE_HBASE_TABLE = "default.hbase_kafka_events";
	public String HIVE_VIEW_ALL_DATABASE = "kafka_events";
	public String HIVE_VIEW_DAY_DATABASE = "kafka_events_last_day";
	public String HIVE_VIEW_WEEK_DATABASE = "kafka_events_last_week";
	
	// processor settings
	public String CONSUL_HOST = "app-01-dwh.test.t24.eu-west-1.sg-cloud.co.uk";
	public int CONSUL_PORT = 8500;
	public int PROCESSOR_SCHEMA_HASH_LENGTH = 32;
	public String EVENT_FIELD_ID = "id";
	public String EVENT_FIELD_TIMESTAMP = "timestamp";
	public String EVENT_FIELD_EVENT_TYPE = "eventType";
	public String EVENT_FIELD_VERSION = "version";

	// persister settings
	public String OUTPUT_DIRECTORY = "out/";
	public String OUTPUT_DRILL_DIRECTORY = "drill_views/";
	public String OUTPUT_HIVE_DIRECTORY = "hive_views/";
	public String OUTPUT_CHANGELOGS_DIRECTORY = "change_logs/";
	public String OUTPUT_TOPIC_DIRECTORY = "topic_reports/";
	public String OUTPUT_EVENTTYPE_DIRECTORY = "event_types/";
	public String OUTPUT_AVROSCHEMAS_DIRECTORY = "avro_schemas/";
	public String OUTPUT_EVENTSTRUCTURES_DIRECTORY = "event_structures/";
	public String OUTPUT_SAMPLES_DIRECTORY = "event_samples/";
	public int OUTPUT_SAMPLES_COUNT = 10;
	public String OUTPUT_DWH_TABLES_DIRECTORY = "dwh_tables/";
	public String OUTPUT_DWH_JOBS_DIRECTORY = "dwh_jobs/";

	// kafka connection settings

	public String KAFKA_CONSUMER_BOOTSTRAP_SERVERS = "app-01-dwh.test.t24.eu-west-1.sg-cloud.co.uk:9092";

	// kafka consumer config
	public String KAFKA_CONSUMER_GROUP_ID = "DataHeartHBaseConnector";
	public String KAFKA_CONSUMER_AUTO_OFFSET_RESET = "earliest";
	public boolean KAFKA_CONSUMER_ENABLE_AUTO_COMMIT = false;
	public int KAFKA_CONSUMER_MAX_POLL_RECORDS = 10;
	public int KAFKA_CONSUMER_POLL_TIMEOUT = 100;
	public boolean KAFKA_POLL_PARTITIONS_INDIVIDUALLY = false;
	public int KAFKA_FETCHER_THREADS = 8;

	// git settings
	
	public boolean GIT_ENABLED=false;
	public String GIT_AUTHENTICATION_METHOD = "ssh"; // either ssh or http
	public String GIT_AUTHENTICATION_USER = "svc-tde-adm"; // only needed for http
	public String GIT_AUTHENTICATION_PASSWORD = "redacted"; // only needed for http
	public String GIT_AUTHENTICATION_SSH_KEY_PATH = "/home/andre.mis/.ssh/team_id_rsa"; // only needed for ssh 
	public String GIT_LOCAL_REPOSITORY_PATH = "git_repository/";
	public String GIT_REPOSITORY_URI = "git@srv-git-01-hh1.alinghi.tipp24.net:andre-mis/drill-views.git";
	public String GIT_BRANCH = "master";
	public String GIT_REMOTE_NAME = "origin";
	public String GIT_AUTHOR = "DrillViewGenerator";
	public String GIT_EMAIL_DEFAULT_USER = "drillviewgenerator";
	public String GIT_EMAIL_DEFAULT_HOST = "esailors.de";

	// dwh settings
	
	public boolean DWH_TABLE_GENERATION_ENABLED = false;
	public String DWH_TABLE_TEMPLATE_FILE = "dwh_table.template";
	public String DWH_TABLE_SCHEMA = "JSON";
	
	public boolean DWH_JOB_GENERATION_ENABLED = false;
	public String DWH_JOB_TEMPLATE_FILE = "dwh_job.template";
	
	private static Config instance;
	
	public static void load(String configPath) {
		instance = new Config(configPath);
	}
	
	public static Config getInstance() {
		if(instance == null) {
			throw new IllegalStateException("Got asked for an instance of config before it was loaded, call Config.load() first");
		}
		return instance;
	}
	
	// config internal fields

	private String configFileName;
	private Properties configProperties;

	private Config(String configFileName) {
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
		loadHiveSettings();
		loadKafkaSettings();
		loadProcessorSettings();
		loadPersisterSettings();
		loadGitSettings();
		loadDwhSettings();
	}

	private void loadGitSettings() {
		GIT_ENABLED = loadConfigBoolean(GIT_ENABLED_KEY);
		GIT_AUTHENTICATION_METHOD = loadConfigString(GIT_AUTHENTICATION_METHOD_KEY);
		GIT_AUTHENTICATION_USER = loadConfigString(GIT_AUTHENTICATION_USER_KEY);
		GIT_AUTHENTICATION_PASSWORD = loadConfigString(GIT_AUTHENTICATION_PASSWORD_KEY);
		GIT_AUTHENTICATION_SSH_KEY_PATH = loadConfigString(GIT_AUTHENTICATION_SSH_KEY_PATH_KEY);
		GIT_LOCAL_REPOSITORY_PATH = loadConfigString(GIT_LOCAL_REPOSITORY_PATH_KEY);
		GIT_REPOSITORY_URI = loadConfigString(GIT_REPOSITORY_URI_KEY);
		GIT_BRANCH = loadConfigString(GIT_BRANCH_KEY);
		GIT_REMOTE_NAME = loadConfigString(GIT_REMOTE_NAME_KEY);
		GIT_AUTHOR = loadConfigString(GIT_AUTHOR_KEY);
		GIT_EMAIL_DEFAULT_USER = loadConfigString(GIT_EMAIL_DEFAULT_USER_KEY);
		GIT_EMAIL_DEFAULT_HOST = loadConfigString(GIT_EMAIL_DEFAULT_HOST_KEY);
	}

	private void loadPersisterSettings() {
		OUTPUT_DIRECTORY = loadConfigString(OUTPUT_DIRECTORY_KEY);
		OUTPUT_DRILL_DIRECTORY = loadConfigString(OUTPUT_DRILL_DIRECTORY_KEY);
		OUTPUT_HIVE_DIRECTORY = loadConfigString(OUTPUT_HIVE_DIRECTORY_KEY);
		OUTPUT_CHANGELOGS_DIRECTORY = loadConfigString(OUTPUT_CHANGELOGS_DIRECTORY_KEY);
		OUTPUT_TOPIC_DIRECTORY = loadConfigString(OUTPUT_TOPIC_DIRECTORY_KEY);
		OUTPUT_EVENTTYPE_DIRECTORY = loadConfigString(OUTPUT_EVENTTYPE_DIRECTORY_KEY);
		OUTPUT_AVROSCHEMAS_DIRECTORY = loadConfigString(OUTPUT_AVROSCHEMAS_DIRECTORY_KEY);
		OUTPUT_EVENTSTRUCTURES_DIRECTORY = loadConfigString(OUTPUT_EVENTSTRUCTURES_DIRECTORY_KEY);
		OUTPUT_SAMPLES_DIRECTORY = loadConfigString(OUTPUT_SAMPLES_DIRECTORY_KEY);
		OUTPUT_SAMPLES_COUNT = loadConfigInteger(OUTPUT_SAMPLES_COUNT_KEY);
		OUTPUT_DWH_TABLES_DIRECTORY = loadConfigString(OUTPUT_DWH_TABLES_DIRECTORY_KEY);
		OUTPUT_DWH_JOBS_DIRECTORY = loadConfigString(OUTPUT_DWH_JOBS_DIRECTORY_KEY);
	}

	private void loadProcessorSettings() {
		PROCESSOR_SCHEMA_HASH_LENGTH = loadConfigInteger(PROCESSOR_SCHEMA_HASH_LENGTH_KEY);
		EVENT_FIELD_ID = loadConfigString(EVENT_FIELD_ID_KEY);
		EVENT_FIELD_TIMESTAMP = loadConfigString(EVENT_FIELD_TIMESTAMP_KEY);
		EVENT_FIELD_EVENT_TYPE = loadConfigString(EVENT_FIELD_EVENT_TYPE_KEY);
		EVENT_FIELD_VERSION = loadConfigString(EVENT_FIELD_VERSION_KEY);
	}

	private void loadConsulSettings() {
		CONSUL_HOST = loadConfigString(CONSUL_HOST_KEY);
		CONSUL_PORT = loadConfigInteger(CONSUL_PORT_KEY);
	}

	private void loadDrillSettings() {
		DRILL_ENABLED = loadConfigBoolean(DRILL_ENABLED_KEY);
		DRILL_JDBC_URL = loadConfigString(DRILL_JDBC_URL_KEY);
		DRILL_JDBC_USER = loadConfigString(DRILL_JDBC_USER_KEY);
		DRILL_JDBC_PASSWORD = loadConfigString(DRILL_JDBC_PASSWORD_KEY);
		DRILL_VIEW_HBASE_TABLE = loadConfigString(DRILL_VIEW_HBASE_TABLE_KEY);
		DRILL_VIEW_HBASE_COLUMN_FAMILY = loadConfigString(DRILL_VIEW_HBASE_COLUMN_FAMILY_KEY);
		DRILL_VIEW_HBASE_JSON_FIELD = loadConfigString(DRILL_VIEW_HBASE_JSON_FIELD_KEY);
		DRILL_VIEW_HBASE_STORAGE_PLUGIN_NAME = loadConfigString(DRILL_VIEW_HBASE_STORAGE_PLUGIN_NAME_KEY);
		DRILL_VIEW_ALL_DATABASE = loadConfigString(DRILL_VIEW_ALL_DATABASE_KEY);
		DRILL_VIEW_DAY_DATABASE = loadConfigString(DRILL_VIEW_DAY_DATABASE_KEY);
		DRILL_VIEW_WEEK_DATABASE = loadConfigString(DRILL_VIEW_WEEK_DATABASE_KEY);
	}
	
	private void loadHiveSettings() {
		HIVE_ENABLED = loadConfigBoolean(HIVE_ENABLED_KEY);
		HIVE_JDBC_URL = loadConfigString(HIVE_JDBC_URL_KEY);
		HIVE_JDBC_USER = loadConfigString(HIVE_JDBC_USER_KEY);
		HIVE_JDBC_PASSWORD = loadConfigString(HIVE_JDBC_PASSWORD_KEY);
		HIVE_HBASE_TABLE = loadConfigString(HIVE_HBASE_TABLE_KEY);
		HIVE_VIEW_ALL_DATABASE = loadConfigString(HIVE_VIEW_ALL_DATABASE_KEY);
		HIVE_VIEW_DAY_DATABASE = loadConfigString(HIVE_VIEW_DAY_DATABASE_KEY);
		HIVE_VIEW_WEEK_DATABASE = loadConfigString(HIVE_VIEW_WEEK_DATABASE_KEY);
	}

	private void loadKafkaSettings() {
		KAFKA_CONSUMER_BOOTSTRAP_SERVERS = loadConfigString(KAFKA_CONSUMER_BOOTSTRAP_SERVERS_KEY);
		KAFKA_CONSUMER_GROUP_ID = loadConfigString(KAFKA_CONSUMER_GROUP_ID_KEY);
		KAFKA_CONSUMER_POLL_TIMEOUT = loadConfigInteger(KAFKA_CONSUMER_POLL_TIMEOUT_KEY);
		KAFKA_CONSUMER_AUTO_OFFSET_RESET = loadConfigString(KAFKA_CONSUMER_AUTO_OFFSET_RESET_KEY);
		KAFKA_CONSUMER_ENABLE_AUTO_COMMIT = loadConfigBoolean(KAFKA_CONSUMER_ENABLE_AUTO_COMMIT_KEY);
		KAFKA_CONSUMER_MAX_POLL_RECORDS = loadConfigInteger(KAFKA_CONSUMER_MAX_POLL_RECORDS_KEY);
		KAFKA_POLL_PARTITIONS_INDIVIDUALLY = loadConfigBoolean(KAFKA_POLL_PARTITIONS_INDIVIDUALLY_KEY);
		KAFKA_FETCHER_THREADS = loadConfigInteger(KAFKA_FETCHER_THREADS_KEY);
	}
	
	private void loadDwhSettings() {
		DWH_TABLE_GENERATION_ENABLED = loadConfigBoolean(DWH_TABLE_GENERATION_ENABLED_KEY);
		DWH_TABLE_TEMPLATE_FILE = loadConfigString(DWH_TABLE_TEMPLATE_FILE_KEY);
		DWH_TABLE_SCHEMA = loadConfigString(DWH_TABLE_SCHEMA_KEY);
		DWH_JOB_GENERATION_ENABLED = loadConfigBoolean(DWH_JOB_GENERATION_ENABLED_KEY);
		DWH_JOB_TEMPLATE_FILE = loadConfigString(DWH_JOB_TEMPLATE_FILE_KEY);
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
