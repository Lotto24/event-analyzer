package de.esailors.dataheart.drillviews.kafka;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.esailors.dataheart.drillviews.conf.Config;
import de.esailors.dataheart.drillviews.data.Topic;
import de.esailors.dataheart.drillviews.util.CollectionUtil;

public class KafkaEventFetcher {

	private static final Logger log = LogManager.getLogger(KafkaEventFetcher.class.getName());

	private Set<String> topicsBlacklist;
	private Set<String> topicsWhitelist;

	private MessageProcessor eventProcessor;

	int createdConsumers = 0;
	
	private Properties consumerProperties;
	private Consumer<byte[], byte[]> metaConsumer;
	private Set<Topic> topics = new HashSet<>();

	public KafkaEventFetcher() {
		initializeConsumerProperties();
		initMetaConsumer();
		initTopicList();
		initMessageProcessor();
		initShutdownHook();
	}

	public Set<Topic> fetchEvents() {
		int maxThreads = 4;
		log.info("Starting to consume events from Kafka using threads: " + maxThreads);
		long consumeStart = System.currentTimeMillis();
		
		ExecutorService executorService = Executors.newFixedThreadPool(maxThreads);
		
		int cnt = 0;
		for (Topic topic : CollectionUtil.toSortedList(topics)) {
			cnt++;
			
			executorService.submit(createThreadForTopic(topic, cnt));
		}
		
		executorService.shutdown();
		try {
			executorService.awaitTermination(1, TimeUnit.HOURS);
		} catch (InterruptedException e) {
			throw new IllegalStateException("Awaiting termination of topic fetchers failed", e);
		}
		
		long consumeEnd = System.currentTimeMillis();
		log.info("Consuming finished after " + (consumeEnd - consumeStart) + " ms");

		return topics;
	}

	private Runnable createThreadForTopic(Topic topic, int cnt) {
		return new Runnable() {

			@Override
			public void run() {
				log.info(cnt + " / " + topics.size() + ": " + topic.toString());
				Consumer<byte[], byte[]> consumer = createConsumer();
				fetchEventsForTopic(topic, consumer);
				consumer.close();
			}
			
		};
	}

	private void fetchEventsForTopic(Topic topic, Consumer<byte[], byte[]> consumer) {

		Collection<TopicPartition> topicParititions = discoverParitionsForTopic(topic, consumer);
		int consumedRecordsTotal = 0;

		if (Config.getInstance().KAFKA_POLL_PARTITIONS_INDIVIDUALLY) {
			// poll each partition individually until we have enough
			for (TopicPartition topicPartition : topicParititions) {
				prepareConsumerForTopicPartition(topicPartition, consumer);
				ConsumerRecords<byte[], byte[]> consumedRecords = consumer
						.poll(Config.getInstance().KAFKA_CONSUMER_POLL_TIMEOUT);

				eventProcessor.processRecords(topic, consumedRecords);
				log.info(consumedRecords.count() + " / " + Config.getInstance().KAFKA_CONSUMER_MAX_POLL_RECORDS
						+ " records consumed from " + topicPartition);
				consumedRecordsTotal += consumedRecords.count();
				if (consumedRecordsTotal >= Config.getInstance().KAFKA_CONSUMER_MAX_POLL_RECORDS) {
					log.info("Consumed enough records for " + topic.toString() + ": " + consumedRecordsTotal + " / "
							+ Config.getInstance().KAFKA_CONSUMER_MAX_POLL_RECORDS);
					break;
				}
			}
		} else {
			prepareConsumerForTopicPartitions(topicParititions, consumer);
			ConsumerRecords<byte[], byte[]> consumedRecords = consumer
					.poll(Config.getInstance().KAFKA_CONSUMER_POLL_TIMEOUT);

			eventProcessor.processRecords(topic, consumedRecords);
			consumedRecordsTotal += consumedRecords.count();
			log.info(consumedRecords.count() + " / " + Config.getInstance().KAFKA_CONSUMER_MAX_POLL_RECORDS
					+ " records consumed");
		}
		if (consumedRecordsTotal == 0) {
			log.info("NO records consumed for " + topic.toString());
		}
	}

	private void prepareConsumerForTopicPartition(TopicPartition topicPartition, Consumer<byte[], byte[]> consumer) {
		log.debug("Preparing for topic partitiong: " + topicPartition);
		prepareConsumerForTopicPartitions(Collections.singleton(topicPartition), consumer);
	}
	
	private void prepareConsumerForTopicPartitions(Collection<TopicPartition> topicPartitions, Consumer<byte[], byte[]> consumer) {
		if (!consumer.subscription().isEmpty()) {
			log.debug("Unsubscribing from all topic partitions");
			consumer.unsubscribe();
		}
		consumer.assign(topicPartitions);
		resetConsumerOffsets(consumer);
	}

	private Collection<TopicPartition> discoverParitionsForTopic(Topic topic, Consumer<byte[], byte[]> consumer) {
		log.debug("Preparing consumer for: " + topic);

		Collection<TopicPartition> partitions = new ArrayList<>();
		List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic.getName());
		for (PartitionInfo partitionInfo : partitionInfos) {
			TopicPartition partition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
			partitions.add(partition);
		}

		topic.setPartitionCount(partitions.size());
		log.debug("Preparation for " + topic + " done, partitions: " + partitions.size());

		return partitions;
	}

	private void resetConsumerOffsets(Consumer<byte[], byte[]> consumer) {
		// forcefully reset offset to 0
		consumer.assignment().forEach(topicPartition -> {
			consumer.seek(topicPartition, 0);
		});
	}

	private void initTopicList() {
		log.debug("Building topic list");

		initBlacklistTopics();
		initWhitelistTopics();

		log.debug("Listing all topics in Kafka");
		Map<String, List<PartitionInfo>> topicNames = metaConsumer.listTopics();

		for (String topicName : topicNames.keySet()) {
			String msg = " * " + topicName + ": ";
			if (topicsWhitelist != null && (topicsWhitelist.size() > 0) && !topicsWhitelist.contains(topicName)) {
				msg += "WHITELIST IGNORED";
				log.warn(msg);
				continue;
			}
			// additionally to hardcoded list from initBlacklistTopics() we ignore all topic
			// names that start with an underscore
			if (topicName.startsWith("_")) {
				topicsBlacklist.add(topicName);
			}
			if (topicsBlacklist.contains(topicName)) {
				msg += "IGNORED";
				log.info(msg);
				continue;
			}
			topics.add(new Topic(topicName));
			msg += "ASSIGNED";
			log.debug(msg);

		}
		log.info("Processing topics " + topics.size() + " / " + topicNames.size() + " discovered");
	}

	private void initMessageProcessor() {
		this.eventProcessor = new MessageProcessor();
	}

	private void initBlacklistTopics() {
		topicsBlacklist = new HashSet<String>();
		topicsBlacklist.add("avro_schema");
		
		// TODO for testing, blacklist topics that dont have entries on testsystem
		topicsBlacklist.add("alinghi_dbg_game_results");
		topicsBlacklist.add("alinghi_dbg_jackpot_published");
		topicsBlacklist.add("alinghi_dbg_lc_pool_winnings_rounding_difference_calculated");
		topicsBlacklist.add("alinghi_dbg_lc_real_ticket_mapped");
		topicsBlacklist.add("alinghi_dbg_lc_ticket_dissolution_canceled");
		topicsBlacklist.add("alinghi_dbg_lc_ticket_dissolved");
		topicsBlacklist.add("alinghi_dbg_lc_ticket_procured");
		topicsBlacklist.add("alinghi_dbg_lottery_quotas");
		topicsBlacklist.add("alinghi_dbg_lottery_winning_information_aggregated");
		topicsBlacklist.add("alinghi_dbg_real_ticket_ownbets_procured");
		topicsBlacklist.add("alinghi_dbg_subscription_migration_completed");
		topicsBlacklist.add("alinghi_dbg_subscription_migration_requested");
		topicsBlacklist.add("alinghi_dbg_subscription_terminated");
		topicsBlacklist.add("alinghi_dbg_ticket_win_quota");
		topicsBlacklist.add("alinghi_dbg_ticket_win_result");
		topicsBlacklist.add("alinghi_iwg_additional_winnings_booked");
		topicsBlacklist.add("alinghi_iwg_orphaned_ticket");
		topicsBlacklist.add("alinghi_iwg_reality_check_restriction_playing_blocked");
		topicsBlacklist.add("alinghi_iwg_ticket_activated");
		topicsBlacklist.add("alinghi_iwg_ticket_reservation_canceled");
		topicsBlacklist.add("alinghi_iwg_ticket_reservation_confirmed");
		topicsBlacklist.add("alinghi_iwg_ticket_reserved");
		topicsBlacklist.add("alinghi_paid_in");
		topicsBlacklist.add("alinghi_voucher_activated");
		topicsBlacklist.add("alinghi_voucher_used");
		topicsBlacklist.add("customer_achievement_challenge_booking");
		topicsBlacklist.add("customer_achievement_quiz_taken");
		topicsBlacklist.add("customer_addiction_prevention");
		topicsBlacklist.add("customer_authentication");
		topicsBlacklist.add("customer_comment_creation");
		topicsBlacklist.add("customer_consent_faq_clicked");
		topicsBlacklist.add("customer_consent_lightbox_shown");
		topicsBlacklist.add("customer_contact_request");
		topicsBlacklist.add("customer_email_request");
		topicsBlacklist.add("customer_email_trigger");
		topicsBlacklist.add("customer_email_verification_triggered_event");
		topicsBlacklist.add("customer_email_verified");
		topicsBlacklist.add("customer_flags_changed");
		topicsBlacklist.add("customer_flags_modify_request");
		topicsBlacklist.add("customer_history_request");
		topicsBlacklist.add("customerReassignedToUniqueCustomer");
		topicsBlacklist.add("dbg_subscription_migration_page_visited");
		topicsBlacklist.add("dwh_customer_personaldata");
		topicsBlacklist.add("dwh_customer_playingbehaviour");
		topicsBlacklist.add("internal_email_trigger");
		topicsBlacklist.add("iwg_demo_ticket_bought");
		topicsBlacklist.add("iwg_demo_ticket_settled");
		topicsBlacklist.add("iwg_kenow_draw_result");
		topicsBlacklist.add("iwg_kenow_next_draw");
		topicsBlacklist.add("iwg_kenow_ticket");
		topicsBlacklist.add("iwg_pool_created");
		topicsBlacklist.add("iwg_restriction_updated");
		topicsBlacklist.add("iwg_rgs_transaction");
		topicsBlacklist.add("iwg_unknown_event");
		topicsBlacklist.add("mail_delivery_status_topic");
		topicsBlacklist.add("pay_in_status");
		topicsBlacklist.add("pay_in_trigger");
		topicsBlacklist.add("payment_adyen_blacklist_request");
		topicsBlacklist.add("payment_adyen_case_management");
		topicsBlacklist.add("payment_adyen_chargeback");
		topicsBlacklist.add("payment_adyen_chargeback_reversed");
		topicsBlacklist.add("payment_adyen_credit_card_migration");
		topicsBlacklist.add("payment_adyen_payin_request");
		topicsBlacklist.add("payment_adyen_payin_response");
		topicsBlacklist.add("payment_adyen_payout_finalization_request");
		topicsBlacklist.add("payment_adyen_payout_reversed");
		topicsBlacklist.add("payment_adyen_refund");
		topicsBlacklist.add("payment_adyen_request_challenge_shopper");
		topicsBlacklist.add("payment_chargeback");
		topicsBlacklist.add("payment_chargeback_reversed");
		topicsBlacklist.add("payment_computop_payin_request");
		topicsBlacklist.add("payment_computop_payin_request_high_priority");
		topicsBlacklist.add("payment_computop_payin_response");
		topicsBlacklist.add("payment_computop_payin_response_high_priority");
		topicsBlacklist.add("payment_direct_debit_completed_high_priority");
		topicsBlacklist.add("payment_direct_debit_completed_low_priority");
		topicsBlacklist.add("payment_payin_cancellation");
		topicsBlacklist.add("payment_payout_processed");
		topicsBlacklist.add("payment_payout_reversed");
		topicsBlacklist.add("payment_refund");
		topicsBlacklist.add("payment_save_credit_card_failed");
		topicsBlacklist.add("payment_verification_payout_reversed");
		topicsBlacklist.add("pay_out_status");
		topicsBlacklist.add("purchase_status");
		topicsBlacklist.add("retention_winning_experience_booking");
		topicsBlacklist.add("rgs_operator_transaction");
		topicsBlacklist.add("rgs_transaction");
		topicsBlacklist.add("send_email_enriched_dlq");
		topicsBlacklist.add("send_email_enriched_high");
		topicsBlacklist.add("send_email_enriched_low");
		topicsBlacklist.add("send_internal_smtp_email");
		topicsBlacklist.add("send_internal_smtp_email_dlq");
		topicsBlacklist.add("send_sms");
		topicsBlacklist.add("send_sms_enriched");
		topicsBlacklist.add("send_sms_enriched_dlq");
		topicsBlacklist.add("softlogin_infotext_requested");
		topicsBlacklist.add("softlogin_opt_out");
		topicsBlacklist.add("softlogin_ticket_played_again");
		topicsBlacklist.add("softlogin_used");
		topicsBlacklist.add("softlogin_wrong_assignment");
		topicsBlacklist.add("ticket_double_procurement");
		topicsBlacklist.add("ticket_fusion");
		topicsBlacklist.add("ticket_wrong_receipted");
		topicsBlacklist.add("triggerAgeVerificationProcessing");
		topicsBlacklist.add("voucher_campaign_changed");
		topicsBlacklist.add("voucher_campaign_creation");
		topicsBlacklist.add("voucher_changed");
		topicsBlacklist.add("voucher_creation");
		topicsBlacklist.add("voucher_deactivation");
		topicsBlacklist.add("voucher_promotion_changed");
		topicsBlacklist.add("voucher_promotion_creation");
		topicsBlacklist.add("voucher_reservation");
		topicsBlacklist.add("voucher_used");
		topicsBlacklist.add("winning_experience_booking");
		topicsBlacklist.add("zoe_order_receipt_zealint");

		
		log.debug("Blacklisted topics: " + topicsBlacklist.size());
	}

	private void initWhitelistTopics() {
		// FOR DEVELOPMENT PURPOSES ONLY!
		topicsWhitelist = new HashSet<String>();
		topicsWhitelist.add("payment_payin_processed");
		topicsWhitelist.add("customer_restrictions_changed");
		topicsWhitelist.add("customer_registration");
		topicsWhitelist.add("postident_response_reporting");
		topicsWhitelist.add("account_balance_change_events");
		topicsWhitelist.add("alinghi_dbg_real_ticket_mapped");
		topicsWhitelist.add("alinghi_dbg_lc_ticket_ordered");
		topicsWhitelist.add("address_change");
		topicsWhitelist.add("fritz_ticket_procurement");

		if (topicsWhitelist.size() > 0) {
			log.warn("DEV ONLY! Whitelisted topics: " + topicsWhitelist.size());
		}
	}

	private void initMetaConsumer() {
		metaConsumer = createConsumer();
	}

	private KafkaConsumer<byte[], byte[]> createConsumer() {
		createdConsumers++;
		return new KafkaConsumer<>(consumerProperties);
	}

	private void initializeConsumerProperties() {
		log.info("Creating Kafka consumer for: " + Config.getInstance().KAFKA_CONSUMER_BOOTSTRAP_SERVERS);

		consumerProperties = new Properties();
		consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.getInstance().KAFKA_CONSUMER_BOOTSTRAP_SERVERS);
		consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, Config.getInstance().KAFKA_CONSUMER_GROUP_ID);
		consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Config.getInstance().KAFKA_CONSUMER_AUTO_OFFSET_RESET);
		consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Config.getInstance().KAFKA_CONSUMER_ENABLE_AUTO_COMMIT);
		consumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Config.getInstance().KAFKA_CONSUMER_MAX_POLL_RECORDS);
	}

	public void close() {
		log.debug("Closing connection to Kafka");
		if (metaConsumer == null) {
			log.warn("Tried to close uninitialized kafka connection");
			return;
		}
		metaConsumer.close();
	}

	private void initShutdownHook() {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				log.debug("Shutdown Hook triggered");
				close();
				System.out.println("Created consumers during run: " + createdConsumers);
			}
		});
	}

}
