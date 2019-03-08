package de.esailors.dataheart.drillviews.kafka;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.adobe.xmp.impl.Base64;
import com.google.common.net.HostAndPort;

import de.esailors.avro.RegistryClient;
import de.esailors.avro.RegistryClient.TransportException;
import de.esailors.avro.RegistryClients;
import de.esailors.avro.SchemaHash;
import de.esailors.dataheart.drillviews.conf.Config;
import de.esailors.dataheart.drillviews.exception.UnknownSchemaException;

public class MessageParser {

	private static final Logger log = LogManager.getLogger(MessageParser.class.getName());

	private Config config;

	private RegistryClient registryClient;
	private Map<String, Schema> schemaCache;
	private ObjectMapper jsonObjectMapper;

	public MessageParser(Config config) {
		this.config = config;

		initJackson();
		initRegistryClient();
		initSchemaCache();
	}

	private void initJackson() {
		jsonObjectMapper = new ObjectMapper();
	}

	private void initRegistryClient() {
		HostAndPort hostAndPort = HostAndPort.fromParts(config.CONSUL_HOST, config.CONSUL_PORT);
		registryClient = RegistryClients.caching(RegistryClients.consul(Collections.singletonList(hostAndPort)));
	}

	private void initSchemaCache() {
		schemaCache = new HashMap<>();
	}

	public JsonNode parseMessage(byte[] message) {

		JsonNode extractedJson;
		try {
			extractedJson = messageToJson(message);
			if (extractedJson == null) {
				throw new IOException("Unable to extract json from message: " + new String(message));
			}
		} catch (UnknownSchemaException | IOException e) {
			throw new IllegalStateException("Unable to handle message: " + Bytes.toString(message), e);
		}

		return extractedJson;

	}

	private GenericRecord decodeAvro(byte[] data, Schema schema) throws IOException {
		BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, config.PROCESSOR_SCHEMA_HASH_LENGTH,
				data.length - config.PROCESSOR_SCHEMA_HASH_LENGTH, null);

		SpecificDatumReader<GenericRecord> reader = new SpecificDatumReader<>(schema);

		GenericRecord result = reader.read(null, decoder);
		return result;
	}

	private Schema getSchemaForHash(String schemaHash) throws UnknownSchemaException {
		log.debug("Getting schema for hash: " + schemaHash);
		// try internal Cache
		Schema fromCache = schemaCache.get(schemaHash);
		if (fromCache != null) {
			log.debug("Schema found in internal cache");
			return fromCache;
		}
		
		// try fetching from consul directly - .. 
		// why? well, i made a mistake which made it so the registry client failed for a specific hash
		// so i implemented the fetching myself and then found my mistake, now i'll leave this in 
		Optional<Schema> schemaFromConsul = fetchSchemaFromConsul(schemaHash);
		if(schemaFromConsul.isPresent()) {
			log.debug("Got schema direclty from Consul");
			schemaCache.put(schemaHash, schemaFromConsul.get());
			return schemaFromConsul.get();
		}

		// try consul registry client
		Optional<Schema> schemaOption = getSchemaForHashFromRegistryClient(schemaHash);
		if (schemaOption.isPresent()) {
			log.debug("Got schema from inhouse RegistryClient");
			schemaCache.put(schemaHash, schemaOption.get());
			return schemaOption.get();
		}

		throw new UnknownSchemaException("Did not find Schema for hash anywhere: " + schemaHash);
	}

	private Optional<Schema> fetchSchemaFromConsul(String schemaHash) {
		String consulUrl = "http://" + config.CONSUL_HOST + ":" + config.CONSUL_PORT + "/v1/kv/avro-schemas/" + schemaHash;
		log.debug("Fetching schema from Consul at: " + consulUrl);
		try {
			Content response = Request.Get(consulUrl).execute().returnContent();
			String jsonString = response.asString();

			ObjectMapper mapper = new ObjectMapper();
			JsonNode jsonNode;
			jsonNode = mapper.readTree(jsonString);
			String avroSchemaAsBase64 = jsonNode.get(0).findValue("Value").asText();
			String avroSchemaString = Base64.decode(avroSchemaAsBase64);
			log.trace("Got avro schema String: " + avroSchemaString);
			Schema schema = new Schema.Parser().parse(avroSchemaString);
			return Optional.of(schema);
			
		} catch (IOException e) {
			log.error("Error while fetching schema direclty from consul", e);
			return Optional.empty();
		}

	}

	private Optional<Schema> getSchemaForHashFromRegistryClient(String schemaHash) {
		try {
			return registryClient.get(SchemaHash.ofHash(schemaHash));
		} catch (TransportException e) {
			log.error("Error while fetching schema from registry client", e);
			return Optional.empty();
		}
	}

	private JsonNode messageToJson(byte[] message) throws IOException, UnknownSchemaException {

		String jsonString;

		if (isMessageAvro(message)) {
			// avro message
			String schemaHash = extractSchemaHashFromMessage(message);
			Schema schema = getSchemaForHash(schemaHash);
			GenericRecord record = decodeAvro(message, schema);
			jsonString = record.toString();
		} else {
			// json message
			jsonString = Bytes.toString(message);
		}

		return stringToJsonNode(jsonString);
	}
	
	public JsonNode stringToJsonNode(String jsonString) throws JsonProcessingException, IOException {
		return jsonObjectMapper.readTree(jsonString);
	}

	public boolean isMessageAvro(byte[] message) {
		String schemaHash = null;
		if (message.length >= 32) {
			schemaHash = extractSchemaHashFromMessage(message);
		}

		return schemaHash != null && !schemaHash.trim().startsWith("{");
	}
	
	public String getAvroSchemaHashForMessage(byte[] message) {
		return extractSchemaHashFromMessage(message);
	}
	
	public Schema getSchemaForMessage(byte[] message) {
		return schemaCache.get(extractSchemaHashFromMessage(message));
	}

	private String extractSchemaHashFromMessage(byte[] message) {
		String schemaHash;
		schemaHash = Bytes.toString(message, 0, config.PROCESSOR_SCHEMA_HASH_LENGTH);
		return schemaHash;
	}

}
