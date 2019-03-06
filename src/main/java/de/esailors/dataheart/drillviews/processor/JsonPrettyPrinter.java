package de.esailors.dataheart.drillviews.processor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

public class JsonPrettyPrinter {
	
	private static final Logger log = LogManager.getLogger(JsonPrettyPrinter.class.getName());
	
	public static String prettyPrintJsonString(JsonNode jsonNode) {
		try {
			ObjectMapper mapper = new ObjectMapper();
			Object json = mapper.readValue(jsonNode.toString(), Object.class);
			return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json);
		} catch (Exception e) {
			log.warn("Unable to pretty print JSON: " + jsonNode.toString(), e);
			return jsonNode.toString();
		}
	}
}
