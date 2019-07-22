package de.esailors.dataheart.drillviews.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

public class JsonUtil {
	
	private static final Logger log = LogManager.getLogger(JsonUtil.class.getName());
	
	public enum JsonType {
		ARRAY, BINARY, BOOLEAN, FLOAT, INTEGER, NULL, OBJECT, TEXTUAL, UNKNOWN
	}
	
	private static ObjectMapper mapper = new ObjectMapper();
	
	public static String prettyPrintJsonString(JsonNode jsonNode) {
		try {
			Object json = mapper.readValue(jsonNode.toString(), Object.class);
			return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json);
		} catch (Exception e) {
			log.warn("Unable to pretty print JSON: " + jsonNode.toString(), e);
			return jsonNode.toString();
		}
	}
	
	public static JsonType getJsonType(JsonNode json) {
		// there must be a better way of doing this in jackson API
		if (json.isArray()) {
			return JsonType.ARRAY;
		} else if (json.isBinary()) {
			return JsonType.BINARY;
		} else if (json.isBoolean()) {
			return JsonType.BOOLEAN;
		} else if (json.isFloatingPointNumber()) {
			return JsonType.FLOAT;
		} else if (json.isIntegralNumber()) {
			return JsonType.INTEGER;
		} else if (json.isNull()) {
			return JsonType.NULL;
		} else if (json.isObject()) {
			return JsonType.OBJECT;
		} else if (json.isTextual()) {
			return JsonType.TEXTUAL;
		}
		
		log.warn("Unable to determine JsonType for: " + json.toString());

		return JsonType.UNKNOWN;
	}
}
