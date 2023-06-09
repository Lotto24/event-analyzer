package de.esailors.dataheart.drillviews.data;

import java.util.Set;

import org.apache.avro.Schema.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

import de.esailors.dataheart.drillviews.util.CollectionUtil;
import de.esailors.dataheart.drillviews.util.JsonUtil.JsonType;

public class PrimitiveTypeDetector {

	private static final Logger log = LogManager.getLogger(PrimitiveTypeDetector.class.getName());
	
	private static PrimitiveTypeDetector instance;
	
	public static PrimitiveTypeDetector getInstance() {
		if(instance == null) {
			instance = new PrimitiveTypeDetector();
		}
		return instance;
	}
	
	
	private PrimitiveTypeDetector() {
		
	}

	public Optional<PrimitiveType> primitiveTypeForNode(Node node) {
		// check if primitive type has been analyzed before
		Set<String> primitiveTypeProperty = node.getProperty(NodePropertyType.PRIMITIVE_TYPE);
		if (primitiveTypeProperty != null && primitiveTypeProperty.size() == 1) {
			String primitiveType = CollectionUtil.popFromSet(primitiveTypeProperty);
			log.trace("Found PrimitiveType " + primitiveType + " from previous analysis for: " + node.getId());
			return Optional.of(PrimitiveType.valueOf(primitiveType));
		}
		Optional<PrimitiveType> determinedPrimitiveType = determinePrimitiveTypeForNode(node);
		if (determinedPrimitiveType.isPresent()) {
			// mark primitive type as node property
			PrimitiveType primitiveType = determinedPrimitiveType.get();
			node.addProperty(NodePropertyType.PRIMITIVE_TYPE, primitiveType);
		}
		return determinedPrimitiveType;
	}

	/**
	 * For nodes that are arrays of primitives
	 */
	public Optional<PrimitiveType> primitiveArrayItemTypeForNode(Node node) {
		// check if primitive type has been analyzed before
		Set<String> primitiveTypeProperty = node.getProperty(NodePropertyType.ARRAY_ITEM_PRIMITIVE_TYPE);
		if (primitiveTypeProperty != null && primitiveTypeProperty.size() == 1) {
			String primitiveType = CollectionUtil.popFromSet(primitiveTypeProperty);
			log.trace("Found PrimitiveType " + primitiveType + " from previous analysis for: " + node.getId());
			return Optional.of(PrimitiveType.valueOf(primitiveType));
		}
		Optional<PrimitiveType> determinedPrimitiveType = determinePrimitiveArrayItemTypeForNode(node);
		if (determinedPrimitiveType.isPresent()) {
			// mark primitive type as node property
			PrimitiveType primitiveType = determinedPrimitiveType.get();
			node.addProperty(NodePropertyType.ARRAY_ITEM_PRIMITIVE_TYPE, primitiveType);
		}
		return determinedPrimitiveType;
	}

	private Optional<PrimitiveType> determinePrimitiveArrayItemTypeForNode(Node node) {
		// first try avro property
		Optional<PrimitiveType> primitiveAvroArrayItemTypeForNode = determinePrimitiveAvroArrayItemTypeForNode(node);
		if (primitiveAvroArrayItemTypeForNode.isPresent()) {
			return primitiveAvroArrayItemTypeForNode;
		}
		// try json property
		return determinePrimitiveJsonArrayItemTypeForNode(node);
	}

	private Optional<PrimitiveType> determinePrimitiveAvroArrayItemTypeForNode(Node node) {
		Set<String> avroArrayTypes = node.getProperty(NodePropertyType.AVRO_ARRAY_ITEM_TYPE);
		if (avroArrayTypes == null || avroArrayTypes.isEmpty()) {
			log.debug("Got asked for primitive array item type of a node without avro_array_item_type set: "
					+ node.getId());
			return Optional.empty();
		}
		Optional<Type> primitiveAvroTypeFromSet = primitiveAvroTypeFromSet(avroArrayTypes);
		if (primitiveAvroTypeFromSet.isPresent()) {
			return primitiveTypeForAvroType(primitiveAvroTypeFromSet.get());
		}

		return Optional.empty();
	}

	private Optional<PrimitiveType> determinePrimitiveJsonArrayItemTypeForNode(Node node) {
		Set<String> jsonArrayTypes = node.getProperty(NodePropertyType.JSON_ARRAY_ITEM_TYPE);
		if (jsonArrayTypes == null || jsonArrayTypes.isEmpty()) {
			log.debug("Got asked for primitive array item type of a node without json_array_item_type set: "
					+ node.getId());
			return Optional.empty();
		}
		return primitiveTypeForJsonType(primitiveJsonTypeFromSet(jsonArrayTypes));
	}

	private Optional<PrimitiveType> determinePrimitiveTypeForNode(Node node) {
		if (node.hasChildren()) {
			throw new IllegalArgumentException("Got asked for a primitive type of a nested node: " + node.getId());
		}

		// try avro type first
		Optional<Type> avroTypeOption = primitiveAvroTypeForNode(node);
		Optional<PrimitiveType> primitiveTypeForAvroType = primitiveTypeForAvroType(avroTypeOption);
		if (primitiveTypeForAvroType.isPresent()) {
			return primitiveTypeForAvroType;
		}
		log.debug("Unable to determined primitivte avroType, trying jsonType for " + node.getId());

		Optional<JsonType> jsonTypeOption = primitiveJsonTypeForNode(node);
		return primitiveTypeForJsonType(jsonTypeOption);
	}

	private Optional<Type> primitiveAvroTypeForNode(Node node) {
		Set<String> avroTypes = node.getProperty(NodePropertyType.AVRO_TYPE);
		if (avroTypes == null || avroTypes.size() == 0) {
			log.debug("Did not find any avro Type property for " + node.getId());
			return Optional.empty();
		}

		Optional<Type> primitiveAvroTypeOption = primitiveAvroTypeFromSet(avroTypes);
		if (primitiveAvroTypeOption.isPresent()) {
			return primitiveAvroTypeOption;
		}
		// check if it's a union, if yes look at union types (nullable)
		if (avroTypes.contains(Type.UNION.toString())) {
			Set<String> unionTypes = node.getProperty(NodePropertyType.AVRO_UNION_TYPE);
			return primitiveAvroTypeFromSet(unionTypes);
		}

		return Optional.empty();
	}

	private Optional<Type> primitiveAvroTypeFromSet(Set<String> avroTypes) {
		if (avroTypes == null || avroTypes.isEmpty()) {
			return Optional.empty();
		}
		// rank primitive avro types from biggest to smallest
		Type[] orderedTypes = { Type.BYTES, Type.STRING, Type.FIXED, Type.ENUM, Type.DOUBLE, Type.FLOAT, Type.LONG,
				Type.INT, Type.BOOLEAN };
		for (Type type : orderedTypes) {
			if (avroTypes.contains(type.toString())) {
				return Optional.of(type);
			}
		}

		return Optional.empty();
	}

	private Optional<JsonType> primitiveJsonTypeForNode(Node node) {
		if (node.hasChildren()) {
			throw new IllegalArgumentException("Got asked for a primitive type of a nested node: " + node.getId());
		}
		Set<String> jsonTypes = node.getProperty(NodePropertyType.JSON_TYPE);
		if (jsonTypes == null || jsonTypes.size() == 0) {
			log.warn("Did not find any JsonTypes property for " + node.getId());
			return Optional.empty();
		}

		return primitiveJsonTypeFromSet(jsonTypes);
	}

	private Optional<JsonType> primitiveJsonTypeFromSet(Set<String> jsonTypes) {

		// rank primitive json types from biggest to smallest
		JsonType[] orderedTypes = { JsonType.TEXTUAL, JsonType.BINARY, JsonType.FLOAT, JsonType.INTEGER,
				JsonType.BOOLEAN };
		for (JsonType type : orderedTypes) {
			if (jsonTypes.contains(type.toString())) {
				return Optional.of(type);
			}
		}
		return Optional.empty();
	}

	private Optional<PrimitiveType> primitiveTypeForAvroType(Optional<Type> avroTypeOption) {
		if (!avroTypeOption.isPresent()) {
			return Optional.empty();
		}
		return primitiveTypeForAvroType(avroTypeOption.get());
	}

	private Optional<PrimitiveType> primitiveTypeForAvroType(Type avroType) {
		switch (avroType) {
		case STRING:
			return Optional.of(PrimitiveType.TEXT);
		case FIXED:
			return Optional.of(PrimitiveType.TEXT);
		case BYTES:
			return Optional.of(PrimitiveType.TEXT);
		case ENUM:
			return Optional.of(PrimitiveType.TEXT);
		case BOOLEAN:
			return Optional.of(PrimitiveType.BOOLEAN);
		case LONG:
			return Optional.of(PrimitiveType.BIGINT);
		case INT:
			return Optional.of(PrimitiveType.INT);
		case DOUBLE:
			return Optional.of(PrimitiveType.DOUBLE);
		case FLOAT:
			return Optional.of(PrimitiveType.FLOAT);

		case MAP:
		case RECORD:
		case UNION: {
			throw new IllegalStateException("Unexpected nested avro type received as primitive " + avroType);
		}

		case ARRAY:
		case NULL:
		default: {
			log.warn("Unable to determine primitive type for avro type: " + avroType);
		}
		}
		return Optional.empty();
	}

	private Optional<PrimitiveType> primitiveTypeForJsonType(Optional<JsonType> jsonTypeOption) {
		if (!jsonTypeOption.isPresent()) {
			return Optional.empty();
		}
		return primitiveTypeForJsonType(jsonTypeOption.get());
	}

	private Optional<PrimitiveType> primitiveTypeForJsonType(JsonType jsonType) {
		switch (jsonType) {
		case TEXTUAL:
			return Optional.of(PrimitiveType.TEXT);
		case INTEGER:
			return Optional.of(PrimitiveType.BIGINT);
		case FLOAT:
			return Optional.of(PrimitiveType.DOUBLE);
		case BOOLEAN:
			return Optional.of(PrimitiveType.BOOLEAN);
		default: {
			log.warn("Unable to determine primitive type for jsonType " + jsonType);
			return Optional.empty();
		}
		}
	}

}
