package de.esailors.dataheart.drillviews.data;

import java.util.Set;

import org.apache.avro.Schema.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Optional;

import de.esailors.dataheart.drillviews.util.JsonUtil.JsonType;

public class PrimitiveTypeDetector {

	private static final Logger log = LogManager.getLogger(PrimitiveTypeDetector.class.getName());

	public Optional<PrimitiveType> primitiveTypeForNode(Node node) {

		// TODO multiple types, no type, avro_type, union_types etc

		// try avro type first
		Optional<Type> avroTypeOption = primitiveAvroTypeForNode(node);
		Optional<PrimitiveType> primitiveTypeForAvroType = primitiveTypeForAvroType(avroTypeOption);
		if(primitiveTypeForAvroType.isPresent()) {
			return primitiveTypeForAvroType;
		}
		log.debug("Unable to determined primitivte avroType, trying jsonType for " + node.getId());

		Optional<JsonType> jsonTypeOption = primitiveJsonTypeForNode(node);
		if (!jsonTypeOption.isPresent()) {
			return Optional.absent();
		}
		JsonType jsonType = jsonTypeOption.get();
		switch (jsonType) {
		case TEXTUAL:
			return Optional.of(PrimitiveType.TEXT);
		case INTEGER:
			return Optional.of(PrimitiveType.INT);
		case FLOAT:
			return Optional.of(PrimitiveType.FLOAT);
		case BOOLEAN:
			return Optional.of(PrimitiveType.BOOLEAN);
		default: {
			log.warn("Unable to determine primitive type for jsonType " + jsonType);
			return Optional.absent();
		}
		}
	}

	private Optional<Type> primitiveAvroTypeForNode(Node node) {
		Set<String> avroTypes = node.getProperty(NodePropertyType.AVRO_TYPE);
		if (avroTypes == null || avroTypes.size() == 0) {
			log.debug("Did not find any avro Type property for " + node.getId());
			return Optional.absent();
		}

		Optional<Type> primitiveAvroTypeOption = primitiveAvroTypeFromSet(avroTypes);
		if (!primitiveAvroTypeOption.isPresent()) {
			return Optional.absent();
		}
		Type primitiveAvroType = primitiveAvroTypeOption.get();
		if (Type.UNION.equals(primitiveAvroType)) {
			// look at union types
			Set<String> unionTypes = node.getProperty(NodePropertyType.AVRO_UNION_TYPE);
			return primitiveAvroTypeFromSet(unionTypes);
		} else {
			return primitiveAvroTypeOption;
		}

//			if (unionTypes == null || unionTypes.size() == 0) {
//				throw new IllegalStateException(
//						"Expect nodes with avro type UNION to have set union types property " + node.getId());
//			}
//			for (String unionType : unionTypes) {
//				if (Type.NULL.toString().equals(unionType)) {
//					continue;
//				}
//				// TODO bad heuristic, same as above
//				if (Type.UNION.toString().equals(primitiveAvroType)
//						|| Type.STRING.toString().equals(primitiveAvroType)) {
//					primitiveAvroType = unionType;
//				}
//			}
//		}
//		return Optional.of(Type.valueOf(primitiveAvroType));
	}

	private Optional<Type> primitiveAvroTypeFromSet(Set<String> avroTypes) {
		String primitiveAvroType = null;
		for (String avroType : avroTypes) {
			if (Type.NULL.toString().equals(avroType)) {
				continue;
			}
			if (primitiveAvroType == null) {
				primitiveAvroType = avroType;
			} else {
				// TODO bad heuristic, if we get conflicting types we should get up to String as
				// well
				if (Type.STRING.toString().equals(avroType)) {
					primitiveAvroType = avroType;
				}
			}
		}
		if (primitiveAvroType == null) {
			return Optional.absent();
		}

		return Optional.of(Type.valueOf(primitiveAvroType));
	}

	public Optional<JsonType> primitiveJsonTypeForNode(Node node) {
		// TODO multiple types, no type, avro_type, union_types etc
		if (node.hasChildren()) {
			throw new IllegalArgumentException("Got asked for a primitive type of a nested node: " + node.getId());
		}
		Set<String> jsonTypes = node.getProperty(NodePropertyType.JSON_TYPE);
		if (jsonTypes == null || jsonTypes.size() == 0) {
			log.warn("Did not find any JsonTypes property for " + node.getId());
			return Optional.absent();
		}
		String primitiveJsonType = null;
		for (String jsonType : jsonTypes) {
			if (JsonType.NULL.toString().equals(jsonType)) {
				continue;
			}
			if (primitiveJsonType == null) {
				primitiveJsonType = jsonType;
			} else {
				// textual always wins
				// TODO if we get any two conflicting types we should return textual as well
				if (JsonType.TEXTUAL.toString().equals(jsonType)) {
					primitiveJsonType = jsonType;
				}
			}
		}
		if (primitiveJsonType == null) {
			log.debug("Unable to determine primitive json type for node: " + node.getId());
			return Optional.absent();
		}
		return Optional.of(JsonType.valueOf(primitiveJsonType));
	}

	/**
	 * For nodes that are arrays of primitives
	 */
	public Optional<PrimitiveType> primitiveAvroArrayItemTypeForNode(Node node) {
		Set<String> arrayTypes = node.getProperty(NodePropertyType.AVRO_ARRAY_ITEM_TYPE);
		if (arrayTypes == null || arrayTypes.isEmpty()) {
			log.warn("Got asked for primitive avro array item type of a nodes withou avro_array_item_type set: "
					+ node.getId());
			return Optional.absent();
		}
		return primitiveTypeForAvroType(primitiveAvroTypeFromSet(arrayTypes));

	}

	private Optional<PrimitiveType> primitiveTypeForAvroType(Optional<Type> avroTypeOption) {
		if (!avroTypeOption.isPresent()) {
			return Optional.absent();
		}
		return primitiveTypeForAvroType(avroTypeOption.get());
	}

	private Optional<PrimitiveType> primitiveTypeForAvroType(Type avroType) {
		switch (avroType) {
		case STRING:
			return Optional.of(PrimitiveType.TEXT);
		case ENUM:
			// TODO special handling for ENUMs?
			return Optional.of(PrimitiveType.TEXT);
		case BOOLEAN:
			return Optional.of(PrimitiveType.BOOLEAN);
		case INT:
			return Optional.of(PrimitiveType.INT);
		case LONG:
			// TODO maybe it makes sense to differentiate between int and long here
			return Optional.of(PrimitiveType.INT);
		case FLOAT:
			return Optional.of(PrimitiveType.FLOAT);
		case DOUBLE:
			// TODO maybe it makes sense to differentiate between float and double here
			return Optional.of(PrimitiveType.FLOAT);

		case MAP:
		case RECORD:
		case UNION: {
			throw new IllegalStateException(
					"Unexpected nested avro type received as primitive " + avroType);
		}

		case BYTES:
		case FIXED:
		case ARRAY:
		case NULL:
		default: {
			log.warn("Unable to determine primitive type for avro type: " + avroType);
		}
		}
		return Optional.absent();
	}

}
