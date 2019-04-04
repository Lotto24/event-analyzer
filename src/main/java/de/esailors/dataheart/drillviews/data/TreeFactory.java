package de.esailors.dataheart.drillviews.data;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

public class TreeFactory {

	private static TreeFactory instance;

	public static TreeFactory getInstance() {
		if (instance == null) {
			instance = new TreeFactory();
		}
		return instance;
	}

	private ObjectMapper mapper;

	private TreeFactory() {
		mapper = new ObjectMapper();
	}

	public Tree buildTreeFromJsonNode(JsonNode json, String name) {
		if (!isNestedJson(json)) {
			throw new IllegalArgumentException("Received json that is not nested: " + json.toString());
		}

		Tree r = new Tree(name);
		
		r.getRootNode().addProperty("EVENT_SOURCE", "");

		extendTreeWithJsonFields(r.getRootNode(), json, false);

		return r;
	}

	private void extendTreeWithJsonFields(Node currentParent, JsonNode json, boolean prependParentName) {
		Iterator<Entry<String, JsonNode>> fields = json.getFields();
		while (fields.hasNext()) {
			Entry<String, JsonNode> field = fields.next();
			String nodeId = "";
			if (prependParentName) {
				nodeId += currentParent.getId() + ".";
			}
			String nodeName = field.getKey();
			nodeId += nodeName;
			Node node = new Node(nodeId, nodeName);
			currentParent.addChild(node);

			JsonNode fieldJson = field.getValue();
			String jsonType = getJsonType(fieldJson);
			node.addProperty("JSON_TYPE", jsonType);
			if(jsonType.equals("null")) {
				node.setOptional(true);
			}
			if (isNestedJson(fieldJson)) {
				extendTreeWithJsonFields(node, fieldJson, true);
			}
		}
	}

	private String getJsonType(JsonNode json) {
		// there must be a better way of doing this in jackson API
		// TODO either way doesn't belong here and should be an enum
		if (json.isArray()) {
			return "array";
		} else if (json.isBinary()) {
			return "binary";
		} else if (json.isBoolean()) {
			return "boolean";
		} else if (json.isFloatingPointNumber()) {
			return "float";
		} else if (json.isIntegralNumber()) {
			return "integer";
		} else if (json.isNull()) {
			return "null";
		} else if (json.isObject()) {
			return "object";
		} else if (json.isTextual()) {
			return "string";
		}

		return "UNKNOWN";
	}

	public Tree buildTreeFromJsonString(String jsonString, String name) {
		try {
			return buildTreeFromJsonNode(mapper.readTree(jsonString), name);
		} catch (IOException e) {
			throw new IllegalArgumentException("Not a json string: " + jsonString, e);
		}
	}

	public Tree buildTreeFromAvroSchema(Schema avroSchema) {
		// make sure top level is a record
		if (!isNestedAvroSchema(avroSchema)) {
			throw new IllegalArgumentException(
					"Unable to build a Tree for non nested Schema: " + avroSchema.toString());
		}

		// either use the same name when comparing trees or exclude them from equals()
		Tree r = new Tree(avroSchema.getName());
		r.getRootNode().addProperty("AVRO_SOURCE", "");

		extendTreeWithAvroFields(r.getRootNode(), avroSchema.getFields(), false);

		return r;
	}

	private void extendTreeWithAvroFields(Node currentParent, Collection<Field> fields, boolean prependParentName) {

		for (Field field : fields) {
			String nodeId = "";
			if (prependParentName) {
				nodeId += currentParent.getId() + ".";
			}
			String nodeName = field.name();
			nodeId += nodeName;

			Node node = new Node(nodeId, nodeName);
			addAvroFieldPropertiesToNode(field, node);
			currentParent.addChild(node);

			if (isNestedAvroSchema(field.schema())) {
				extendTreeWithAvroFields(node, getNestedFieldsFromAvroSchema(field.schema()), true);
			}

		}

	}

	private void addAvroFieldPropertiesToNode(Field field, Node node) {
		Type fieldType = field.schema().getType();
		node.addProperty("AVRO_TYPE", fieldType);
		
		// list enum values
		if(fieldType.equals(Type.ENUM)) {
			int cnt = 0;
			for(String enumSymbol : field.schema().getEnumSymbols()) {
				cnt++;
				node.addProperty("AVRO_ENUM_SYMBOL" + cnt, enumSymbol);
			}
		}
		
		// list union types and mark node as optional if union types contains NULL
		if(fieldType.equals(Type.UNION)) {
			int cnt = 0;
			boolean sawNullType = false;
			for(Schema unionSchema : field.schema().getTypes()) {
				cnt++;
				node.addProperty("AVRO_UNION_TYPE-" + cnt, unionSchema.getType());
				if(unionSchema.getType().equals(Type.NULL)) {
					sawNullType = true;
				}
			}
			if(sawNullType) {
				node.setOptional(true);
			}
		}
	}

	private Set<Field> getNestedFieldsFromAvroSchema(Schema avroSchema) {
		Set<Field> r = new HashSet<>();
		switch (avroSchema.getType()) {
		case RECORD: {
			r.addAll(avroSchema.getFields());
			return r;
		}
		case UNION: {
			for (Schema unionSchema : avroSchema.getTypes()) {
				// for now if we have a union of records, we will merge the records, basically
				r.addAll(getNestedFieldsFromAvroSchema(unionSchema));
			}
			return r;
		}
		case MAP: {
			// a map is technically nested, but we can not extract the nested fields from
			// the schema, so we leave it as is for now
			// TODO maybe mark this in the node, when we merge avro EventStructure with the
			// regular one its more clear
		}
		default:
			return r;
		}
	}

	private boolean isNestedAvroSchema(Schema schema) {
		// in the end this will basically check for either the schema being of type
		// RECORD or being of type UNION with a nested subfield

		Type schemaType = schema.getType();
		if (isPrimitiveAvroType(schemaType)) {
			return false;
		}
		// there are 6 types that are complex in the avro sense but that does not
		// necessarily lead to nesting:
		// https://avro.apache.org/docs/1.8.1/spec.html#schema_complex
		switch (schemaType) {
		case RECORD:
			return true;
		case ENUM:
		case FIXED:
		case ARRAY: // we might be able to handle these nicer later, for now we use them as is
			return false;
		case MAP:
			// a map is technically nested, but we can not extract the nested fields from
			// the schema, so we leave it as is for now
			return false;
		case UNION: {
			// we use unions a lot for nullable, for now we say a union is nested if any of
			// it's children are nested
			for (Schema subSchema : schema.getTypes()) {
				if (isNestedAvroSchema(subSchema)) {
					return true;
				}
			}
			return false;
		}
		default:
			throw new IllegalStateException(
					"Unimplemented or not properly handled Avro Schema type detected: " + schemaType);
		}
	}

	private boolean isPrimitiveAvroType(Type type) {
		// https://avro.apache.org/docs/1.8.1/spec.html#schema_primitive
		switch (type) {
		case NULL:
		case BOOLEAN:
		case INT:
		case LONG:
		case FLOAT:
		case DOUBLE:
		case BYTES:
		case STRING:
			return true;
		default:
			return false;
		}
	}

	private boolean isNestedJson(JsonNode json) {
		return json.isObject();
	}

}
