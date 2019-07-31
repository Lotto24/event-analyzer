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

import de.esailors.dataheart.drillviews.util.JsonUtil;
import de.esailors.dataheart.drillviews.util.JsonUtil.JsonType;

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

		r.getRootNode().addProperty(NodePropertyType.SOURCE, "EVENT");

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
			JsonType jsonType = JsonUtil.getJsonType(fieldJson);
			node.addProperty(NodePropertyType.JSON_TYPE, jsonType);
			if (jsonType.equals(JsonType.NULL)) {
				node.setOptional(true);
			}
			if (isNestedJson(fieldJson)) {
				if (fieldJson.isObject()) {
					extendTreeWithJsonFields(node, fieldJson, true);
				} else if (fieldJson.isArray()) {
					Iterator<JsonNode> arrayItemIterator = fieldJson.getElements();
					while (arrayItemIterator.hasNext()) {
						JsonNode arrayItem = arrayItemIterator.next();
						node.addProperty(NodePropertyType.JSON_ARRAY_ITEM_TYPE, JsonUtil.getJsonType(arrayItem));
						if (isNestedJson(arrayItem)) {
							extendTreeWithJsonFields(node, arrayItem, true);
						}
					}
				} else {
					throw new IllegalStateException("Expect nested jsons to either be objects/records or arrays");
				}
			}
		}
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
		r.getRootNode().addProperty(NodePropertyType.SOURCE, "AVRO");

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
		node.addProperty(NodePropertyType.AVRO_TYPE, fieldType);

		// list enum values
		if (fieldType.equals(Type.ENUM)) {
			for (String enumSymbol : field.schema().getEnumSymbols()) {
				node.addProperty(NodePropertyType.AVRO_ENUM_SYMBOL, enumSymbol);
			}
		}

		// list union types and mark node as optional if union types contains NULL
		if (fieldType.equals(Type.UNION)) {
			boolean sawNullType = false;
			for (Schema unionSchema : field.schema().getTypes()) {
				node.addProperty(NodePropertyType.AVRO_UNION_TYPE, unionSchema.getType().toString());
				if (unionSchema.getType().equals(Type.NULL)) {
					sawNullType = true;
				}
			}
			if (sawNullType) {
				node.setOptional(true);
			}
		}

		// describe array item
		if (fieldType.equals(Type.ARRAY)) {
			Schema arrayItemSchema = field.schema().getElementType();
			node.addProperty(NodePropertyType.AVRO_ARRAY_ITEM_TYPE, arrayItemSchema.getType().toString());
			node.addProperty(NodePropertyType.AVRO_ARRAY_ITEM_NAME, arrayItemSchema.getName());
			// if it's an array of enums we can decorate the avro enum symbols as well
			if (arrayItemSchema.getType().equals(Type.ENUM)) {
				for (String enumSymbol : arrayItemSchema.getEnumSymbols()) {
					node.addProperty(NodePropertyType.AVRO_ENUM_SYMBOL, enumSymbol);
				}
			}
		}

		// list map value types
		if (fieldType.equals(Type.MAP)) {
			Schema valueType = field.schema().getValueType();
			node.addProperty(NodePropertyType.AVRO_MAP_VALUE_TYPE, valueType.getType().toString());
			if (valueType.getType().equals(Type.UNION)) {
				for (Schema valueSchema : valueType.getTypes()) {
					node.addProperty(NodePropertyType.AVRO_MAP_VALUE_TYPE, valueSchema.getType().toString());
				}
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
		case ARRAY: {
			r.addAll(getNestedFieldsFromAvroSchema(avroSchema.getElementType()));
			return r;
		}
		case MAP: {
			// a map is technically nested, but we can not extract the nested fields from
			// the schema, so we leave it as is for now
			// we mark this in the node, when we merge avro EventStructure with the
			// regular one so its more clear
		}
		default:
			return r;
		}
	}

	private boolean isNestedAvroSchema(Schema schema) {
		// in the end this will basically check for either the schema being of type
		// RECORD or being of type UNION or ARRAY with a nested subfield

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
		case FIXED: // we might be able to handle these nicer later, for now we use them as is
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
		case ARRAY: {
			// if the items inside an array are nested we treat the array as nested
			return isNestedAvroSchema(schema.getElementType());
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
		return json.isContainerNode();
	}

}
