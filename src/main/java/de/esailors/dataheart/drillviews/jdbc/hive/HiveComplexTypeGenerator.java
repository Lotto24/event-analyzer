package de.esailors.dataheart.drillviews.jdbc.hive;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Optional;

import de.esailors.dataheart.drillviews.data.Node;
import de.esailors.dataheart.drillviews.data.PrimitiveType;
import de.esailors.dataheart.drillviews.data.PrimitiveTypeDetector;
import de.esailors.dataheart.drillviews.util.CollectionUtil;

public class HiveComplexTypeGenerator {

	private static final Logger log = LogManager.getLogger(HiveComplexTypeGenerator.class.getName());

	private PrimitiveTypeDetector primitiveTypeDetector = new PrimitiveTypeDetector();

	public String generateComplexTypeFor(Node node) {
		StringBuilder r = new StringBuilder();
		generateComplexTypeFor(node, r);
		return r.toString();
	}

	private void generateComplexTypeFor(Node node, StringBuilder r) {
		if (node.hasArrayType()) {
			r.append("array<");
		}
		if (node.hasChildren()) {
			// record -> named struct
			r.append("struct<");
			boolean first = true;
			// order children alphabetically
			Map<String, Node> childMap = node.getChildMap();
			for (String childPath : CollectionUtil.toSortedList(childMap.keySet())) {
				Node child = childMap.get(childPath);
				if (first) {
					first = false;
				} else {
					r.append(",");
				}
				r.append(child.getName());
				r.append(":");
				generateComplexTypeFor(child, r);
			}
			r.append(">");
		} else {
			if (node.hasArrayType()) {
				// an array of primitives
				Optional<PrimitiveType> primitiveArrayItemTypeOption = primitiveTypeDetector
						.primitiveAvroArrayItemTypeForNode(node);
				if (!primitiveArrayItemTypeOption.isPresent()) {
					throw new IllegalStateException(
							"Expect arrays withouth children to have a primitive type: " + node.getId());
				}
				PrimitiveType primitiveType = primitiveArrayItemTypeOption.get();
				r.append(hiveTypeFor(primitiveType));
			} else {
				// primitive type
				Optional<PrimitiveType> primitiveTypeOption = primitiveTypeDetector.primitiveTypeForNode(node);
				PrimitiveType primitiveType;
				if (!primitiveTypeOption.isPresent()) {
					// happens for example for non-avro NULL nodes
					log.warn("Expected node without children to have a primitve type: " + node.getId());
					primitiveType = PrimitiveType.TEXT;
				} else {
					primitiveType = primitiveTypeOption.get();
				}
				r.append(hiveTypeFor(primitiveType));
			}
		}
		if (node.hasArrayType()) {
			r.append(">");
		}
	}

	public String hiveTypeFor(PrimitiveType primitiveType) {
		// TODO would be great to distinguish between int/bigint and float/double
		switch (primitiveType) {
		case BOOLEAN:
			return "boolean";
		case FLOAT:
			return "double";
		case INT:
			return "bigint";
		case TEXT:
			return "string";
		default:
			throw new IllegalArgumentException("Unknown primitive type: " + primitiveType);
		}
	}

}
