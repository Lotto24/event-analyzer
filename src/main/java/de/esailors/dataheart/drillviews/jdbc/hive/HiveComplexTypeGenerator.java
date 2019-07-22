package de.esailors.dataheart.drillviews.jdbc.hive;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Optional;

import de.esailors.dataheart.drillviews.data.Node;
import de.esailors.dataheart.drillviews.data.PrimitiveType;
import de.esailors.dataheart.drillviews.data.PrimitiveTypeDetector;

public class HiveComplexTypeGenerator {

	private static final Logger log = LogManager.getLogger(HiveComplexTypeGenerator.class.getName());

	private PrimitiveTypeDetector primitiveTypeDetector = new PrimitiveTypeDetector();
	
	public String generateComplexTypeFor(Node node) {
		StringBuilder r = new StringBuilder();
		generateComplexTypeFor(node, r);
		return r.toString();
	}

	private void generateComplexTypeFor(Node node, StringBuilder r) {
		if (node.hasChildren()) {
			if(node.hasArrayType()) {
				r.append("array<");
			}
			// record -> named struct
			r.append("struct<");
			boolean first = true;
			for (Node child : node.getChildren()) {
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
			if(node.hasArrayType()) {
				r.append(">");
			}
		} else {
			// primitive type
			// TODO quick n dirty for now
			Optional<PrimitiveType> primitiveTypeOption = primitiveTypeDetector.primitiveTypeForNode(node);
			if(!primitiveTypeOption.isPresent()) {
				// TODO just use string
				throw new IllegalStateException("Expect node without children to have a primitve type: " + node.getId());
			}
			PrimitiveType primitiveType = primitiveTypeOption.get();
			r.append(hiveTypeFor(primitiveType));
		}
	}

	public String hiveTypeFor(PrimitiveType primitiveType) {
		// TODO would be great to distinguish between int/bigint and float/double
		switch(primitiveType) {
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
