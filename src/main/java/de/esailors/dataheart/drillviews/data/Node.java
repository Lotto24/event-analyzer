package de.esailors.dataheart.drillviews.data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.avro.Schema.Type;

import com.google.common.base.Optional;

import de.esailors.dataheart.drillviews.util.CollectionUtil;

public class Node implements Serializable {

	private static final long serialVersionUID = 1L;

	// i.e. parterinfo.trackinginfo.url
	private String id;
	// i.e. url
	private String name;
	// id -> child Node
	private Map<String, Node> children = new HashMap<>();
	private Map<String, Set<String>> properties = new HashMap<>();
	private boolean isOptional;

	public Node(String id, String name) {
		this.id = id;
		this.name = name;
		this.isOptional = false;
	}

	public Node(Node toCopy) {
		// copy constructor, copies children as well
		this.id = toCopy.id;
		this.name = toCopy.name;
		this.isOptional = toCopy.isOptional;
		this.properties.putAll(toCopy.properties);
		for (Entry<String, Node> toCopyChildEntry : toCopy.children.entrySet()) {
			children.put(toCopyChildEntry.getKey(), new Node(toCopyChildEntry.getValue()));
		}
	}

	public void addProperty(NodePropertyType name, String property) {
		if (properties.get(name.toString()) == null) {
			properties.put(name.toString(), new HashSet<>());
		}
		properties.get(name.toString()).add(property);
	}

	public void addProperties(Node anotherNode) {
		for (String propertyName : anotherNode.properties.keySet()) {
			addPropertySet(propertyName, anotherNode.properties.get(propertyName));
		}
	}

	private void addPropertySet(String name, Set<String> propertiesToAdd) {
		if (properties.get(name) == null) {
			properties.put(name, new HashSet<>());
		}
		for (String property : propertiesToAdd) {
			properties.get(name).add(property);
		}
	}

	public boolean hasProperties() {
		return !properties.isEmpty();
	}

	public Set<String> getProperty(NodePropertyType name) {
		return properties.get(name.toString());
	}

	public List<NodePropertyType> getPropertyKeys() {
		Set<NodePropertyType> r = new HashSet<>();
		for (String propertyName : properties.keySet()) {
			r.add(NodePropertyType.valueOf(propertyName));
		}
		return CollectionUtil.toSortedList(r);
	}

	public Optional<Node> getChildById(String childId) {
		if (children == null || children.get(childId) == null) {
			return Optional.absent();
		}
		return Optional.of(children.get(childId));
	}

	public boolean hasChildren() {
		return !children.isEmpty();
	}

	public void addChild(Node child) {
		children.put(child.getId(), child);
	}

	public boolean hasArrayType() {
		// TODO for now we only treat arrays properly when event is defined with avro
		// getProperty(NodePropertyType.JSON_TYPE).contains(JsonType.ARRAY.toString());
		Set<String> avroTypes = getProperty(NodePropertyType.AVRO_TYPE);
		Set<String> avroUnionTypes = getProperty(NodePropertyType.AVRO_UNION_TYPE);
		return (avroTypes != null && avroTypes.contains(Type.ARRAY.toString()))
				|| (avroUnionTypes != null && avroUnionTypes.contains(Type.ARRAY.toString()));
	}
	
	public String getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public boolean isOptional() {
		return isOptional;
	}

	public void setOptional(boolean isOptional) {
		this.isOptional = isOptional;
	}

	public Set<Node> getChildren() {
		// values() can not have duplicates in our case, as they are mapped by name and
		// name is part of equals check
		return new HashSet<>(children.values());
	}

	public Map<String, Node> getChildMap() {
		return children;
	}

	public boolean equalIgnoringId(Node otherNode) {
		if (this == otherNode)
			return true;
		if (otherNode == null)
			return false;
		if (children == null) {
			if (otherNode.children != null)
				return false;
		} else if (!children.equals(otherNode.children))
			return false;
		if (isOptional != otherNode.isOptional)
			return false;
		return true;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((children == null) ? 0 : children.hashCode());
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((properties == null) ? 0 : properties.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Node other = (Node) obj;
		if (children == null) {
			if (other.children != null)
				return false;
		} else if (!children.equals(other.children))
			return false;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (properties == null) {
			if (other.properties != null)
				return false;
		} else if (!properties.equals(other.properties))
			return false;
		return true;
	}

}