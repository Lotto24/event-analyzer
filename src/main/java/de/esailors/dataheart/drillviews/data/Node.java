package de.esailors.dataheart.drillviews.data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Optional;

public class Node {

	// i.e. parterinfo.trackinginfo.url
	private String id;
	// i.e. url
	private String name;
	private Map<String, Node> children;
	// TODO properties currently experimental, only for avro now (not taken over in
	// merges)
	private Map<String, Object> properties = new HashMap<>();

	public Node(String id, String name) {
		this.id = id;
		this.name = name;
	}

	public void addProperty(String name, Object property) {
		properties.put(name, property);
	}
	
	public boolean hasProperties() {
		return !properties.isEmpty();
	}

	public Optional<Node> getChildByName(String childName) {
		if (children == null || children.get(childName) == null) {
			return Optional.absent();
		}
		return Optional.of(children.get(childName));
	}

	public boolean hasChildren() {
		return children != null;
	}

	public void addChild(Node child) {
		if (children == null) {
			children = new HashMap<>();
		}
		children.put(child.getId(), child);
	}

	public String getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public Set<Node> getChildren() {
		// values() can not have duplicates in our case, as they are mapped by name and
		// name is part of equals check
		if (children == null) {
			return new HashSet<>();
		} else {
			return new HashSet<>(children.values());
		}
	}

	public String toDot() {
		StringBuilder r = new StringBuilder();
		r.append("\"" + id + "\"");
		
		List<String> customization = new ArrayList<>();

		if(hasProperties() ) {
			String label = id + "\\n";
			
			// sort the keys so it looks nicer
			List<String> sortedPropertyList = new ArrayList<>(properties.keySet());
			Collections.sort(sortedPropertyList);
			for(String property : sortedPropertyList) {
				label += property + "="  + properties.get(property) + "\\l";
			}
			customization.add("label=\""+label+"\"");
		}
		
		if (hasChildren()) {
			customization.add("shape=record");
		}
		
		
		if(!customization.isEmpty()) {
			r.append(" [" + String.join(",", customization) + "]");
		}
		
		r.append(";\n");
		if (hasChildren()) {
			r.append("subgraph \"cluster_" + id + "\" {\n");
			for (Node child : children.values()) {
				r.append(child.toDot());
				r.append("\"" + id + "\" -> \"" + child.getId() + "\";\n");
			}
			r.append("}\n");
		}
		return r.toString();
	}

	public boolean equalChildren(Node otherNode) {
		if (this == otherNode)
			return true;
		if (otherNode == null)
			return false;
		if (children == null) {
			if (otherNode.children != null)
				return false;
		} else if (!children.equals(otherNode.children))
			return false;
		return true;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((children == null) ? 0 : children.hashCode());
		result = prime * result + ((id == null) ? 0 : id.hashCode());
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
		return true;
	}

}