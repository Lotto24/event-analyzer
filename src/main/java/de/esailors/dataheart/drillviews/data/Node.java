package de.esailors.dataheart.drillviews.data;

import java.util.HashSet;
import java.util.Set;

public class Node {

	private String name;
	private Set<Node> children;

	public Node(String name) {
		this.name = name;
		// TODO would be nice to be able to set properties for nodes to contain additional information
	}

	public boolean hasChildren() {
		return children != null;
	}

	public void addChild(Node child) {
		if (children == null) {
			children = new HashSet<>();
		}
		children.add(child);
	}

	public String getName() {
		return name;
	}

	public Set<Node> getChildren() {
		return children;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((children == null) ? 0 : children.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
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
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}

	public String toDot() {
		StringBuilder r = new StringBuilder();
		r.append("\"" + name + "\"");
		if(hasChildren()) {
			r.append(" [shape=record]");
		}
		r.append(";\n");
		if (hasChildren()) {
			r.append("subgraph \"cluster_" + name + "\" {\n");
			for (Node child : children) {
				r.append(child.toDot());
				r.append("\"" + name + "\" -> \"" + child.getName() + "\";\n");
			}
			r.append("}\n");
		}
		return r.toString();
	}

}