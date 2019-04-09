package de.esailors.dataheart.drillviews.data;

/**
 * Wow, there is no official implemenatation for a tree in Java oO
 * 
 * @author andre.mis
 *
 */
public class Tree {
	
	private Node rootNode;
	
	public Tree(String rootNodeName) {
		this.rootNode = new Node(rootNodeName, rootNodeName);
	}
	
	public Node getRootNode() {
		return rootNode;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((rootNode == null) ? 0 : rootNode.hashCode());
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
		Tree other = (Tree) obj;
		if (rootNode == null) {
			if (other.rootNode != null)
				return false;
		} else if (!rootNode.equals(other.rootNode))
			return false;
		return true;
	}
	
}
