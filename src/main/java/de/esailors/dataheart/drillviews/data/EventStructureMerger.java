package de.esailors.dataheart.drillviews.data;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Optional;

public class EventStructureMerger {

	private static EventStructureMerger instance;

	public static EventStructureMerger getInstance() {
		if (instance == null) {
			instance = new EventStructureMerger();
		}
		return instance;
	}

	private EventStructureMerger() {
	}

	public Tree mergeEventStructures(EventType eventType, Collection<EventStructure> sourceStructures) {
		Set<Tree> sourceTrees = new HashSet<>();
		for(EventStructure sourceStructure : sourceStructures) {
			sourceTrees.add(sourceStructure.getEventStructureTree());
		}
		return mergeTrees(eventType.getName(), sourceTrees);
	}

	public Tree mergeTrees(String treeName, Set<Tree> sourceTrees) {
		if (sourceTrees == null || sourceTrees.isEmpty()) {
			throw new IllegalArgumentException("Need to be given at least 1 tree to do a merge");
		}

		Tree mergedTree = new Tree(treeName);

		for (Tree sourceTree : sourceTrees) {
			// merged Tree will be incrementally appended with nodes from soure tree
			mergeTreeInto(sourceTree, mergedTree);
		}

		return mergedTree;
	}

	private void mergeTreeInto(Tree sourceTree, Tree mergedTree) {
		Node sourceNode = sourceTree.getRootNode();
		Node mergedNode = mergedTree.getRootNode();
		
		mergeNodeInto(sourceNode, mergedNode, true);
	}

	private void mergeNodeInto(Node sourceNode, Node mergedNode, boolean isRootNode) {
		mergedNode.addProperties(sourceNode.getProperties());
		if (isRootNode) {
			// for root nodes we ignore the name for now
			if(mergedNode.equalIgnoringId(sourceNode)) {
				return;
			}
		} else if(mergedNode.equals(sourceNode)) {
			// already equal
			return;
		}
		for (Node sourceChild : sourceNode.getChildren()) {
			Optional<Node> mergedChildOption = mergedNode.getChildByName(sourceChild.getId());
			if (mergedChildOption.isPresent()) {
				Node mergedChild = mergedChildOption.get();
				mergeNodeInto(sourceChild, mergedChild, false);
			} else {
				// source Node not in merge target yet, just add it
				// TODO this is where we can mark the node as "optional"
				mergedNode.addChild(sourceChild);
			}

		}
	}
}
