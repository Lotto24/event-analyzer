package de.esailors.dataheart.drillviews.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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
		List<Tree> sourceTrees = new ArrayList<>();
		for (EventStructure sourceStructure : sourceStructures) {
			sourceTrees.add(sourceStructure.getEventStructureTree());
		}
		return mergeTrees(eventType.getName(), sourceTrees, sourceStructures);
	}

	public Tree mergeTrees(String treeName, List<Tree> sourceTrees, Collection<EventStructure> sourceStructures) {
		if (sourceTrees == null || sourceTrees.isEmpty()) {
			throw new IllegalArgumentException("Need to be given at least 1 tree to do a merge");
		}

		Tree mergedTree = new Tree(treeName);

		mergedTree.getRootNode().addProperty(NodePropertyType.SOURCE, "MERGE");
		for (EventStructure sourceStructure : sourceStructures) {
			// need to exclude deserialized sources, otherwise each run will produce a new
			// structure even if nothing changes
			if (!sourceStructure.getSource().getType().equals(EventStructureSource.Type.DESERIALIZED)) {
				mergedTree.getRootNode().addProperty(NodePropertyType.SOURCE_STRUCTURE, sourceStructure.toString());
			}
		}

		boolean markOptionality = false;
		for (Tree sourceTree : sourceTrees) {
			// merged Tree will be incrementally appended with nodes from source tree
			mergeTreeInto(sourceTree, mergedTree, markOptionality);
			// in the first iteration we start with an empty tree, only after that does it
			// make sense to mark optionality
			markOptionality = true;
		}

		return mergedTree;
	}

	private void mergeTreeInto(Tree sourceTree, Tree mergedTree, boolean markOptionality) {
		Node sourceNode = sourceTree.getRootNode();
		Node mergedNode = mergedTree.getRootNode();

		mergeNodeInto(sourceNode, mergedNode, true, markOptionality);
	}

	private void mergeNodeInto(Node sourceNode, Node mergedNode, boolean isRootNode, boolean markOptionality) {
		mergedNode.addProperties(sourceNode);
		if (sourceNode.isOptional()) {
			mergedNode.setOptional(true);
		}
		if (isRootNode) {
			// for root nodes we ignore the name/id
			if (mergedNode.equalIgnoringId(sourceNode)) {
				return;
			}
		} else if (mergedNode.equals(sourceNode)) {
			// already equal
			return;
		}
		for (Node sourceChild : sourceNode.getChildren()) {
			Optional<Node> mergedChildOption = mergedNode.getChildById(sourceChild.getId());
			if (mergedChildOption.isPresent()) {
				Node mergedChild = mergedChildOption.get();
				mergeNodeInto(sourceChild, mergedChild, false, markOptionality);
			} else {
				// source Node not in merge target yet, just add it
				Node sourceChildCopy = new Node(sourceChild);
				if (markOptionality) {
					sourceChildCopy.setOptional(true);
				}
				mergedNode.addChild(sourceChildCopy);
			}
		}
		// we need to traverse the mergedNodes as well to check for optionality
		if (markOptionality) {
			for (Node mergeChild : mergedNode.getChildren()) {
				Optional<Node> sourceChildOption = sourceNode.getChildById(mergeChild.getId());
				if (!sourceChildOption.isPresent()) {
					mergeChild.setOptional(true);
				}
			}
		}
	}
}
