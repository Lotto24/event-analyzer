package de.esailors.dataheart.drillviews.processor;

import java.util.HashSet;
import java.util.Set;

import de.esailors.dataheart.drillviews.data.EventStructure;
import de.esailors.dataheart.drillviews.data.Node;
import de.esailors.dataheart.drillviews.data.Tree;
import de.esailors.dataheart.drillviews.util.CollectionUtil;

public class Dotter {

	private EventStructure eventStructure;
	private StringBuilder r = new StringBuilder();
	private int indentationLevel = 0;

	public Dotter(EventStructure eventStructure) {
		this.eventStructure = eventStructure;
	}

	public String generateDot() {
		generateDot(eventStructure.getEventStructureTree());
		return r.toString();
	}

	private void generateDot(Tree tree) {

		appendLine("digraph G {");
		increaseIndentationLevel();
		appendLine("rankdir=LR;");
		generateDot(tree.getRootNode());
		decreaseIndentationLevel();
		appendLine("}");
	}

	private void generateDot(Node node) {
		indent();
		appendNodeId(node);
		append(" [");
		appendNewLine();
		increaseIndentationLevel();
		appendLine("shape=plaintext");
		appendLine("label=<");
		appendTableStart(!node.hasChildren());

		appendTableHeader(node);
		appendNodeProperties(node);

		appendTableEnd();
		appendLine(">");
		decreaseIndentationLevel();
		appendLine("];");

//		List<String> customization = new ArrayList<>();
//
//		if (node.hasProperties()) {
//			String label = node.getId() + "\\n";
//
//			// sort the keys so it looks nicer
//			List<String> sortedPropertyList = CollectionUtil.toSortedList(node.getProperties().keySet());
//			for (String property : sortedPropertyList) {
//				label += property + "="
//						+ String.join(", ", CollectionUtil.toSortedList(node.getProperties().get(property))) + "\\l";
//			}
//			customization.add("label=\"" + label + "\"");
//		}
//
//		if (node.hasChildren()) {
//			customization.add("shape=record");
//		}
//
//		if (node.isOptional()) {
//			customization.add("style=dotted");
//		}
//
//		if (!customization.isEmpty()) {
//			append(" [" + String.join(",", customization) + "]");
//		}
//
//		append(";\n");

		if (node.hasChildren()) {
			appendLine("subgraph \"cluster_" + node.getId() + "\" {");
			for (Node child : node.getChildren()) {
				generateDot(child);
				generateEdge(node, child);
			}
			appendLine("}");
		}
	}

	private void appendNodeProperties(Node node) {
		int rowNumber = 0;
		if (!node.getId().equals(node.getName())) {
			appendTableRow("PATH", node.getId());
			rowNumber++;
		}
		
		if (!node.hasProperties()) {
			return;
		}

		for (String entryKey : CollectionUtil.toSortedList(node.getProperties().keySet())) {
			rowNumber += appendTableRow(entryKey, node.getProperties().get(entryKey), rowNumber);
		}
	}

	private void appendTableRow(String key, String value) {
		HashSet<String> values = new HashSet<>();
		values.add(value);
		appendTableRow(key, values, 0);
	}

	private int appendTableRow(String key, Set<String> values, int rowNumber) {
		indent();
		append("<tr><td valign='top' align='left' rowspan='");
		append(values.size());
		append("'><i>");
		append(key);
		append("</i></td>");
		appendNewLine();
		increaseIndentationLevel();
		int internalRowNumber = 0;
		for (String value : CollectionUtil.toSortedList(values)) {
			indent();
			if (internalRowNumber > 0) {
				append("<tr>");
			}
			append("<td align='left'");
			if((rowNumber + internalRowNumber) % 2 == 1) {
				append(" bgcolor='#DDDDDD'");
			}
			internalRowNumber++;
			append(">");
			append(value);
			append("</td></tr>");
			appendNewLine();
		}
		decreaseIndentationLevel();
		return internalRowNumber;
	}

	private void appendTableHeader(Node node) {
		indent();
		append("<tr><td colspan='2'><font point-size='20'><b>");
		append(node.getName());
		append("</b></font></td></tr>");
		appendNewLine();
	}

	private void appendTableStart(boolean rounded) {
		append("<table border='1' cellborder='0' cellspacing='2' cellpadding='2'");
		if (rounded) {
			append(" style='rounded'");
		}
		append(">");
		appendNewLine();
		increaseIndentationLevel();
	}

	private void appendTableEnd() {
		decreaseIndentationLevel();
		appendLine("</table>");
	}

	private void generateEdge(Node node, Node child) {
		indent();
		appendNodeId(node);
		append(" -> ");
		appendNodeId(child);
		append(";");
		appendNewLine();
	}

	private void appendNodeId(Node node) {
		append("\"");
		append(node.getId());
		append("\"");
	}

	private void appendLine(String line) {
		indent();
		append(line);
		appendNewLine();
	}

	private void indent() {
		for (int i = 0; i < indentationLevel; i++) {
			append("  ");
		}
	}

	private void appendNewLine() {
		append(System.lineSeparator());
	}

	private void append(String toAppend) {
		r.append(toAppend);
	}

	private void append(int toAppend) {
		r.append(toAppend);
	}

	private void increaseIndentationLevel() {
		indentationLevel++;
	}

	private void decreaseIndentationLevel() {
		if (indentationLevel <= 0) {
			throw new IllegalStateException(
					"Got asked to decrease indentation level but level already: " + indentationLevel);
		}
		indentationLevel--;
	}
}
