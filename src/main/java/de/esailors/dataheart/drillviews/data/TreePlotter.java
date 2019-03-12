package de.esailors.dataheart.drillviews.data;

import java.io.File;

import de.esailors.dataheart.drillviews.git.SystemUtil;
import de.esailors.dataheart.drillviews.processor.FileWriterUtil;

public class TreePlotter {

	private static TreePlotter instance;
	
	public static TreePlotter getInstance() {
		if(instance == null) {
			instance = new TreePlotter();
		}
		return instance;
	}
	
	private TreePlotter() {
		
	}
	
	public void plotTree(Tree tree, String dotFileFolder, String dotFileName, String pngFileFolder, String pngFileName) {
		// TODO very quick and dirty for now
		String newRender = tree.toDot();
		// TODO check if .dot changed from git repository, if not don't render it again
		
		FileWriterUtil.writeFile(dotFileFolder, dotFileName, newRender);
		
		String renderDotCommand = "dot -Tpng " + dotFileFolder + File.separator + dotFileName + " -o" + pngFileFolder + File.separator + pngFileName;

		SystemUtil.executeCommand(renderDotCommand);
	}
}
