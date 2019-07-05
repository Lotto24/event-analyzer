package de.esailors.dataheart.drillviews.processor;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Optional;

import de.esailors.dataheart.drillviews.conf.Config;
import de.esailors.dataheart.drillviews.data.AvroSchema;
import de.esailors.dataheart.drillviews.data.EventStructure;
import de.esailors.dataheart.drillviews.data.EventType;
import de.esailors.dataheart.drillviews.data.Topic;
import de.esailors.dataheart.drillviews.util.SystemUtil;

public class PersisterPaths {

	private static final Logger log = LogManager.getLogger(PersisterPaths.class.getName());

	private static final String README_FILE = "README.md";

	public void initOutputDirectories() {
		wipeDirectory(Config.getInstance().OUTPUT_DIRECTORY);
		ensureDirectoryExists(Config.getInstance().OUTPUT_DIRECTORY);
		ensureDirectoryExists(outputDirectoryPathFor(Config.getInstance().OUTPUT_AVROSCHEMAS_DIRECTORY));
		ensureDirectoryExists(outputDirectoryPathFor(Config.getInstance().OUTPUT_DRILL_DIRECTORY));
		ensureDirectoryExists(outputDirectoryPathFor(Config.getInstance().OUTPUT_SAMPLES_DIRECTORY));
		ensureDirectoryExists(outputDirectoryPathFor(Config.getInstance().OUTPUT_TOPIC_DIRECTORY));
		ensureDirectoryExists(outputDirectoryPathFor(Config.getInstance().OUTPUT_CHANGELOGS_DIRECTORY));
		ensureDirectoryExists(outputDirectoryPathFor(Config.getInstance().OUTPUT_EVENTSTRUCTURES_DIRECTORY));
		ensureDirectoryExists(outputDirectoryPathFor(Config.getInstance().OUTPUT_EVENTTYPE_DIRECTORY));
		ensureDirectoryExists(outputDirectoryPathFor(Config.getInstance().OUTPUT_HIVE_DIRECTORY));
		ensureDirectoryExists(outputDirectoryPathFor(Config.getInstance().OUTPUT_DWH_TABLES_DIRECTORY));
		ensureDirectoryExists(outputDirectoryPathFor(Config.getInstance().OUTPUT_DWH_JOBS_DIRECTORY));
	}

	private void wipeDirectory(String directoryPath) {
		File directory = new File(directoryPath);
		log.debug("Wiping directory: " + directory.getAbsolutePath());
		if (!directory.exists()) {
			return;
		}
		if (!directory.isDirectory()) {
			throw new IllegalArgumentException(
					"Got asked to wipe a path that exists but is not a directory: " + directoryPath);
		}

		// only wipe directories that are somewhere within working directory to avoid
		// absolute mayhem
		Optional<String> workingDirectoryOption = SystemUtil.getWorkingDirectory();
		if (!workingDirectoryOption.isPresent()) {
			throw new IllegalStateException("Unable to determine working directory, refusing to wipe directory");
		}
		String workingDirectoryPath = workingDirectoryOption.get();
		if (!isParentOf(workingDirectoryPath, directory.getAbsolutePath())) {
			throw new IllegalArgumentException("Was asked to wipe a directory (" + directory.getAbsolutePath()
					+ ") that is not part of current working directory: " + workingDirectoryPath);
		}

		try {
			FileUtils.forceDelete(directory);
		} catch (IOException e) {
			throw new IllegalStateException("Unable to wipe directory: " + directory.getAbsolutePath(), e);
		}
	}

	private void ensureDirectoryExists(String directoryPath) {
		File outputDirectory = new File(directoryPath);
		if (!outputDirectory.exists()) {
			try {
				FileUtils.forceMkdir(outputDirectory);
			} catch (IOException e) {
				throw new IllegalStateException("Unable to create directory at " + outputDirectory.getAbsolutePath());
			}
		}
	}

	public String outputDirectoryPathFor(EventStructure eventStructure) {
		return outputDirectoryPathFor(eventStructure.getEventType());
	}

	public String outputDirectoryPathFor(EventType eventType) {
		return outputDirectoryPathFor(Config.getInstance().OUTPUT_EVENTSTRUCTURES_DIRECTORY) + eventType.getName()
				+ File.separator;
	}

	public String outputDirectoryPathFor(AvroSchema avroSchema) {
		return outputDirectoryPathFor(Config.getInstance().OUTPUT_AVROSCHEMAS_DIRECTORY) + avroSchema.getName()
				+ File.separator;
	}

	public String outputDirectoryPathForReadme() {
		return outputDirectoryPathFor("");
	}

	public String outputDirectoryPathFor(String subPath) {
		return Config.getInstance().OUTPUT_DIRECTORY + File.separator + subPath
				+ (subPath.isEmpty() ? "" : File.separator);
	}

	public String fileNameForDrillView(EventType eventType) {
		return eventType.getName() + ".sql";
	}

	public String fileNameForHiveView(EventType eventType) {
		return eventType.getName() + ".sql";
	}

	public String fileNameForTopicReport(Topic topic) {
		return topic.getName() + ".md";
	}

	public String fileNameForEventSamples(EventType eventType) {
		return eventType.getName() + ".json";
	}

	public String fileNameForEventTypeReport(EventType eventType) {
		return fileNameForEventTypeReportByName(eventType.getName());
	}

	public String fileNameForEventTypeReportByName(String eventTypeName) {
		return eventTypeName + ".md";
	}

	public String fileNameForAvroSchemaJson(AvroSchema avroSchema) {
		return avroSchema.getSchemaHash() + ".json";
	}

	public String fileNameForAvroSchemaReport(AvroSchema avroSchema) {
		return avroSchema.getSchemaHash() + ".md";
	}

	public String fileNameForEventStructure(EventStructure eventStructure) {
		return eventStructure.toString() + ".md";
	}

	public String fileNameForEventStructure(String eventStructure) {
		return eventStructure.toString() + ".md";
	}

	public String fileNameForEventStructureDot(EventStructure eventStructure) {
		return eventStructure.toString() + ".dot";
	}

	public String fileNameForEventStructurePlot(EventStructure eventStructure) {
		return eventStructure.toString() + ".png";
	}

	public String fileNameForEventStructureSerialization(EventStructure mergedEventStructure) {
		return fileNameForEventStructureSerialization(mergedEventStructure.getEventType());
	}

	public String fileNameForEventStructureSerialization(EventType eventType) {
		return eventType.getName() + ".tree";
	}
	
	public String fileNameForDwhTable(EventType eventType) {
		return Config.getInstance().DWH_TABLE_SCHEMA + "." + eventType.getName() + ".sql";
	}
	
	public String fileNameForDwhJob(EventType eventType) {
		return eventType.getName().toLowerCase() + ".sql";
	}

	public String fileNameForReadme() {
		return README_FILE;
	}

	public boolean isParentOf(String parentPath, String childPath) {
		// inspired by
		// https://stackoverflow.com/questions/4746671/how-to-check-if-a-given-path-is-possible-child-of-another-path
		Path parent = Paths.get(parentPath).toAbsolutePath();
		Path child = Paths.get(childPath).toAbsolutePath();
		boolean r = child.startsWith(parent);
		log.debug("Checking if " + parent.toString() + " is parent of " + child.toString() + ": " + r);
		return r;
	}

	public String relativePathBetween(String sourcePath, String targetPath) {
		// inspired by
		// https://stackoverflow.com/questions/204784/how-to-construct-a-relative-path-in-java-from-two-absolute-paths-or-urls
		return Paths.get(sourcePath).relativize(Paths.get(targetPath)).toString();
	}
	
}
