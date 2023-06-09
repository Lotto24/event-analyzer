package de.esailors.dataheart.drillviews.processor;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FileUtil {

	private static final Logger log = LogManager.getLogger(FileUtil.class.getName());

	public static void writeFile(String folder, String fileName, String content) {
		File viewFolder = new File(folder);
		if (!viewFolder.exists()) {
			viewFolder.mkdirs();
		}

		File f = new File(folder + File.separator + fileName);
		log.debug("Writing file to: " + f.getAbsolutePath());
		if (f.exists()) {
			log.warn("Overwriting file that already exists: " + f.getAbsolutePath());
			if (!f.delete()) {
				throw new IllegalStateException(
						"Writing to file that existed before and can not be deleted: " + f.getAbsolutePath());
			}
		}

		try {
			f.createNewFile();
		} catch (IOException e) {
			log.error("Unable to create output file at " + f.getAbsolutePath(), e);
		}
		FileWriter fileWriter = null;
		try {
			fileWriter = new FileWriter(f);
			fileWriter.write(content);
			fileWriter.flush();
		} catch (IOException e) {
			log.error("Unable to create FileWriter for File at " + f.getAbsolutePath(), e);
		} finally {
			try {
				if (fileWriter != null) {
					fileWriter.close();
				}
			} catch (IOException e) {
				log.error("Unable to close fileWriter", e);
			}
		}
	}
	
	public static String loadFromResources(String path) {
		ClassLoader classLoader = FileUtil.class.getClassLoader();
		try {
			return IOUtils.toString(classLoader.getResourceAsStream(path));
		} catch (IOException e) {
			throw new IllegalStateException("Unable to load from resources: " + path, e);
		}
	}

}
