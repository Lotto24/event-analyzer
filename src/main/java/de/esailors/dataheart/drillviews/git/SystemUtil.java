package de.esailors.dataheart.drillviews.git;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Optional;

public class SystemUtil {

	private static final Logger log = LogManager.getLogger(SystemUtil.class.getName());

	public static Optional<String> getLocalHostname() {
		try {
			String hostName = InetAddress.getLocalHost().getHostName();
			log.debug("Determined host name: " + hostName);
			return Optional.of(hostName);
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return Optional.absent();
		}
	}

	public static Optional<String> getCurrentUser() {
		String systemUser = System.getProperty("user.name");
		log.debug("Determined current user: " + systemUser);
		if (systemUser != null && !systemUser.isEmpty()) {
			return Optional.of(systemUser);
		} else {
			return Optional.absent();
		}
	}

	public static void executeCommand(String command) {
		// inspired by https://alvinalexander.com/java/edu/pj/pj010016

		log.info("Executing system command: " + command);
		
		String s = null;
		try {

			Process p = Runtime.getRuntime().exec(command);

			BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
			BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));

			// read the output from the command
			while ((s = stdInput.readLine()) != null) {
				log.debug("STDOUT: " + s);
			}

			// read any errors from the attempted command
			while ((s = stdError.readLine()) != null) {
				log.warn("STDERR: " + s);
			}

		} catch (IOException e) {
			log.error("Error while executing system command: " + command, e);
		}
	}

}
