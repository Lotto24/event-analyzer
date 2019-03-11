package de.esailors.dataheart.drillviews.git;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.google.common.base.Optional;

public class SystemUtil {

	public static Optional<String> getLocalHostname() {
		try {
			return Optional.of(InetAddress.getLocalHost().getHostName());
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return Optional.absent();
		}
	}
	
	public static Optional<String> getCurrentUser() {
		String systemUser = System.getProperty("user.name");
		if(systemUser != null && !systemUser.isEmpty()) {
			return Optional.of(systemUser);
		} else {
			return Optional.absent();
		}
	}
	
}
