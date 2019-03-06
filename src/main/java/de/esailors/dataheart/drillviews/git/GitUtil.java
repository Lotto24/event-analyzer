package de.esailors.dataheart.drillviews.git;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.api.CloneCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.PushCommand;
import org.eclipse.jgit.api.TransportCommand;
import org.eclipse.jgit.api.TransportConfigCallback;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.transport.JschConfigSessionFactory;
import org.eclipse.jgit.transport.OpenSshConfig.Host;
import org.eclipse.jgit.transport.SshSessionFactory;
import org.eclipse.jgit.transport.SshTransport;
import org.eclipse.jgit.transport.Transport;
import org.eclipse.jgit.util.FS;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

import de.esailors.dataheart.drillviews.conf.Config;

public class GitUtil {

	// TODO move to config
	private String gitSshKey = "/home/andre.mis/.ssh/team_id_rsa";
	private String gitUri = "git@srv-git-01-hh1.alinghi.tipp24.net:andre-mis/drill-views.git";
	private String gitBranch = "master";
	private String gitRemote = "origin";

	private static final Logger log = LogManager.getLogger(GitUtil.class.getName());

	private Config config;

	private File gitDirectory;
	private SshSessionFactory sshSessionFactory;
	private Git git;

	public GitUtil(Config config) {
		this.config = config;

		initShutdownHook();
		initSshSessionFactory();
		initRepository();
	}

	private void initRepository() {
		File gitDirectory = initFreshGitDirectory();
		log.info("Initializing git repository at " + gitDirectory.getAbsolutePath());

		CloneCommand cloneCommand = Git.cloneRepository();
		cloneCommand.setURI(gitUri);
		configureAuthentication(cloneCommand);
		cloneCommand.setBranch(gitBranch);
		cloneCommand.setRemote(gitRemote);
		cloneCommand.setDirectory(gitDirectory);
		try {
			git = cloneCommand.call();
		} catch (GitAPIException e) {
			throw new IllegalStateException("Unable to clone git repo", e);
		}
	
	}

	private File initFreshGitDirectory() {
		String gitDirectoryPath = "/tmp/gittest";
		gitDirectory = new File(gitDirectoryPath);
		if (gitDirectory.exists()) {
			log.warn("Git directory does already exist, deleting it first " + gitDirectoryPath);
			try {
				FileUtils.deleteDirectory(gitDirectory);
			} catch (IOException e) {
				throw new IllegalStateException("Unable to delete gitDirectory at " + gitDirectoryPath, e);
			}
		}
		return gitDirectory;
	}

	private void close() {
		if (git != null) {
			// TODO maybe warn of uncommitted changes?
			git.close();
		}
	}

	private void initShutdownHook() {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				log.debug("Shutdown Hook triggered");
				close();
			}
		});
	}

	public void addToRepository(String filePath) {
		File fileToAdd = new File(filePath);
		if (!fileToAdd.exists()) {
			throw new IllegalArgumentException("Unable to add non existing path to repository: " + filePath);
		}
		String newlyAddedPath = gitDirectory.getAbsolutePath() + File.separatorChar + fileToAdd.getName();
		log.info("Adding " + filePath + " to git repository: " + newlyAddedPath);
		if (fileToAdd.isDirectory()) {
			try {
				File gitDirectorySubfolder = new File(newlyAddedPath);
				FileUtils.copyDirectory(fileToAdd, gitDirectorySubfolder);
			} catch (IOException e) {
				throw new IllegalStateException("Unable to copy directory to git repository: " + filePath, e);
			}
		} else if (fileToAdd.isFile()) {
			try {
				FileUtils.copyFileToDirectory(fileToAdd, gitDirectory);
			} catch (IOException e) {
				throw new IllegalStateException("Unable to copy file to git repository: " + filePath, e);
			}
		} else {
			throw new IllegalArgumentException("Received a path that is neither a file nor a directory: " + filePath);
		}
		try {
			git.add().addFilepattern(fileToAdd.getName()).call();
		} catch (GitAPIException e) {
			throw new IllegalStateException("Unable to add to git repository: " + filePath, e);
		}
	}

	public void commitAndPush() {
		String commitMessage = "first automated commit with ssh";
		log.info("Pushing changes to git: " + commitMessage);
		try {
			git.commit().setMessage(commitMessage).call();
			PushCommand push = git.push();
			configureAuthentication(push);
			push.call();
		} catch (GitAPIException e) {
			throw new IllegalStateException("Unable to commit and push changes to git", e);
		}
	}

	private void configureAuthentication(TransportCommand<?, ?> transportCommand) {

		transportCommand.setTransportConfigCallback(new TransportConfigCallback() {
			@Override
			public void configure(Transport transport) {
				SshTransport sshTransport = (SshTransport) transport;
				sshTransport.setSshSessionFactory(sshSessionFactory);
			}
		});
	}

	private void initSshSessionFactory() {
		sshSessionFactory = new JschConfigSessionFactory() {
			@Override
			protected void configure(Host host, Session session) {
				// do nothing
			}

			@Override
			protected JSch createDefaultJSch(FS fs) throws JSchException {
				JSch defaultJSch = super.createDefaultJSch(fs);
				defaultJSch.removeAllIdentity();
				defaultJSch.addIdentity(gitSshKey);
				return defaultJSch;
			}

		};
	}

}
