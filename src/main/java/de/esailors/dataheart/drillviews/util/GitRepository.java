package de.esailors.dataheart.drillviews.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.api.CloneCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.LsRemoteCommand;
import org.eclipse.jgit.api.PullCommand;
import org.eclipse.jgit.api.PullResult;
import org.eclipse.jgit.api.Status;
import org.eclipse.jgit.api.TransportCommand;
import org.eclipse.jgit.api.TransportConfigCallback;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.InvalidRemoteException;
import org.eclipse.jgit.api.errors.TransportException;
import org.eclipse.jgit.errors.NoWorkTreeException;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.JschConfigSessionFactory;
import org.eclipse.jgit.transport.OpenSshConfig.Host;
import org.eclipse.jgit.transport.SshSessionFactory;
import org.eclipse.jgit.transport.SshTransport;
import org.eclipse.jgit.transport.Transport;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;
import org.eclipse.jgit.util.FS;

import com.google.common.base.Optional;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

import de.esailors.dataheart.drillviews.conf.Config;

public class GitRepository {

	// authentication via ssh is not possible from livec (port not opened), we need
	// to support http as well
	public enum AuthenticationMethodType {
		SSH, HTTP
	}

	private static final Logger log = LogManager.getLogger(GitRepository.class.getName());

	private AuthenticationMethodType authenticationMethodType;
	private File localGitRepositoryDirectory;
	private SshSessionFactory sshSessionFactory;
	private Git git;

	private CredentialsProvider credentialsProvider;

	public GitRepository() {
		initAuthenticationType();
		initShutdownHook();
		initRepository();
	}

	private void initAuthenticationType() {
		String authenticationMethodTypeConfigValue = Config.getInstance().GIT_AUTHENTICATION_METHOD;
		switch (authenticationMethodTypeConfigValue.toUpperCase()) {
		case "HTTP": {
			authenticationMethodType = AuthenticationMethodType.HTTP;
			initCredentialsProvider();
			break;
		}
		case "SSH": {
			authenticationMethodType = AuthenticationMethodType.SSH;
			initSshSessionFactory();
			break;
		}
		default: {
			throw new IllegalStateException(
					"Unknown config value for git authentication method, expecting either ssh or http - received: "
							+ authenticationMethodTypeConfigValue);
		}
		}
	}

	private void initCredentialsProvider() {
		this.credentialsProvider = new UsernamePasswordCredentialsProvider(Config.getInstance().GIT_AUTHENTICATION_USER,
				Config.getInstance().GIT_AUTHENTICATION_PASSWORD);
	}

	private void initRepository() {

		localGitRepositoryDirectory = new File(Config.getInstance().GIT_LOCAL_REPOSITORY_PATH);
		log.info("Initializing git repository at " + localGitRepositoryDirectory.getAbsolutePath());
		if (!localGitRepositoryDirectory.exists()) {
			log.info("Repository directory path is empty, cloning repository to: "
					+ localGitRepositoryDirectory.getAbsolutePath());
			cloneRepositoryToDirectory(localGitRepositoryDirectory);
			return;
		}

		// if the repository already exists locally do not delete it and clone new
		// (unless it has local modifications) instead just pull update and reuse it

		log.debug("Git directroy path already exists, trying to reuse repository");
		// check if directory already is a repository
		File existingDirectoryRepository = new File(
				localGitRepositoryDirectory.getAbsolutePath() + File.separator + ".git/");

		if (existingDirectoryRepository.exists() && existingDirectoryRepository.isDirectory()) {

			// see if there are local changes, if yes fail, if not pull remote changes if
			// necessary
			try {
				git = Git.open(existingDirectoryRepository);
				checkStatusIsUnmodified();
				if (!checkNoUnpushedCommits()) {
					pullFromRemote();
				}

			} catch (IOException | NoWorkTreeException | GitAPIException e) {
				throw new IllegalStateException(
						"Git directory is not empty and already contains a git repository but I can't open it: "
								+ existingDirectoryRepository.getAbsolutePath(),
						e);
			}

		} else {
			throw new IllegalStateException("Local git repository path is not empty but also not a repository: "
					+ Config.getInstance().GIT_LOCAL_REPOSITORY_PATH);
		}
	}

	private void pullFromRemote() {
		log.info("Pulling changes from remote");
		PullCommand pullCommand = git.pull();
		pullCommand.setRemote(Config.getInstance().GIT_REMOTE_NAME);
		pullCommand.setRemoteBranchName(Config.getInstance().GIT_BRANCH);
		configureAuthentication(pullCommand);
		try {
			PullResult pullResult = pullCommand.call();
			log.debug("Fetched from: " + pullResult.getFetchedFrom());
			Collection<Ref> advertisedRefs = pullResult.getFetchResult().getAdvertisedRefs();
			for (Ref ref : advertisedRefs) {
				log.debug("Got advertisedRef: " + ref.toString());
			}

			String messages = pullResult.getFetchResult().getMessages();
			if (!messages.trim().isEmpty()) {
				log.info("Got PullResult messages: " + messages);
			}
		} catch (GitAPIException e) {
			throw new IllegalStateException("Unable to pull changes from remote", e);
		}
	}

	/**
	 * 
	 * @return true iff commits are alrady in sync
	 */
	private boolean checkNoUnpushedCommits() {
		try {

			String localHead = determineLocalHead();
			String remoteHead = fetchRemoteHead();

			if (localHead.equals(remoteHead)) {
				log.debug("Local HEAD and remote HEAD are equal, commits are in sync");
				return true;
			}

			log.info("Local and remote HEAD are not the same, checking for unpushed commits");
			// if remote head is already part of local refs, then we have unpushed commits
			if (logHasCommit(remoteHead)) {
				throw new IllegalStateException(
						"Unpushed local commits detected, remote HEAD part of local commits but heads are not equal");
			}

			log.info("Remote is ahead of local, need to pull");
			return false;

		} catch (GitAPIException | IOException e) {
			throw new IllegalStateException("Unable to check for unpushed commits", e);
		}

	}

	private boolean logHasCommit(String commitId) throws InvalidRemoteException, TransportException, GitAPIException {
		Iterable<RevCommit> logResults = git.log().call();
		for (RevCommit revCommit : logResults) {
			if (commitId.equals(revCommit.getName())) {
				log.info("Found commitId in local repository: " + commitId);
				return true;
			}
		}
		log.info("Did not find commitId in local repository: " + commitId);
		return false;
	}

	private String determineLocalHead() throws IOException {
		Ref localHeadRef = git.getRepository().getRefDatabase().getRef("HEAD");
		log.debug("Local HEAD: " + localHeadRef);
		if (localHeadRef == null) {
			throw new IllegalStateException("Dit not find local HEAD rev");
		}
		return localHeadRef.getObjectId().getName();

	}

	private String fetchRemoteHead() throws InvalidRemoteException, TransportException, GitAPIException {
		Collection<Ref> remoteRefs = fetchRemoteRefs(true);
		for (Ref ref : remoteRefs) {
			log.debug("Got remote head ref: " + ref.toString());
			if (ref.getName().equals("refs/heads/" + Config.getInstance().GIT_BRANCH)) {
				return ref.getObjectId().getName();
			}
		}

		throw new IllegalStateException("Unable to determine remote head");
	}

	private Collection<Ref> fetchRemoteRefs(boolean fetchHeadRefs)
			throws GitAPIException, InvalidRemoteException, TransportException {
		log.debug("Fetching remote refs, heads: " + fetchHeadRefs);
		LsRemoteCommand lsRemote = git.lsRemote().setHeads(fetchHeadRefs);
		configureAuthentication(lsRemote);
		Collection<Ref> remoteRefs = lsRemote.call();
		return remoteRefs;
	}

	private void checkStatusIsUnmodified() throws GitAPIException {

		log.debug("Making sure local git repository does not have any kind of local modifications");

		Status status = git.status().call();

		// check for any kind of local modifications to the working directory
		checkStatusIsUnmodified("Uncomitted", status.getUncommittedChanges());
		checkStatusIsUnmodified("Added", status.getAdded());
		checkStatusIsUnmodified("Changed", status.getChanged());
		checkStatusIsUnmodified("Conflicting", status.getConflicting());
		checkStatusIsUnmodified("Missing", status.getMissing());
		checkStatusIsUnmodified("Modified", status.getModified());
		checkStatusIsUnmodified("Removed", status.getRemoved());
		checkStatusIsUnmodified("Untracked", status.getUntracked());
		checkStatusIsUnmodified("UntrackedFolder", status.getUntrackedFolders());

		// yaya I just found this convenience method later
		if (!status.isClean()) {
			throw new IllegalStateException("Local repository is not clean");
		}
	}

	private void checkStatusIsUnmodified(String statusType, Set<String> statusFiles) {
		if (!statusFiles.isEmpty()) {
			log.error("Found local changes in git repository of type: " + statusType);
			for (String modified : statusFiles) {
				log.error(statusType + ": " + modified);
			}
			throw new IllegalStateException("Local repository contains modifications of type: " + statusType);
		}
	}

	private void cloneRepositoryToDirectory(File gitDirectory) {
		CloneCommand cloneCommand = Git.cloneRepository();
		cloneCommand.setURI(Config.getInstance().GIT_REPOSITORY_URI);
		cloneCommand.setBranch(Config.getInstance().GIT_BRANCH);
		cloneCommand.setRemote(Config.getInstance().GIT_REMOTE_NAME);
		cloneCommand.setDirectory(gitDirectory);
		configureAuthentication(cloneCommand);
		try {
			git = cloneCommand.call();
		} catch (GitAPIException e) {
			throw new IllegalStateException("Unable to clone git repo", e);
		}
	}

	private void close() {
		if (git != null) {
			try {
				if (!git.status().call().isClean()) {
					log.warn("About to close git repository but there are still local modifications");
				}
			} catch (NoWorkTreeException | GitAPIException e) {
				log.warn("Unable to check git status", e);
			}
			git.close();
			git = null;
		}
	}

	private void initShutdownHook() {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				close();
			}
		});
	}

	public void addToRepository(String filePath) {
		File fileToAdd = new File(filePath);
		if (!fileToAdd.exists()) {
			throw new IllegalArgumentException("Unable to add non existing path to repository: " + filePath);
		}
		String newlyAddedPath = localGitRepositoryDirectory.getAbsolutePath() + File.separatorChar
				+ fileToAdd.getName();
		log.debug("Adding " + filePath + " to git repository: " + newlyAddedPath);
		if (fileToAdd.isDirectory()) {
			try {
				File gitDirectorySubfolder = new File(newlyAddedPath);
				FileUtils.copyDirectory(fileToAdd, gitDirectorySubfolder);
			} catch (IOException e) {
				throw new IllegalStateException("Unable to copy directory to git repository: " + filePath, e);
			}
		} else if (fileToAdd.isFile()) {
			try {
				FileUtils.copyFileToDirectory(fileToAdd, localGitRepositoryDirectory);
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

	public void commitAndPush(String commitMessagePrefix) {
		String commitMessage = commitMessagePrefix + " DrillViewGenerator commit";
		log.debug("Pushing changes to git: " + commitMessage);
		try {
			// check if there even is anything to commit
			if (git.status().call().isClean()) {
				log.info("No local modifications to commit");
				return;
			}
			log.debug("Commit local changes in git repository");
			git.commit().setMessage(commitMessage).setAuthor(Config.getInstance().GIT_AUTHOR, gitEmailAddress()).call();
			log.info("Pushing to git remote branch " + Config.getInstance().GIT_BRANCH + " at "
					+ Config.getInstance().GIT_REPOSITORY_URI);
			configureAuthentication(git.push()).call();
		} catch (GitAPIException e) {
			throw new IllegalStateException("Unable to commit and push changes to git", e);
		}
	}

	private String gitEmailAddress() {
		String emailUser;
		Optional<String> localUser = SystemUtil.getCurrentUser();
		if (localUser.isPresent()) {
			emailUser = localUser.get();
		} else {
			emailUser = Config.getInstance().GIT_EMAIL_DEFAULT_USER;
		}
		String emailHost;
		Optional<String> localHostname = SystemUtil.getLocalHostname();
		if (localHostname.isPresent()) {
			emailHost = localHostname.get();
		} else {
			emailHost = Config.getInstance().GIT_EMAIL_DEFAULT_HOST;
		}
		return emailUser + "@" + emailHost;
	}

	private TransportCommand<?, ?> configureAuthentication(TransportCommand<?, ?> transportCommand) {

		switch (authenticationMethodType) {
		case SSH: {
			// inspired by https://www.codeaffine.com/2014/12/09/jgit-authentication/
			transportCommand.setTransportConfigCallback(new TransportConfigCallback() {
				@Override
				public void configure(Transport transport) {
					// only needed when using ssh authentication, not for http
					if (transport instanceof SshTransport) {
						SshTransport sshTransport = (SshTransport) transport;
						sshTransport.setSshSessionFactory(sshSessionFactory);
					}
				}
			});
			break;
		}
		case HTTP: {
			transportCommand.setCredentialsProvider(credentialsProvider);
			break;
		}
		}

		// for convenience
		return transportCommand;
	}

	private void initSshSessionFactory() {
		sshSessionFactory = new JschConfigSessionFactory() {
			@Override
			protected void configure(Host host, Session session) {
				// do nothing
			}

			@Override
			protected JSch createDefaultJSch(FS fs) throws JSchException {
				// inspired by
				// https://stackoverflow.com/questions/13686643/using-keys-with-jgit-to-access-a-git-repository-securely
				JSch defaultJSch = super.createDefaultJSch(fs);
				defaultJSch.removeAllIdentity();
				defaultJSch.addIdentity(Config.getInstance().GIT_AUTHENTICATION_SSH_KEY_PATH);
				return defaultJSch;
			}

		};
	}

	public Optional<String> loadFile(String subPath) {

		log.debug("Loading file from local repository: " + subPath);

		File fileToLoad = new File(Config.getInstance().GIT_LOCAL_REPOSITORY_PATH + File.separator + subPath);
		if (!fileToLoad.exists()) {
			log.debug(
					"Unable to load file from git repository as file doesn't exist at " + fileToLoad.getAbsolutePath());
			return Optional.absent();
		}

		if (!fileToLoad.canRead()) {
			log.debug(
					"Unable to load file from git repository as file can't be read at " + fileToLoad.getAbsolutePath());
			return Optional.absent();
		}

		try {
			return Optional.of(FileUtils.readFileToString(fileToLoad));
		} catch (IOException e) {
			log.warn("Unable to read file from local git repository even though the file exists at: "
					+ fileToLoad.getAbsolutePath(), e);
			return Optional.absent();
		}
	}

	public List<String> listFiles(String directoryPath) {

		List<String> r = new ArrayList<>();

		File directory = new File(Config.getInstance().GIT_LOCAL_REPOSITORY_PATH + File.separator + directoryPath);
		log.debug("Listing files in local git repository at: " + directory.getAbsolutePath());
		if (!directory.exists() || !directory.isDirectory()) {
			log.error("Was supposed to list files from non-existing directory: " + directoryPath);
			return r;
		}

		Collections.addAll(r, directory.list());
		return r;
	}

}
