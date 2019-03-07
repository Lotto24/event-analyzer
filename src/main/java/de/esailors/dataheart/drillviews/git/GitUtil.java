package de.esailors.dataheart.drillviews.git;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.api.CloneCommand;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.LsRemoteCommand;
import org.eclipse.jgit.api.PushCommand;
import org.eclipse.jgit.api.Status;
import org.eclipse.jgit.api.TransportCommand;
import org.eclipse.jgit.api.TransportConfigCallback;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.InvalidRemoteException;
import org.eclipse.jgit.api.errors.TransportException;
import org.eclipse.jgit.errors.NoWorkTreeException;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.ReflogEntry;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
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
	private static final String gitDirectoryPath = "out/git_repository";

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

		File gitDirectory = new File(gitDirectoryPath);
		log.info("Initializing git repository at " + gitDirectory.getAbsolutePath());
		if (gitDirectory.exists()) {
			// TODO if the repository already exists locally do not delete it and clone new
			// (unless it has local modifications) instead just pull update and reuse it

			log.info("Git directroy path already exists, trying to reuse repository");
			// check if directory already is a repository
			File existingDirectoryRepository = new File(gitDirectory.getAbsolutePath() + File.separator + ".git/");

			if (existingDirectoryRepository.exists() && existingDirectoryRepository.isDirectory()) {

				// see if there are local changes
				try {
					git = Git.open(existingDirectoryRepository);
					checkStatusIsUnmodified();
					checkNoUnpushedCommits();
					
				} catch (IOException | NoWorkTreeException | GitAPIException e) {
					throw new IllegalStateException(
							"Git directory is not empty and already contains a git repository but I can't open it: "
									+ existingDirectoryRepository.getAbsolutePath(),
							e);
				}
				
				// local repository is in sync with remote, we can proceed


				// fuck you
				// https://www.codeaffine.com/2014/09/22/access-git-repository-with-jgit/ for
				// writing the easy way so late
//				try {
//					Repository existingRepository = new FileRepositoryBuilder().setMustExist(true).setGitDir(existingDirectoryRepository).build();
//					log.warn("Reusing existing git repository at " + existingDirectoryRepository.getAbsolutePath());
//					if(existingRepository.getRefDatabase().getRef("HEAD") == null) {
//						throw new IllegalStateException("Unable to find HEAD revision of existing git directory");
//					}
//					// TODO check if there are local modifications, if yes fail otherwise pull latest changes
//				} catch (IOException e) {
//					throw new IllegalStateException("Git directory is not empty and already contains a git repository but I can't open it: " + existingDirectoryRepository.getAbsolutePath(), e);
//				}

			} else {
				throw new IllegalStateException(
						"Git directoy is not empty but also not a repository: " + gitDirectoryPath);
			}
		} else

		{
			log.info("Git directory path is empty, cloning repository to: " + gitDirectory.getAbsolutePath());
			cloneRepositoryToDirectory(gitDirectory);
		}

	}

	private void checkNoUnpushedCommits() {
		try {

			String localHead = determineLocalHead();
			String remoteHead = fetchRemoteHead();
			
			// TODO STOPPED HERE! HEADs could be different because remote is ahead, need to pull first and then check again
			
			if(!localHead.equals(remoteHead)) {
				throw new IllegalStateException("Local head different than remote head: local " + localHead + "  remote " + remoteHead);
			}
		} catch (GitAPIException | IOException e) {
			throw new IllegalStateException("Unable to check for unpushed commits", e);
		}
		
	}

	private String determineLocalHead() throws IOException {
		Ref localHeadRef = git.getRepository().getRefDatabase().getRef("HEAD");
		log.info("Local HEAD: " + localHeadRef);
		if(localHeadRef == null) {
			throw new IllegalStateException("Dit not find local HEAD rev");
		}
		return localHeadRef.getObjectId().getName();

	}

	private String fetchRemoteHead() throws InvalidRemoteException, TransportException, GitAPIException {
		log.info("Fetching remote HEAD");
		LsRemoteCommand lsRemote = git.lsRemote().setHeads(true);
		configureAuthentication(lsRemote);
		Collection<Ref> remoteRefs = lsRemote.call();
		for(Ref ref : remoteRefs) {
			log.info("Got remote ref: " + ref.toString());
			if(ref.getName().equals("refs/heads/" + gitBranch)) {
				return ref.getObjectId().getName();
			}
		}
		
		throw new IllegalStateException("Unable to determine remote head");
	}

	private void checkStatusIsUnmodified() throws GitAPIException {

		log.info("Making sure local repository does not have any kind of modifications");
		
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
		if(!status.isClean()) {
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
		cloneCommand.setURI(gitUri);
		cloneCommand.setBranch(gitBranch);
		cloneCommand.setRemote(gitRemote);
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
		// TODO write proper commit message, probably want to add a date
		String commitMessage = "first automated commit with ssh";
		log.info("Pushing changes to git: " + commitMessage);
		try {
			git.commit().setMessage(commitMessage).call();
			configureAuthentication(git.push()).call();
		} catch (GitAPIException e) {
			throw new IllegalStateException("Unable to commit and push changes to git", e);
		}
	}

	private TransportCommand<?, ?> configureAuthentication(TransportCommand<?, ?> transportCommand) {

		// inspired by https://www.codeaffine.com/2014/12/09/jgit-authentication/

		transportCommand.setTransportConfigCallback(new TransportConfigCallback() {
			@Override
			public void configure(Transport transport) {
				SshTransport sshTransport = (SshTransport) transport;
				sshTransport.setSshSessionFactory(sshSessionFactory);
			}
		});
		
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
				defaultJSch.addIdentity(gitSshKey);
				return defaultJSch;
			}

		};
	}

}
