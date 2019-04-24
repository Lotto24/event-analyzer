@Library("jenkins-library-boomerang") _
@Library("cara-tests") __

def projectName = "event-analyzer"
def changes = new pipeline.common.Scm(this)
def latest_commit_user

pipeline {
  agent { label 'fast' }

  environment {
    git_credentials = "a15c3954-18de-4c02-991b-97e23b975936"
  }

  stages {
    stage('Prepare') {
      steps {
        script {
          is_pr = checkIfIsPr(env)
          stopRedudantRelease(is_pr == 'true', env)
          dockerTag = getDockerTag(env)
          setConcurrentBuilds(env)

          echo "SCM changes: ${changes.changelist(currentBuild)}"
          echo "Committers: ${changes.committers(currentBuild)}"
        }
      }
    }

    stage('Create uber-jar') {
      steps {
        echo "Running on node $env.NODE_NAME in $env.WORKSPACE"
        withEnv(["JAVA_HOME=/etc/alternatives/java_sdk_1.8.0"]){
            sh """
               ./gradlew clean shadowJar
               """
        }
      }
    }

    stage('Create new release') {
      when { branch 'master' }
      steps {

        sshagent([env.git_credentials]) {
          script {
            latest_commit_user = sh(returnStdout: true, script: 'git show -s --pretty=%an').trim()
            echo "Running on node $env.NODE_NAME in $env.WORKSPACE"
            echo "Creating Release with changes made by ${latest_commit_user}"
            if (latest_commit_user != 'jenkins') {

              echo "Creating new release"
              withEnv(["JAVA_HOME=/etc/alternatives/java_sdk_1.8.0"]){
                  sh """
                    git remote set-url origin git@srv-git-01-hh1.alinghi.tipp24.net:data-engineering/event-analyzer.git
                    git fetch origin
                    git checkout master
                    git reset --hard origin/master
                    ./gradlew clean release -Prelease.useAutomaticVersion=true -Pgradle.release.useAutomaticVersion=true
                    """
              }
            }
          }
        }
      }
    }

    stage('Upload latest Snapshot') {
      when {
        allOf {
          not {
            branch 'master'
          }
          expression {
            is_pr == 'true'
          }
        }
      }
      steps {
        sshagent([env.git_credentials]) {
          script {
            latest_commit_user = sh(returnStdout: true, script: 'git show -s --pretty=%an').trim()
            echo "Running on node $env.NODE_NAME in $env.WORKSPACE"
            echo "Uploading Snapshot with changes made by ${latest_commit_user}"
            if (latest_commit_user != 'jenkins') {
              echo "Just uploading the new SNAPSHOT version"
              sh """
                git remote set-url origin git@srv-git-01-hh1.alinghi.tipp24.net:data-engineering/event-analyzer.git
                ./gradlew clean build uploadArchive
                """
            }
          }
        }
      }

    }
  }

  // The options directive is for configuration that applies to the whole job.
  options {
    // Discard old builds
    buildDiscarder(logRotator(numToKeepStr: '42', daysToKeepStr: '7'))

    ansiColor('xterm')

    timestamps()
  }


}