
pipeline {
    agent any

    stages {
        
        stage('Static code analysis') {

            steps {

		        script {
		          // requires SonarQube Scanner 2.8+
		          scannerHome = tool name: 'sonar_scanner', type: 'hudson.plugins.sonar.SonarRunnerInstallation';
		        }

	            withSonarQubeEnv('SonarQube') {
	              sh "${scannerHome}/bin/sonar-scanner \
	        		-Dsonar.projectKey=com.njkol:top-ten-genres \
	        		-Dsonar.projectName=top-ten-genres \
	        		-Dsonar.projectVersion=0.0.1-SNAPSHOT \
	        		-Dsonar.sources=/var/lib/jenkins/workspace/spark-jenkins-integration/src/main/scala \
	        		-Dsonar.tests=/var/lib/jenkins/workspace/spark-jenkins-integration/src/test/scala \
	        		-Dsonar.sourceEncoding=UTF-8 \
	        		-Dsonar.scala.version=2.11 \
	        		-Dsonar.scoverage.reportPath=/var/lib/jenkins/workspace/spark-jenkins-integration/target/scoverage.xml"
	            }
          	}
        }
      
        stage ('Compile') {

            steps {
                withMaven(maven : 'local_maven') {
                    sh 'mvn clean compile'
                }
            }
        }

        stage ('Test') {

            steps {
                withMaven(maven : 'local_maven') {
                    sh 'mvn test'
                }
            }
        }
        
        stage ('Package') {
            steps {
                withMaven(maven : 'local_maven') {
                    sh 'mvn package'
                }
            }
        }

        stage('Publish') {
            steps {
        		nexusPublisher nexusInstanceId: 'nexus2', 
        		nexusRepositoryId: 'releases', 
        		packages: [[$class: 'MavenPackage', 
        		mavenAssetList: [[classifier: '', extension: '', filePath: '/var/lib/jenkins/workspace/spark-jenkins-integration/target/top-ten-genres-0.0.1-SNAPSHOT.jar']],
        		mavenCoordinate: [artifactId: 'top-ten-genres', groupId: 'com.njkol', packaging: 'jar', version: '0.0.1']]]
        	}
        }

    }
}
