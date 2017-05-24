node('docker') {

    try {

        stage('Checkout') {
            // clean git clone, but don't fail in case it doesn't exist yet
            sh(script: 'git clean -x -d -f', returnStatus: true)
            checkout scm
        }

        // start up build container
        def img = docker.image('hub.bccvl.org.au/bccvl/bccvlbase:2017-05-22')
        docker.withRegistry('https://hub.bccvl.org.au', 'hub.bccvl.org.au') {
            img.inside('-v /etc/machine-id:/etc/machine-id') {

                withVirtualenv() {

                    stage('Build') {
                        sh '. ${VIRTUALENV}/bin/activate; pip install -e .[http,scp,swift,metadata,exports]'

                    }

                    stage('Test') {
                        // Install movelib dependencies ... sholud have been done already, but extras don't seem to work recursively
                        sh '. ${VIRTUALENV}/bin/activate; pip install org.bccvl.movelib[http,scp,swift]'
                        // install test depenhencios
                        sh '. ${VIRTUALENV}/bin/activate; pip install -e .[test]'
                        // install test runner
                        sh '. ${VIRTUALENV}/bin/activate; pip install pytest pytest-cov'
                        // TODO: use --cov-report=xml -> coverage.xml
                        sh(script: '. ${VIRTUALENV}/bin/activate; pytest -v --junitxml=junit.xml --cov-report=xml --cov=org.bccvl.tasks src',
                           returnStatus: true)

                        // capture test result
                        step([
                            $class: 'XUnitBuilder',
                            thresholds: [
                                [$class: 'FailedThreshold', failureThreshold: '0',
                                                            unstableThreshold: '1']
                            ],
                            tools: [
                                [$class: 'JUnitType', deleteOutputFiles: true,
                                                      failIfNotNew: true,
                                                      pattern: 'junit.xml',
                                                      stopProcessingIfError: true]
                            ]
                        ])
                        // publish html coverage report
                        step([$class: 'CoberturaPublisher',
                              coberturaReportFile: 'coverage.xml']
                        )
                    }

                    stage('Package') {

                        if (publishPackage(currentBuild.result, env.BRANCH_NAME)) {

                            sh 'rm -fr build dist'
                            sh '. ${VIRTUALENV}/bin/activate; python setup.py register -r devpi sdist bdist_wheel upload -r devpi'

                        }

                    }

                    stage ('Push Artifact') {

                        // uninstall editable package
                        sh '. ${VIRTUALENV}/bin/activate; pip uninstall -y org.bccvl.tasks'
                        sh '. ${VIRTUALENV}/bin/activate; pip freeze > requirements.txt'
                        archiveArtifacts artifacts: 'requirements.txt', fingerprint: true, onlyIfSuccessful: true

                    }
                }
            }
        }

    } catch(err) {
        throw err
    } finally {

        // clean git clone (removes all build files from local clone)
        sh 'git clean -x -d -f'

        step([
            $class: 'Mailer',
            notifyEveryUnstableBuild: true,
            recipients: 'gerhard.weis@gmail.com ' + emailextrecipients([
                [$class: 'CulpritsRecipientProvider'],
                [$class: 'RequesterRecipientProvider']
            ])
        ])
    }
}
