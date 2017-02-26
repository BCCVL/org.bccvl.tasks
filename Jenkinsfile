node('docker') {

    try {

        stage('Checkout') {
            // clean git clone, but don't fail in case it doesn't exist yet
            sh(script: 'git clean -x -d -f', returnStdout: true)
            checkout scm
        }

        // start up build container
        def img = docker.image('hub.bccvl.org.au/bccvl/bccvlbase:2017-02-23')
        img.inside('-v /etc/machine-id:/etc/machine-id') {

            withVirtualenv() {

                stage('Build') {
                    // build wheel for install
                    sh '. ${VIRTUALENV}/bin/activate; python setup.py bdist_wheel'
                    sh '. ${VIRTUALENV}/bin/activate; pip install $(ls ./dist/*.whl)[http,scp,swift,metadata]'

                }

                stage('Test') {
                    // install test depenhencios
                    sh '. ${VIRTUALENV}/bin/activate; pip install .[test]'
                    // install test runner
                    sh '. ${VIRTUALENV}/bin/activate; pip install nose'
                    // TODO: use --cov-report=xml -> coverage.xml
                    sh(script: '. ${VIRTUALENV}/bin/activate; nosetests --verbosity=2 --with-xunit --xunit-file=junit.xml --with-coverage --cover-package=org.bccvl.tasks --cover-html --cover-branches',
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
                    publishHTML(target: [
                        allowMissing: false,
                        alwaysLinkToLastBuild: false,
                        keepAll: true,
                        reportDir: 'cover',
                        reportFiles: 'index.html',
                        reportName: 'Coverage Report'
                    ])

                }

                stage('Package') {

                    if (publishPackage(currentBuild.result, env.BRANCH_NAME)) {

                        sh 'rm -fr build dist'
                        sh '. ${VIRTUALENV}/bin/activate; python setup.py register -r devpi sdist bdist_wheel upload -r devpi'

                    }

                }

                stage ('Push Artifact') {

                    sh '. ${VIRTUALENV}/bin/activate; pip freeze > requirements.txt'
                    archiveArtifacts artifacts: 'requirements.txt', fingerprint: true, onlyIfSuccessful: true

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
