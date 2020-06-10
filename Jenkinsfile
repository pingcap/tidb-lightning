def TIKV_BRANCH = "master"
def PD_BRANCH = "master"
def TIDB_BRANCH = "master"
def TIKV_IMPORTER_BRANCH = "master"
def BUILD_NUMBER = "${env.BUILD_NUMBER}"

if (params.containsKey("release_test")) {
    echo "this build is triggered by qa for release testing"
    ghprbActualCommit = params.getOrDefault("release_test__ghpr_actual_commit", params.release_test__lightning_commit)
    ghprbTargetBranch = params.getOrDefault("release_test__ghpr_target_branch", params.release_test__release_branch)
    ghprbCommentBody = params.getOrDefault("release_test__ghpr_comment_body", "")
    ghprbPullId = params.getOrDefault("release_test__ghpr_pull_id", "")
    ghprbPullTitle = params.getOrDefault("release_test__ghpr_pull_title", "")
    ghprbPullLink = params.getOrDefault("release_test__ghpr_pull_link", "")
    ghprbPullDescription = params.getOrDefault("release_test__ghpr_pull_description", "")
    TIDB_BRANCH = ghprbTargetBranch
    TIKV_BRANCH = ghprbTargetBranch
    PD_BRANCH = ghprbTargetBranch
    TIKV_IMPORTER_BRANCH = ghprbTargetBranch
}

def specStr = "+refs/heads/*:refs/remotes/origin/*"
if (ghprbPullId != null && ghprbPullId != "") {
    specStr = "+refs/pull/${ghprbPullId}/*:refs/remotes/origin/pr/${ghprbPullId}/*"
}

// parse tikv branch
def m1 = ghprbCommentBody =~ /tikv\s*=\s*([^\s\\]+)(\s|\\|$)/
if (m1) {
    TIKV_BRANCH = "${m1[0][1]}"
}
m1 = null
println "TIKV_BRANCH=${TIKV_BRANCH}"
// parse pd branch
def m2 = ghprbCommentBody =~ /pd\s*=\s*([^\s\\]+)(\s|\\|$)/
if (m2) {
    PD_BRANCH = "${m2[0][1]}"
}
m2 = null
println "PD_BRANCH=${PD_BRANCH}"
// parse tidb branch
def m3 = ghprbCommentBody =~ /tidb\s*=\s*([^\s\\]+)(\s|\\|$)/
if (m3) {
    TIDB_BRANCH = "${m3[0][1]}"
}
m3 = null
println "TIDB_BRANCH=${TIDB_BRANCH}"
// parse tikv-importer branch
def m4 = ghprbCommentBody =~ /tikv-importer\s*=\s*([^\s\\]+)(\s|\\|$)/
if (m4) {
    TIKV_IMPORTER_BRANCH = "${m4[0][1]}"
}
m4 = null
println "TIKV_IMPORTER_BRANCH=${TIKV_IMPORTER_BRANCH}"

def test_case_names = []

catchError {
    stage('Prepare') {
        node ("${GO_TEST_SLAVE}") {
            container("golang") {
                def ws = pwd()
                deleteDir()
                dir("/home/jenkins/agent/git/tidb-lightning") {
                    if (sh(returnStatus: true, script: '[ -d .git ] && [ -f Makefile ] && git rev-parse --git-dir > /dev/null 2>&1') != 0) {
                        deleteDir()
                    }
                    try {
                        checkout changelog: false, poll: false, scm: [$class: 'GitSCM', branches: [[name: 'master']], doGenerateSubmoduleConfigurations: false, extensions: [[$class: 'PruneStaleBranch'], [$class: 'CleanBeforeCheckout']], submoduleCfg: [], userRemoteConfigs: [[credentialsId: 'github-sre-bot-ssh', refspec: specStr, url: 'git@github.com:pingcap/tidb-lightning.git']]]
                    } catch (error) {
                        retry(2) {
                            echo "checkout failed, retry.."
                            sleep 60
                            if (sh(returnStatus: true, script: '[ -d .git ] && [ -f Makefile ] && git rev-parse --git-dir > /dev/null 2>&1') != 0) {
                                deleteDir()
                            }
                            checkout changelog: false, poll: false, scm: [$class: 'GitSCM', branches: [[name: 'master']], doGenerateSubmoduleConfigurations: false, extensions: [[$class: 'PruneStaleBranch'], [$class: 'CleanBeforeCheckout']], submoduleCfg: [], userRemoteConfigs: [[credentialsId: 'github-sre-bot-ssh', refspec: specStr, url: 'git@github.com:pingcap/tidb-lightning.git']]]
                        }
                    }
                }

                dir("go/src/github.com/pingcap/tidb-lightning") {
                    sh """
                        cp -R /home/jenkins/agent/git/tidb-lightning/. ./
                        git checkout -f ${ghprbActualCommit}
                    """

                    // Collect test case names.
                    def list = sh(script: "find tests/ -mindepth 2 -maxdepth 2 -name run.sh | cut -d / -f 2", returnStdout:true).trim()
                    test_case_names = list.split("\\n")
                }

                stash includes: "go/src/github.com/pingcap/tidb-lightning/**", name: "tidb-lightning", useDefaultExcludes: false

                // tikv
                def tikv_sha1 = sh(label: 'Fetch TiKV commit', returnStdout: true, script: "curl ${FILE_SERVER_URL}/download/refs/pingcap/tikv/${TIKV_BRANCH}/sha1").trim()
                sh "curl ${FILE_SERVER_URL}/download/builds/pingcap/tikv/${tikv_sha1}/centos7/tikv-server.tar.gz | tar xz"
                // pd
                def pd_sha1 = sh(label: 'Fetch PD commit', returnStdout: true, script: "curl ${FILE_SERVER_URL}/download/refs/pingcap/pd/${PD_BRANCH}/sha1").trim()
                sh "curl ${FILE_SERVER_URL}/download/builds/pingcap/pd/${pd_sha1}/centos7/pd-server.tar.gz | tar xz"
                // tidb
                def tidb_sha1 = sh(label: 'Fetch TiDB commit', returnStdout: true, script: "curl ${FILE_SERVER_URL}/download/refs/pingcap/tidb/${TIDB_BRANCH}/sha1").trim()
                sh "curl ${FILE_SERVER_URL}/download/builds/pingcap/tidb/${tidb_sha1}/centos7/tidb-server.tar.gz | tar xz"
                // tikv-importer
                def tikv_importer_sha1 = sh(label: 'Fetch TiKV-Importer commit', returnStdout: true, script: "curl ${FILE_SERVER_URL}/download/refs/pingcap/importer/${TIKV_IMPORTER_BRANCH}/sha1").trim()
                sh "curl ${FILE_SERVER_URL}/download/builds/pingcap/importer/${tikv_importer_sha1}/centos7/importer.tar.gz | tar xz"

                stash includes: "bin/**", name: "binaries"
            }
        }
    }

    stage('Unit test') {
        node("${GO_TEST_SLAVE}") {
            container("golang") {
                println "kubectl -n jenkins-ci exec -ti ${NODE_NAME} bash"
                def ws = pwd()
                deleteDir()
                unstash 'tidb-lightning'
                dir("go/src/github.com/pingcap/tidb-lightning") {
                    timeout(2) {
                        sh """
                            rm -rf /tmp/lightning_test_result
                            mkdir -p /tmp/lightning_test_result
                            GO111MODULE=on  PATH=\$GOPATH/go:${ws}/go/bin:\$PATH make test
                            rm -rf cov_dir
                            mkdir -p cov_dir
                            ls /tmp
                            ls /tmp/lightning_test_result
                            cp /tmp/lightning_test_result/cov*out cov_dir
                        """
                    }
                }

                stash includes: "go/src/github.com/pingcap/tidb-lightning/cov_dir/**", name: "unit-cov"
            }
        }
    }

    stage('Integration tests') {
        def test_cases = [:]

        test_case_names.each { test_name ->
            test_cases["integration test ${test_name}"] = {
                node("${GO_TEST_SLAVE}") {
                    container("golang") {
                        println "kubectl -n jenkins-ci exec -ti ${NODE_NAME} bash"

                        def ws = pwd()
                        deleteDir()
                        unstash 'tidb-lightning'
                        unstash 'binaries'

                        dir("go/src/github.com/pingcap/tidb-lightning") {
                            sh "mv ${ws}/bin ./bin/"
                            try {
                                timeout(10) {
                                    sh """
                                    rm -rf /tmp/lightning_test_result
                                    mkdir -p /tmp/lightning_test_result
                                    GO111MODULE=on PATH=\$GOPATH/bin:${ws}/go/bin:\$PATH TEST_NAME=${test_name} make integration_test
                                    rm -rf cov_dir
                                    mkdir -p cov_dir
                                    ls /tmp
                                    ls /tmp/lightning_test_result
                                    cp /tmp/lightning_test_result/cov*out cov_dir || true
                                    """
                                }
                            } catch (Exception e) {
                                sh "cat '/tmp/lightning_test_result/pd.log' || true"
                                sh "cat '/tmp/lightning_test_result/tikv.log' || true"
                                sh "cat '/tmp/lightning_test_result/tidb.log' || true"
                                sh "cat '/tmp/lightning_test_result/importer.log' || true"
                                sh "cat '/tmp/lightning_test_result/lightning.log' || true"
                                sh "echo 'build failed'"
                                throw e;
                            }
                        }

                        stash includes: "go/src/github.com/pingcap/tidb-lightning/cov_dir/**", name: "intergration-${test_name}-cov", useDefaultExcludes: false, allowEmpty: true
                    }
                }
            }
        }

        parallel test_cases
    }

    if (!params.containsKey("release_test")) {

    stage('Coverage') {
        node("${GO_TEST_SLAVE}") {
            def ws = pwd()
            deleteDir()
            unstash 'tidb-lightning'
            test_case_names.each { test_name ->
                unstash "intergration-${test_name}-cov"
            }
            unstash 'unit-cov'

            dir("go/src/github.com/pingcap/tidb-lightning") {
                container("golang") {
                    timeout(3) {
                        sh """
                        rm -rf /tmp/lightning_test_result
                        mkdir -p /tmp/lightning_test_result
                        cp cov_dir/* /tmp/lightning_test_result
                        set +x
                        export COVERALLS_TOKEN=${COVERALLS_TOKEN}
                        set -x
                        BUILD_NUMBER=${BUILD_NUMBER} PULL_REQUEST_NUMBER=${ghprbPullId} GO111MODULE=on PATH=\$GOPATH/bin:${ws}/go/bin:\$PATH JenkinsCI=1 make coverage
                        """
                    }
                }
            }

        }

    }

    } // endif (!params.containsKey("release_test"))

    currentBuild.result = "SUCCESS"
}

stage('Summary') {
    def duration = ((System.currentTimeMillis() - currentBuild.startTimeInMillis) / 1000 / 60).setScale(2, BigDecimal.ROUND_HALF_UP)
    def slackmsg = "[#${ghprbPullId}: ${ghprbPullTitle}]" + "\n" +
    "${ghprbPullLink}" + "\n" +
    "${ghprbPullDescription}" + "\n" +
    "Integration Common Test Result: `${currentBuild.result}`" + "\n" +
    "Elapsed Time: `${duration} mins` " + "\n" +
    "${env.RUN_DISPLAY_URL}"

    if (currentBuild.result != "SUCCESS") {
        slackSend channel: '#jenkins-ci', color: 'danger', teamDomain: 'pingcap', tokenCredentialId: 'slack-pingcap-token', message: "${slackmsg}"
    }
}
