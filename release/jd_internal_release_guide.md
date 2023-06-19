<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Overview

## For major Release:

Attention: Only committer has permission to create release branch in apache/hudi.
**Skip this step if it is a bug fix release. But do set the env variables.**

Release candidates are built from a release branch. As a final step in preparation for the release, you should create the release branch, push it to the Apache code repository, and update version information on the original branch.

Export Some Environment variables in the terminal where you are running the release scripts

- export RELEASE_VERSION=JD.<RELEASE_VERSION_TO_BE_PUBLISHED>
- export NEXT_VERSION=<NEW_VERSION_IN_MASTER>
- export RELEASE_BRANCH=jd-release-$RELEASE_VERSION
- export RC_NUM=<release_candidate_num_starting_from_1>

Use cut_release_branch.sh to cut a release branch

- Script: [cut_release_branch.sh](https://github.com/apache/incubator-hudi/blob/master/scripts/release/cut_release_branch.sh)
  
Usage

```shell
# Cut a release branch
Cd scripts && ./release/cut_release_branch.sh \
--release=${RELEASE_VERSION} \
--next_release=${NEXT_VERSION} \
--rc_num=${RC_NUM}
# Show help page
./hudi/scripts/release/cut_release_branch.sh -h
```

## For minor release:

Here is how to go about a bug fix release.

- Create a branch in your repo (<user>/hudi).
- Cherry-pick commits from master that needs to be part of this release. (git cherry-pick commit-hash). You need to manually resolve the conflicts. For eg, a file might have been moved to a diff class in master where as in your release branch, it could be in older place. You need to take a call where to place it. Similar things like file addition, file deletion, etc.
- Update the release version by running "mvn versions:set -DnewVersion=${RELEASE}-rc${RC_NUM}", with "RELEASE" as the version and "RC_NUM" as the RC number.  Make sure the version changes are intended.  Then git commit the changes.
- Ensure both compilation and tests are good.
- I assume you will have apache/hudi as upstream. If not add it as upstream
- Once the branch is ready with all commits, go ahead and push your branch to upstream.
- Go to apache/hudi repo locally and pull this branch. Here after you can work on this branch and push to origin when need be.
- Do not forget to set the env variables from above section.

## Verify that a Release Build Works

Run "mvn -Prelease clean install" to ensure that the build processes are in good shape. // You need to execute this command once you have the release branch in apache/hudi

Good to run this for all profiles

```shell
mvn -Prelease clean install
mvn -Prelease clean install -Dscala-2.12
```


# Build a release candidate

- export RELEASE_VERSION=jd-<RELEASE_VERSION_TO_BE_PUBLISHED>
- export RELEASE_BRANCH=jd-release-<RELEASE_VERSION_TO_BE_PUBLISHED>

1. git checkout ${RELEASE_BRANCH} 
2. Run mvn version to set the proper rc number in all artifacts 
   1. mvn versions:set -DnewVersion=${RELEASE_VERSION}
3. Commit and push this change to RELEASE branch
   1. git commit -am "Bumping release candidate number ${RELEASE_BRANCH}"
   2. git push hudi_jd ${RELEASE_BRANCH}
   3. make sure all the UT and IT are all passed
4. Create tag
   1. git tag -s release-${RELEASE_VERSION} -m "${RELEASE_VERSION}".
   2. if apache repo is origin.
      > git push jd_hudi tags/release-${RELEASE_VERSION}
5. Deploy maven artifacts and verify
   1. This will deploy jar artifacts to the JD Repository.
   2. git checkout ${RELEASE_BRANCH}
   3. ./scripts/release/deploy_staging_jars.sh 2>&1 | tee -a "/tmp/${RELEASE_VERSION}.deploy.log"