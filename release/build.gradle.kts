/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import com.github.vlsi.gradle.crlf.CrLfSpec
import com.github.vlsi.gradle.crlf.LineEndings
import com.github.vlsi.gradle.git.FindGitAttributes
import com.github.vlsi.gradle.git.dsl.gitignore
import com.github.vlsi.gradle.license.GatherLicenseTask
import com.github.vlsi.gradle.license.api.SpdxLicense
import com.github.vlsi.gradle.release.Apache2LicenseRenderer
import com.github.vlsi.gradle.release.ArtifactType
import com.github.vlsi.gradle.release.ReleaseExtension
import com.github.vlsi.gradle.release.ReleaseParams
import com.github.vlsi.gradle.release.dsl.dependencyLicenses
import com.github.vlsi.gradle.release.dsl.licensesCopySpec

plugins {
    id("com.github.vlsi.stage-vote-release")
}

rootProject.configure<ReleaseExtension> {
    voteText.set { it.voteTextGen() }
}

fun ReleaseParams.voteTextGen(): String = """
Hi all,

I have created a build for $componentName $version, release
candidate $rc.

Thanks to everyone who has contributed to this release.

You can read the release notes here:
$previewSiteUri/docs/history.html

The commit to be voted upon:
https://gitbox.apache.org/repos/asf?p=calcite.git;a=commit;h=$gitSha

Its hash is $gitSha

Tag:
$sourceCodeTagUrl

The artifacts to be voted on are located here:
$svnStagingUri
(revision $svnStagingRevision)

RAT report:
$previewSiteUri/rat/rat-report.txt

Site preview is here:
$previewSiteUri/

JavaDoc API preview is here:
$previewSiteUri/api

The hashes of the artifacts are as follows:
${artifacts.joinToString(System.lineSeparator()) { it.sha512 + System.lineSeparator() + "*" + it.name }}

A staged Maven repository is available for review at:
$nexusRepositoryUri/org/apache/$tlpUrl/

Release artifacts are signed with the following key:
https://people.apache.org/keys/committer/$committerId.asc
https://www.apache.org/dist/$tlpUrl/KEYS

N.B.
To create the jars and test $componentName: "./gradlew build".

If you do not have a Java environment available, you can run the tests
using docker. To do so, install docker and docker-compose, then run
"docker-compose run test" from the root of the directory.

Please vote on releasing this package as $componentName $version.

The vote is open for the next 72 hours and passes if a majority of at
least three +1 PMC votes are cast.

[ ] +1 Release this package as Apache Calcite $version
[ ]  0 I don't feel strongly about it, but I'm okay with the release
[ ] -1 Do not release this package because...


Here is my vote:

+1 (binding)
""".trimIndent()

val distributionGroup = "distribution"
val baseFolder = "apache-calcite-${rootProject.version}"

// This task scans the project for gitignore / gitattributes, and that is reused for building
// source/binary artifacts with the appropriate eol/executable file flags
val gitProps by tasks.registering(FindGitAttributes::class) {
    // Scanning for .gitignore and .gitattributes files in a task avoids doing that
    // when distribution build is not required (e.g. code is just compiled)
    root.set(rootDir)
}

val getLicenses by tasks.registering(GatherLicenseTask::class) {
    extraLicenseDir.set(file("$rootDir/src/main/config/licenses"))
    // Parts of the web site generated by Jekyll (http://jekyllrb.com/)
    addDependency(":jekyll:", SpdxLicense.MIT)
    addDependency("font-awesome:font-awesome-code:4.2.0", SpdxLicense.MIT)
    // git.io/normalize
    addDependency(":normalize:3.0.2", SpdxLicense.MIT)
    // Gridism: A simple, responsive, and handy CSS grid by @cobyism
    // https://github.com/cobyism/gridism
    addDependency(":gridsim:", SpdxLicense.MIT)
    addDependency("cobyism:html5shiv:3.7.2", SpdxLicense.MIT)
    addDependency(":respond:1.4.2", SpdxLicense.MIT)
}

val license by tasks.registering(Apache2LicenseRenderer::class) {
    group = LifecycleBasePlugin.BUILD_GROUP
    description = "Generates LICENSE file for the source distribution"
    artifactType.set(ArtifactType.SOURCE)
    metadata.from(getLicenses)
    failOnIncompatibleLicense.set(false)
}

val licenseFiles = licensesCopySpec(license)

fun CopySpec.excludeLicenseFromSourceRelease() {
    // Source release has "/licenses" folder with licenses for third-party dependencies
    // It is populated by "dependencyLicenses" above,
    // so we ignore the folder when building source releases
    exclude("licenses/**")
    exclude("LICENSE")
}

fun CopySpec.excludeCategoryBLicensedWorksFromSourceRelease() {
    // The source distribution contains "font-awesome:fonts" which is licensed as
    // http://fontawesome.io/license (Font: SIL OFL 1.1, CSS: MIT License).
    //
    // OFL 1.1 is "category B" (see LEGAL-112).
    //
    // According to
    // https://www.apache.org/legal/resolved.html#binary-only-inclusion-condition,
    // the source code can not include Category B licensed works.

    // We need to remove "web and desktop font files".
    exclude("site/fonts/**")
}

fun CrLfSpec.sourceLayout() = copySpec {
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    gitattributes(gitProps)
    into("$baseFolder-src") {
        // Note: license content is taken from "/build/..", so gitignore should not be used
        // Note: this is a "license + third-party licenses", not just Apache-2.0
        dependencyLicenses(licenseFiles)
        // Include all the source files
        from(rootDir) {
            gitignore(gitProps)
            excludeLicenseFromSourceRelease()
            excludeCategoryBLicensedWorksFromSourceRelease()
        }
    }
}

for (archive in listOf(Zip::class, Tar::class)) {
    val taskName = "dist${archive.simpleName}"
    val archiveTask = tasks.register(taskName, archive) {
        val eol = if (archive == Tar::class) LineEndings.LF else LineEndings.CRLF
        group = distributionGroup
        description = "Creates source distribution with $eol line endings for text files"
        if (this is Tar) {
            compression = Compression.GZIP
            archiveExtension.set("tar.gz")
        }
        // Gradle does not track "filters" as archive/copy task dependencies,
        // So a mere change of a file attribute won't trigger re-execution of a task
        // So we add a custom property to re-execute the task in case attributes change
        inputs.property("gitproperties", gitProps.map { it.props.attrs.toString() })

        // Gradle defaults to the following pattern:
        // [baseName]-[appendix]-[version]-[classifier].[extension]
        archiveBaseName.set("apache-calcite")
        archiveClassifier.set("src")

        CrLfSpec(eol).run {
            wa1191SetInputs(gitProps)
            with(sourceLayout())
        }
        doLast {
            logger.lifecycle("Source distribution is created: ${archiveFile.get().asFile}")
        }
    }
    releaseArtifacts {
        artifact(archiveTask)
    }
}
