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
plugins {
    kotlin("jvm")
    id("com.github.vlsi.ide")
    calcite.fmpp
    calcite.javacc
}

dependencies {
    api(project(":core"))

    implementation("com.google.guava:guava")
    implementation("org.apache.calcite.avatica:avatica-core")
    implementation("org.slf4j:slf4j-api")

    testImplementation("net.hydromatic:quidem")
    testImplementation("net.hydromatic:scott-data-hsqldb")
    testImplementation("org.hsqldb:hsqldb")
    testImplementation("org.incava:java-diff")
    testImplementation("org.slf4j:slf4j-log4j12")
    testImplementation(project(":core", "testClasses"))
    testImplementation(project(":parsing", "testClasses"))
    testImplementation(project(":parsing:intermediate:dialects:postgresqlBase", "testClasses"))
}

val dialectGenerate by tasks.registering(org.apache.calcite.buildtools.parser.DialectGenerateTask::class) {
    rootDirectory.set(file("$rootDir/parsing"))
    dialectDirectory.set(file("."))
    outputFile = "build/generated/templates/parserImpls.ftl"
}

val fmppMain by tasks.registering(org.apache.calcite.buildtools.fmpp.FmppTask::class) {
    dependsOn(dialectGenerate)

    inputs.dir(".")
    config.set(file("config.fmpp"))
    templates.set(file("$rootDir/parsing/src/main/resources"))
}

val javaCCMain by tasks.registering(org.apache.calcite.buildtools.javacc.JavaCCTask::class) {
    dependsOn(fmppMain)
    lookAhead.set(2)
    val parserFile = fmppMain.map {
        it.output.asFileTree.matching { include("**/Parser.jj") }.singleFile
    }
    inputFile.set(parserFile)
    packageName.set("org.apache.calcite.sql.parser.postgresql")
}

ide {
    fun generatedSource(
        javacc: TaskProvider<org.apache.calcite.buildtools.javacc.JavaCCTask>,
        sourceSet: String
    ) = generatedJavaSources(javacc.get(), javacc.get().output.get().asFile,
            sourceSets.named(sourceSet))

    generatedSource(javaCCMain, "main")
}
