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
    java
    application
    kotlin("jvm")
}

dependencies {
    api(project(":parsing:dialects:bigquery"))
    api(project(":parsing:dialects:defaultdialect"))
    api(project(":parsing:dialects:dialect1"))
    api(project(":parsing:dialects:hive"))
    api(project(":parsing:dialects:mysql"))
    api(project(":parsing:dialects:postgresqlBase:dialects:postgresql"))
    api(project(":parsing:dialects:postgresqlBase:dialects:redshift"))

    implementation("net.sf.opencsv:opencsv")
    implementation("com.google.code.gson:gson:2.8.5")
}

tasks.register("findErrors", JavaExec::class) {
    main = "com.apache.calcite.parsing.tools.FindErrorsExec"
    classpath = sourceSets["main"].runtimeClasspath
    val inputPath = findProperty("inputPath") as String
    val outputPath = findProperty("outputPath") as String
    val dialect = findProperty("dialect") as String
    var groupByErrors = "false"
    if (findProperty("groupByErrors") != null) {
        groupByErrors = "true"
    }
    val numSampleQueries = findProperty("numSampleQueries")
    val commandLineArgs = mutableListOf(inputPath, outputPath, dialect, groupByErrors)
    if (numSampleQueries != null) {
        commandLineArgs.add(numSampleQueries as String)
    }
    args = commandLineArgs
}
