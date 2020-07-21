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
import java.io.FileReader
import au.com.bytecode.opencsv.CSVReader

plugins {
    kotlin("jvm")
}

repositories {
}

buildscript {
    repositories {
        mavenCentral()
    }
    dependencies {
        "classpath"(group = "net.sf.opencsv", name = "opencsv", version = "2.3")
    }
}
tasks.register<FindErrors>("findErrors") {

}

open class FindErrors : DefaultTask() {
    private lateinit var inputPath: String
    private lateinit var outputPath: String
    private lateinit var dialect: Dialect
    private var groupByErrors: Boolean = false
    private var numSampleQueries: String = "10"

    @Option(option = "inputPath", description = "Configures the input CSV file path.")
    fun setInputPath(inputPath: String) {
        this.inputPath = inputPath
    }

    @Option(option = "outputPath", description = "Configures the output CSV file path.")
    fun setOutputPath(outputPath: String) {
        this.outputPath = outputPath
    }

    @Option(option = "dialect", description = "Configures which dialectic parser to use.")
    fun setDialect(dialect: Dialect) {
        this.dialect = dialect
    }

    @Option(option = "groupByErrors", description = "Specifies if output format should group by errors.")
    fun setGroupByErrors(groupByErrors: Boolean) {
        this.groupByErrors = groupByErrors
    }

    @Option(option = "numSampleQueries", description = "Configures the number of sample queries to output in group by errors format.")
    fun setNumSampleQueries(numSampleQueries: String) {
        this.numSampleQueries = numSampleQueries
    }

    @TaskAction
    fun run() {
        val reader = CSVReader(FileReader("src/test/resources/csv/test_queries.csv"));
    }

    enum class Dialect {
        BIGQUERY, DEFAULTDIALECT, DIALECT1, HIVE, MYSQL, POSTGRESQL, REDSHIFT
    }
}
