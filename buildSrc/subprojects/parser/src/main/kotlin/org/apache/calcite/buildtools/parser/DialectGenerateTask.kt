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
package org.apache.calcite.buildtools.parser

import java.io.File
import java.util.LinkedList
import java.util.Queue
import javax.inject.Inject
import kotlin.collections.HashMap
import kotlin.collections.Map
import org.gradle.api.DefaultTask
import org.gradle.api.model.ObjectFactory
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputDirectory
import org.gradle.api.tasks.TaskAction

open class DialectGenerateTask @Inject constructor(
    objectFactory: ObjectFactory
) : DefaultTask() {

    private val CALCITE_OFFSET = 8

    @InputDirectory
    val dialectDirectory = objectFactory.directoryProperty()

    @InputDirectory
    val rootDirectory = objectFactory.directoryProperty()

    @Input
    var outputFile = ""

    @TaskAction
    fun run() {
        extractFunctions()
    }

    fun extractFunctions(): Map<String, String> {
        val rootDirectoryFile = rootDirectory.get().asFile
        val queue = getTraversalPath()
        traverse(queue, rootDirectoryFile)
        return HashMap()
    }

    fun generateParserImpls(functions: Map<String, String>) {}

    private fun getTraversalPath(): Queue<String> {
        val dialectDirectoryFile = dialectDirectory.get().asFile
        var dialectPath = dialectDirectoryFile.absolutePath
        val calciteIndex = dialectPath.lastIndexOf("calcite/")
        dialectPath = dialectPath.substring(calciteIndex + CALCITE_OFFSET)
        val queue: Queue<String> = LinkedList(dialectPath.split("/"))
        // Remove the root directory.
        queue.poll()
        return queue
    }

    /* Traverses the determined path given by the queue. Once the queue is
       empty, the dialect directory has been reached. In that case any *.ftl
       file should be processed and no further traversal should happen. */
    private fun traverse(directories: Queue<String>, currentDirectory: File) {
        val files = currentDirectory.listFiles()
        files.sortBy { it.isDirectory }
        val nextDirectory = directories.peek()
        for (f in files) {
            if (f.isFile && f.extension == "ftl") {
                processFile(f)
            }
            if (directories.isNotEmpty() && f.name == nextDirectory) {
                println(f.name.toString())
                directories.poll()
                traverse(directories, f)
            }
        }
    }

    private fun processFile(f: File) {
        println("Found File: " + f.absolutePath.toString())
    }
}
