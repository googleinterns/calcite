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
import java.util.StringTokenizer
import javax.inject.Inject
import kotlin.collections.HashMap
import kotlin.collections.MutableMap
import kotlin.text.Regex
import kotlin.text.StringBuilder
import org.gradle.api.DefaultTask
import org.gradle.api.model.ObjectFactory
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputDirectory
import org.gradle.api.tasks.TaskAction

open class DialectGenerateTask @Inject constructor(
    objectFactory: ObjectFactory
) : DefaultTask() {

    @InputDirectory
    val dialectDirectory = objectFactory.directoryProperty()

    @InputDirectory
    val rootDirectory = objectFactory.directoryProperty()

    @Input
    var outputFile = ""

    @TaskAction
    fun run() {
        val functionMap = extractFunctions()
        println(functionMap)
    }

    private fun extractFunctions(): Map<String, String> {
        val rootDirectoryFile = rootDirectory.get().asFile
        val queue = getTraversalPath(rootDirectoryFile)
        val functionMap: MutableMap<String, String> = HashMap<String, String>()
        traverse(queue, rootDirectoryFile, functionMap)
        return functionMap
    }

    private fun generateParserImpls(functions: Map<String, String>) {}

    /**
     * Gets the traversal path for the dialect by "subtracting" the root
     * absolute path from the dialect directory absolute path.
     *
     * @param rootDirectoryFile The file for the root parsing directory
     */
    private fun getTraversalPath(rootDirectoryFile: File): Queue<String> {
        val dialectDirectoryFile = dialectDirectory.get().asFile
        var dialectPath = dialectDirectoryFile.absolutePath
        val rootPath = rootDirectoryFile.absolutePath
        val rootIndex = dialectPath.indexOf(rootPath)
        dialectPath = dialectPath.substring(rootIndex + rootPath.length + 1)

        val queue: Queue<String> = LinkedList(dialectPath.split("/"))
        return queue
    }

    /**
     * Traverses the determined path given by the queue. Once the queue is
     * empty, the dialect directory has been reached. In that case any *.ftl
     * file should be processed and no further traversal should happen.
     *
     * @param directories The directories to traverse in topdown order
     * @param currentDirectory The current directory the function is processing
     * @param functionMap The map to which the parsing functions will be added to
     */
    private fun traverse(
        directories: Queue<String>,
        currentDirectory: File,
        functionMap: MutableMap<String, String>
    ) {
        val files = currentDirectory.listFiles()
        files.sortBy { it.isDirectory }
        val nextDirectory = directories.peek()
        for (f in files) {
            if (f.isFile && f.extension == "ftl") {
                processFile(f, functionMap)
            }
            if (directories.isNotEmpty() && f.name == nextDirectory) {
                println(f.name.toString())
                directories.poll()
                traverse(directories, f, functionMap)
            }
        }
    }

    private fun processFile(f: File, functionMap: MutableMap<String, String>) {
        println("Found File: " + f.absolutePath.toString())
        var fileText = f.readText(Charsets.UTF_8)
        val declarationPattern =
            Regex("(\\w+\\s+\\w+\\s*\\(\\w+\\s+\\w+\\s*(\\,\\s*\\w+\\s+\\w+\\s*)*\\)\\s*\\:\n)")
        val matches = declarationPattern.findAll(fileText)
        for (m in matches) {
            val functionDeclaration = m.value
            val functionName = getFunctionName(functionDeclaration)
            val functionBuilder = StringBuilder(functionDeclaration)
            val declarationIndex = fileText.indexOf(functionDeclaration)
            fileText = fileText.substring(declarationIndex + functionDeclaration.length)
            val delims = " \n\""
            fileText = fileText.substring(fileText.indexOf("{"))
            val tokenizer = StringTokenizer(fileText, delims, /*returnDelims=*/ true)
            processCurlyBlock(functionBuilder, tokenizer)
            processCurlyBlock(functionBuilder, tokenizer)
            functionMap.put(functionName, functionBuilder.toString())
        }
    }

    private fun processCurlyBlock(functionBuilder: StringBuilder, tokenizer: StringTokenizer) {
        var curlyCounter = 0
        var insideString = false
        while (tokenizer.hasMoreTokens()) {
            val token = tokenizer.nextToken()
            functionBuilder.append(token)
            if (token == "\n") {
                continue
            }
            if (token == "\"") {
                insideString = !insideString
            } else if (token == "{" && !insideString) {
                curlyCounter++
            } else if (token == "}" && !insideString) {
                curlyCounter--
            }
            if (curlyCounter == 0) {
                return
            }
        }
    }

    /**
     * Gets the function name from the declaration.
     *
     * @param functionDeclaration The function declaration of form <return_type> <name> (<args>) :
     */
    private fun getFunctionName(functionDeclaration: String): String {
        val nameRegex = Regex("\\w+")
        val matches = nameRegex.findAll(functionDeclaration)
        return matches.elementAt(1).value
    }
}
