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
        functionMap.forEach {
            k, v ->
                println("$k = $v")
            }
    }

    /**
     * Traverses the parsing directory structure and extracts all of the
     * functions located in *.ftl files into a Map
     *
     */
    private fun extractFunctions(): Map<String, String> {
        val rootDirectoryFile = rootDirectory.get().asFile
        val queue = getTraversalPath(rootDirectoryFile)
        val functionMap: MutableMap<String, String> = HashMap<String, String>()
        traverse(queue, rootDirectoryFile, functionMap)
        return functionMap
    }

    /**
     * Generates the parserImpls.ftl file for the dialect.
     *
     * @param functions The functions to place into the output file
     */
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
        return LinkedList(dialectPath.split("/"))
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
        // Ensures that files are processed first.
        files.sortBy { it.isDirectory }
        val nextDirectory = directories.peek()
        for (f in files) {
            if (f.isFile && f.extension == "ftl") {
                processFile(f, functionMap)
            }
            if (directories.isNotEmpty() && f.name == nextDirectory) {
                directories.poll()
                traverse(directories, f, functionMap)
            }
        }
    }

    /**
     * Extracts the functions from the given file into functionMap. Parses
     * functions of the form:
     * <return_type> <name>(<args>) :
     * {
     *     <properties>
     * }
     * {
     *     <body>
     * }
     * @param file The file to process
     * @param functionMap The map to which the parsing functions will be added to
     */
    private fun processFile(file: File, functionMap: MutableMap<String, String>) {
        var fileText = file.readText(Charsets.UTF_8)
        val typeAndName = "\\w+\\s+\\w+"
        val declarationPattern =
            Regex("(%s\\s*\\(\\s*(%s\\s*(\\,\\s*%s\\s*)*)?\\)\\s*\\:\n)"
                    .format(typeAndName, typeAndName, typeAndName))
        val delims = "(\\s|\n|\"|(//)|(/\\*)|(\\*/))"
        // Uses Lookahead and Lookbehind to ensure that the delims are added as tokens
        val splitRegex = Regex("((?<=%s)|(?=%s))".format(delims, delims))
        val declarationMatches = declarationPattern.findAll(fileText)
        for (m in declarationMatches) {
            val functionDeclaration = m.value
            val functionName = getFunctionName(functionDeclaration)
            val functionBuilder = StringBuilder(functionDeclaration)
            val declarationIndex = fileText.indexOf(functionDeclaration)
            fileText = fileText.substring(declarationIndex + functionDeclaration.length)
            val tokens: Queue<String> = LinkedList(splitRegex.split(fileText))
            processCurlyBlock(functionBuilder, tokens)
            processCurlyBlock(functionBuilder, tokens)
            functionMap.put(functionName, functionBuilder.toString())
        }
    }

    /**
     * Parses a block of text surrounded by curly braces. The function keeps track
     * of the number of curly braces that have yet to be closed. Once this counter
     * reaches 0 the block has been fully parsed. The function also ensures that
     * curly braces within single-line comments, multi-line comments, and strings
     * are not counted for this.
     *
     * @param functionBuilder The builder unto which the tokens get added to once parsed
     * @param tokens The tokens to be parsed
     */
    private fun processCurlyBlock(functionBuilder: StringBuilder, tokens: Queue<String>) {
        var curlyCounter = 0
        var insideString = false
        var insideSingleLineComment = false
        var insideMultiLineComment = false
        var validBrace = true
        while (tokens.isNotEmpty()) {
            val token = tokens.poll()
            functionBuilder.append(token)
            validBrace = !insideString &&
                    !insideSingleLineComment &&
                    !insideMultiLineComment
            if (token == "\n") {
                if (insideSingleLineComment) {
                    insideSingleLineComment = false
                }
                // Allows for an abitrary amount of new lines to be parsed.
                continue
            }
            if (token == "\"" && !insideSingleLineComment && !insideMultiLineComment) {
                insideString = !insideString
            } else if (token == "//" && !insideMultiLineComment) {
                insideSingleLineComment = true
            } else if (token == "/*" && !insideString && !insideSingleLineComment) {
                insideMultiLineComment = true
            } else if (token == "*/" && !insideString && !insideSingleLineComment) {
                insideMultiLineComment = false
            } else if (token == "{" && validBrace) {
                curlyCounter++
            } else if (token == "}" && validBrace) {
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
     * @param functionDeclaration The function declaration of form
     *                            <return_type> <name> (<args>) :
     */
    private fun getFunctionName(functionDeclaration: String): String {
        val nameRegex = Regex("\\w+")
        val matches = nameRegex.findAll(functionDeclaration)
        return matches.elementAt(1).value
    }
}
