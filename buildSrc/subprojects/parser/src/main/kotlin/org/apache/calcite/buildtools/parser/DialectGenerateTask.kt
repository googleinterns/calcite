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
import kotlin.text.MatchResult
import kotlin.text.Regex
import kotlin.text.StringBuilder
import org.gradle.api.DefaultTask
import org.gradle.api.model.ObjectFactory
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputDirectory
import org.gradle.api.tasks.TaskAction

/* Gradle task that traverses the parsing directory and generates the parserImpls.ftl
   file for the given dialect.
 */
open class DialectGenerateTask @Inject constructor(
    objectFactory: ObjectFactory
) : DefaultTask() {

    private val typeAndName = "\\w+\\s+\\w+"
    private val space = "\\s"
    private val newLine = "\n"
    private val splitDelims = "(\\s|\n|\"|//|/\\*|\\*/|')"

    private val tokenizer = Regex("((?<=$splitDelims)|(?=$splitDelims))")
    private val declarationPattern =
        Regex("($typeAndName\\s*\\(\\s*($typeAndName\\s*(\\,\\s*$typeAndName\\s*)*)?\\)\\s*\\:\n?)")
    private val nameRegex = Regex("\\w+")

    // Flags used when parsing.
    private var validBrace = false
    private var validDoubleQuote = false
    private var validSingleQuote = false
    private var validSingleComment = false
    private var validMultiComment = false

    @InputDirectory
    val dialectDirectory = objectFactory.directoryProperty()

    @InputDirectory
    val rootDirectory = objectFactory.directoryProperty()

    @Input
    var outputFile = ""

    @TaskAction
    fun run() {
        val functionMap = extractFunctions()
    }

    /**
     * Traverses the parsing directory structure and extracts all of the
     * functions located in *.ftl files into a Map.
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
            } else if (directories.isNotEmpty() && f.name == nextDirectory) {
                // Remove the front element in the queue, the value is used above with
                // directories.peek().
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
        val declarations: Queue<MatchResult> = LinkedList(declarationPattern.findAll(fileText).toList())
        val tokens: Queue<String> = LinkedList(tokenizer.split(fileText))
        var charIndex = 0
        while (declarations.isNotEmpty()) {
            val declaration = declarations.poll()
            while (charIndex < declaration.range.start) {
                charIndex += tokens.poll().length
            }
            charIndex = processFunction(tokens, functionMap, charIndex,
                declaration.range.endInclusive, getFunctionName(declaration.value))
        }
    }

    /**
     * Parses a function of the form:
     *
     * <return_type> <name>(<args>) :
     * {
     *     <properties>
     * }
     * {
     *     <body>
     * }
     *
     * @param tokens The tokens starting from the function declaration and
     *               ending at EOF
     * @param functionMap The map to which the extracted functions will be added to
     * @param charIndex The character index of the entire text of the file at which
     *                  the parsing is commencing at
     * @param declarationEnd The char index (of entire file text) at which the l
     *                       function declaration ends
     * @param functionName The name of the function being processed
     */
    private fun processFunction(
        tokens: Queue<String>,
        functionMap: MutableMap<String, String>,
        charIndex: Int,
        declarationEnd: Int,
        functionName: String
    ): Int {
        var updatedCharIndex = charIndex
        val functionBuilder = StringBuilder()
        // Process the declaration:
        while (updatedCharIndex < declarationEnd && tokens.isNotEmpty()) {
            val token = tokens.poll()
            functionBuilder.append(token)
            updatedCharIndex += token.length
        }
        // Called twice as there are two curly blocks.
        updatedCharIndex = processCurlyBlock(functionBuilder, tokens, updatedCharIndex)
        updatedCharIndex = processCurlyBlock(functionBuilder, tokens, updatedCharIndex)

        functionMap.put(functionName, functionBuilder.toString())
        return updatedCharIndex
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
     * @param charIndex The character index of the entire text of the file at which
     *                  the parsing is commencing at
     */
    private fun processCurlyBlock(
        functionBuilder: StringBuilder,
        tokens: Queue<String>,
        charIndex: Int
    ): Int {
        var curlyCounter = 0
        var insideString = false
        var insideCharacter = false
        var insideSingleComment = false
        var insideMultiComment = false
        var updatedCharIndex = charIndex
        while (tokens.isNotEmpty()) {
            val token = tokens.poll()
            functionBuilder.append(token)
            updatedCharIndex += token.length
            if (token == "\n" || token == " ") {
                if (token == "\n") {
                    if (insideSingleComment) {
                        insideSingleComment = false
                    }
                }
                // Since new lines and spaces between curly blocks are valid in calcite,
                // we want to allow an arbitrary number of them before we start
                // keeping track of curly braces.
                continue
            }
            // The following checks ensure that curly braces inside of strings
            // and comments do not affect curlyCounter.
            determineTokenValidity(insideString, insideCharacter,
                    insideSingleComment, insideMultiComment)
            if (token == "\"" && validDoubleQuote) {
                insideString = !insideString
            } else if (token == "'" && validSingleQuote) {
                insideCharacter = !insideCharacter
            } else if (token == "//" && validSingleComment) {
                insideSingleComment = true
            } else if (token == "/*" && validMultiComment) {
                insideMultiComment = true
            } else if (token == "*/" && validMultiComment) {
                insideMultiComment = false
            } else if (token == "{" && validBrace) {
                curlyCounter++
            } else if (token == "}" && validBrace) {
                curlyCounter--
            }
            if (curlyCounter == 0) {
                return updatedCharIndex
            }
        }
        return updatedCharIndex
    }

    /**
     * Gets the function name from the declaration.
     *
     * @param functionDeclaration The function declaration of form
     *                            <return_type> <name> (<args>) :
     */
    private fun getFunctionName(functionDeclaration: String): String {
        val matches = nameRegex.findAll(functionDeclaration)
        return matches.elementAt(1).value
    }

    /**
     * Sets the various flags which are used by processCurlyBloc() when
     * parsing special characters.
     *
     * @param insideString Whether currently within a string " "
     * @param insideCharacter Whether currently within a character ' '
     * @param insideSingleLineComment Whether curently within single comment //
     * @param insideMultiLineComment Whether currently within multi comment
     */
    private fun determineTokenValidity(
        insideString: Boolean,
        insideCharacter: Boolean,
        insideSingleLineComment: Boolean,
        insideMultiLineComment: Boolean
    ) {
        validBrace = !insideString &&
                !insideSingleLineComment &&
                !insideMultiLineComment &&
                !insideCharacter

        validDoubleQuote = !insideCharacter &&
                !insideSingleLineComment &&
                !insideMultiLineComment

        validSingleQuote = !insideString &&
                !insideSingleLineComment &&
                !insideMultiLineComment

        validSingleComment = !insideString &&
                !insideCharacter &&
                !insideMultiLineComment

        validMultiComment = !insideString &&
                !insideCharacter &&
                !insideSingleLineComment
    }
}
