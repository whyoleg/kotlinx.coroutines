/*
 * Copyright 2016-2019 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.flow

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlin.test.*

class OnElementDropTest : TestBase() {

    @Test
    fun testOnElementDrop1() = runTest {
        flow<Int> {
            repeat(10) {
                println("Before emit: $it")
                emit(it)
                println("After emit: $it")
            }
        }.buffer().onEach {
            println("operation: $it")
        }.take(5).onAnyElementDrop {
            println("drop: $it")
        }.collect {
            println("collect: $it")
            delay(100)
        }
    }

    @Test
    fun testonElementDrop() = runTest {
        flow<String> {
            emit("a")
            emit("bb")
            emit("ccc")
            emit("dddd")
        }.onEach {
            println("Next String: $it")
        }.buffer(65).map {
            it.length
        }.onCurrentElementDrop {
            println("Drop Int: $it")
        }.onEach {
            println("Next Int: $it")
        }.onOtherElementDrop<_, String> {
            println("Drop String: $it")
        }.onEach {
            println("intermediate Int: $it")
        }.buffer(1).onAnyElementDrop {
            println("Drop Any: $it")
        }.map {
            it.toString() + "!!!"
        }.collect {
            println("Result: $it")
            delay(1000)
            error("HM")
        }
    }
}
