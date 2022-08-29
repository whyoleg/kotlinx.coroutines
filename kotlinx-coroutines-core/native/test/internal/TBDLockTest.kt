/*
 * Copyright 2016-2022 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.internal

import kotlinx.atomicfu.*
import kotlin.native.concurrent.*
import kotlin.test.*

class NonReentrantLockTest {

    private val lock = SpinLockWithCondition()
    private val condition = lock.Condition()
    private var counter = 0
    private val iterations = 1_000
    private val workers = 8
    private val expectedResult = iterations * workers
    private var isDone = atomic(false)

    @Test
    fun testStress() {
        val workers = List(workers) { Worker.start() }
        workers.forEach {
            it.executeAfter {
                repeat(iterations) {
                    lock.lock()

                    try {
                        if (++counter == expectedResult) {
                            isDone.value = true
                            condition.notifyOne()
                        }
                    } finally {
                        lock.unlock()
                    }
                }
            }
        }


        lock.lock()
        try {
            while (!isDone.value) {
                condition.wait()
            }
        } finally {
            lock.unlock()
        }
        assertEquals(expectedResult, counter)
    }
}
