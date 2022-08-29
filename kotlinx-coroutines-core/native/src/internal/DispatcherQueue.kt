/*
 * Copyright 2016-2022 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.internal

import kotlinx.atomicfu.*
import kotlinx.coroutines.*

internal class DispatcherQueue {

    private val queue = LockFreeTaskQueue<Runnable>(false)
    private val lock = SpinLockWithCondition()
    private val nonEmpty = lock.Condition()
    private val nonEmptyWaiters = atomic(0)

    fun put(value: Runnable) {
        queue.addLast(value)
        notifyNonEmpty()
    }

    private fun notifyNonEmpty() {
        if (nonEmptyWaiters.value == 0) return
        lock.withLock {
            nonEmpty.notifyOne()
        }
    }

    fun take(): Runnable {
        takeFastPath()?.let { return it }
        return takeSlowPath()
    }

    private fun takeFastPath(): Runnable? {
        val fastPathResult = queue.removeFirstOrNull()
        if (fastPathResult !== null) {
            return fastPathResult
        }
        return null
    }

    private fun takeSlowPath(): Runnable = lock.withLock {
        nonEmptyWaiters.incrementAndGet()
        while (true) {
            // Check if `put` missed our increment
            val result = takeFastPath()
            if (result !== null) {
                nonEmptyWaiters.decrementAndGet()
                return result
            }
            nonEmpty.wait()
        }
        TODO() // Hack for some reason required for this code to compile
    }
}
