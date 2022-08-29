/*
 * Copyright 2016-2022 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.internal

import kotlinx.atomicfu.*
import kotlin.native.concurrent.*
import kotlin.test.*

private val UNPARK_TASK = {}
private const val DEFAULT = 0
private const val PARKED = 1
private const val UNPARKED = 2


internal class SpinLockWithCondition {

    private val isLocked = atomic(false)

    private class ParkSupport(private val worker: Worker) {
        private val parkingState = atomic(0)

        fun park() {
            parkingState.loop { state ->
                when (state) {
                    DEFAULT -> {
                        if (parkingState.compareAndSet(DEFAULT, PARKED)) {
                            worker.park(-1)
                            return
                        }
                    }
                    UNPARKED -> {
                        return
                    }
                    PARKED -> {
                        fail("Unexpected state: worker is already parked")
                    }
                }
            }
        }

        fun unpark() {
            parkingState.loop { state ->
                when (state) {
                    DEFAULT -> {
                        if (parkingState.compareAndSet(DEFAULT, UNPARKED)) {
                            return
                        }
                    }
                    UNPARKED -> {
                        fail("Unexpected state: worker is already unparked")
                    }
                    PARKED -> {
                        parkingState.compareAndSet(DEFAULT, UNPARKED)
                        worker.executeAfter(0L, UNPARK_TASK)
                        return
                    }
                }
            }
        }
    }

    inner class Condition {
        private val waitingWorkers = ArrayList<ParkSupport>()

        fun wait() {
            assertLocked()
            val worker = ParkSupport(Worker.current)
            waitingWorkers += worker
            unlock() // Release before parking
            worker.park()
            lock() // Immediately lock
        }

        fun notifyOne() {
            assertLocked()
            waitingWorkers.removeLastOrNull()?.unpark()
        }
    }

    public fun lock() {
        isLocked.loop { locked ->
            if (locked) return@loop
            else if (isLocked.compareAndSet(false, true)) return
        }
    }


    public fun unlock() {
        require(isLocked.compareAndSet(true, false)) { "Lock release should always succeed" }
    }

    private fun assertLocked() {
        require(isLocked.value) { "Lock should be locked" }
    }
}

internal inline fun <T> SpinLockWithCondition.withLock(block: () -> T): T {
    try {
        lock()
        return block()
    } finally {
        unlock()
    }
}
