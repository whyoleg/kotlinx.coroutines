/*
 * Copyright 2016-2022 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.internal

/**
 * A domain-specific bounded array-backed blocking queue used
 * in multithreaded dispatchers.
 *
 * It is written in the most straightforward way and is not expected to be a bottleneck.
 *
 * NB: this implementation used POSIX mutexes and should be explicitly [closed][close]
 * in order to release underlying POSIX-specific primitives.
 */
internal class BlockingQueue<T: Any>(capacity: Int) {

    init {
        // Prior to all resource allocations
        require(capacity >= 1) { "Capacity should be positive, but had $capacity" }
    }

    private val lock = SpinLockWithCondition()
    private val notEmptyCondition = lock.Condition()
    private val notFullCondition = lock.Condition()

    private val elements = arrayOfNulls<Any?>(capacity)
    private var tailIndex = 0
    private var headIndex = 0
    private var size = 0

    fun put(value: T) = lock.withLock {
        while (size == elements.size) {
            notFullCondition.wait()
        }
        elements[headIndex] = value
        if (++headIndex == elements.size) headIndex = 0
        ++size
        notEmptyCondition.notifyOne()
    }

    fun take(): T = lock.withLock {
        while (size == 0) {
            notEmptyCondition.wait()
        }
        val result = elements[tailIndex]
        elements[tailIndex] = null
        if (++tailIndex == elements.size) tailIndex = 0
        --size
        notFullCondition.notifyOne()
        @Suppress("UNCHECKED_CAST")
        return result as T
    }

    fun close() {
        // TODO
    }
}
