/*
 * Copyright 2016-2021 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines

import kotlinx.atomicfu.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.internal.*
import kotlin.coroutines.*
import kotlin.native.concurrent.*

@ExperimentalCoroutinesApi
public actual fun newSingleThreadContext(name: String): CloseableCoroutineDispatcher {
    return WorkerDispatcher(name)
}

public actual fun newFixedThreadPoolContext(nThreads: Int, name: String): CloseableCoroutineDispatcher {
    require(nThreads >= 1) { "Expected at least one thread, but got: $nThreads"}
    return MultiWorkerDispatcher(name, nThreads)
}

internal class WorkerDispatcher(name: String) : CloseableCoroutineDispatcher(), Delay {
    private val worker = Worker.start(name = name)

    override fun dispatch(context: CoroutineContext, block: Runnable) {
        worker.executeAfter(0L) { block.run() }
    }

    override fun scheduleResumeAfterDelay(timeMillis: Long, continuation: CancellableContinuation<Unit>) {
        worker.executeAfter(timeMillis.toMicrosSafe()) {
            with(continuation) { resumeUndispatched(Unit) }
        }
    }

    override fun invokeOnTimeout(timeMillis: Long, block: Runnable, context: CoroutineContext): DisposableHandle {
        // Workers don't have an API to cancel sent "executeAfter" block, but we are trying
        // to control the damage and reduce reachable objects by nulling out `block`
        // that may retain a lot of references, and leaving only an empty shell after a timely disposal
        // This is a class and not an object with `block` in a closure because that would defeat the purpose.
        class DisposableBlock(block: Runnable) : DisposableHandle, Function0<Unit> {
            private val disposableHolder = AtomicReference<Runnable?>(block)

            override fun invoke() {
                disposableHolder.value?.run()
            }

            override fun dispose() {
                disposableHolder.value = null
            }
        }

        val disposableBlock = DisposableBlock(block)
        worker.executeAfter(timeMillis.toMicrosSafe(), disposableBlock)
        return disposableBlock
    }

    override fun close() {
        worker.requestTermination().result // Note: calling "result" blocks
    }

    private fun Long.toMicrosSafe(): Long {
        val result = this * 1000
        return if (result > this) result else Long.MAX_VALUE
    }
}

public class MultiWorkerDispatcher(
    private val name: String, private val workersCount: Int
) : CloseableCoroutineDispatcher() {
    private val runningWorkers = atomic(0)
    private val queue = DispatcherQueue()
    private val workers = atomicArrayOfNulls<Worker>(workersCount)
    private val isTerminated = atomic(false)

    override fun dispatch(context: CoroutineContext, block: Runnable) {
        if (runningWorkers.value != workersCount) {
            tryAddWorker()
        }
        queue.put(block)
    }

    private fun tryAddWorker() {
        runningWorkers.loop {
            if (it == workersCount) return
            if (runningWorkers.compareAndSet(it, it + 1)) {
                addWorker(it)
                return
            }
        }
    }

    private fun addWorker(sequenceNumber: Int) {
        val worker = Worker.start(name = "$name-#$sequenceNumber")
        workers[sequenceNumber].value = worker
        worker.executeAfter(0L) {
            workerLoop()
        }
    }

    private fun workerLoop() {
        while (!isTerminated.value) {
            val runnable = queue.take()
            runnable.run()
        }
    }

    override fun close() {
        // TODO it races with worker creation
        if (!isTerminated.compareAndSet(false, true)) return
        repeat(workersCount) {
            queue.put(Runnable {}) // Empty poison pill to wakeup workers and make them check isTerminated
        }

        val requests = ArrayList<Future<Unit>>()
        for (i in 0 until workers.size) {
            val worker = workers[i].value ?: continue
            requests += worker.requestTermination(false)
        }
        for (request in requests) {
            request.result // Wait for workers termination
        }

//        queue.close()
    }
}
