import kotlinx.atomicfu.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlin.coroutines.*
import kotlin.native.concurrent.*
import kotlin.random.*
import kotlin.test.*
import kotlin.time.*

/*
 * Copyright 2016-2022 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

class PretendToBeBenchmark {

    private val concurrency = 8
    private var totalSum = atomic(0)

    @Test
    fun contendedNew() {
        measureContended { MultiWorkerDispatcher("", concurrency) }
        measureContended { MultiWorkerDispatcher("", concurrency) }
    }

    @Test
    fun contendedOld() {
        measureContended { MultiWorkerDispatcherOld("", concurrency) }
        measureContended { MultiWorkerDispatcherOld("", concurrency) }
    }

    @OptIn(ExperimentalTime::class)
    fun measureContended(dispatcher: () -> CloseableCoroutineDispatcher) {
        val d = dispatcher()
        warmupDispatcher(d) // Worker.start takes ~0.5 seconds
        val value = measureTimedValue {
            doContended(d)
            d
        }
        println("Run took: ${value.duration}")
        value.value.close()
    }

    private fun warmupDispatcher(d: CloseableCoroutineDispatcher) {
        runBlocking {
            val ch = Channel<Unit>()
            repeat(concurrency) {
                launch(d) { ch.receive() }
            }
            delay(500)
            repeat(concurrency) {
                ch.send(Unit)
            }
        }
    }

    @Test
    fun pingPongNew() {
        measurePingPong { MultiWorkerDispatcher("", concurrency) }
        measurePingPong { MultiWorkerDispatcher("", concurrency) }
    }

    @Test
    fun pingPongOld() {
        measurePingPong { MultiWorkerDispatcherOld("", concurrency) }
        measurePingPong { MultiWorkerDispatcherOld("", concurrency) }
    }

    @OptIn(ExperimentalTime::class)
    fun measurePingPong(dispatcher: () -> CloseableCoroutineDispatcher) {
        val value = measureTimedValue {
            val d = dispatcher()
            doPingPong(d)
            d
        }
        println("Run took: ${value.duration}")
        value.value.close()
    }

    private fun doContended(dispatcher: CoroutineDispatcher) = runBlocking {
        repeat(concurrency) {
            launch(dispatcher) {
                val r = Random(Random.nextInt())
                repeat(10_000) {
                    doCpuWork(r)
                    yield()
                }
                totalSum.addAndGet(r.nextInt())
            }
        }
    }

    private fun doCpuWork(r: Random) {
        repeat(10_000) {
            r.nextInt()
        }
    }

    private fun doPingPong(dispatcher: CoroutineDispatcher) = runBlocking {
        val channel = Channel<Int>(0)
        launch(dispatcher) {
            repeat(10_000) {
                channel.send(it)
            }
            channel.close()
        }

        launch(dispatcher) {
            for (i in channel) {
                // Empty loop, just consume the channel
            }
        }
    }

    // Old implementation from pre 1.7.0
    internal class MultiWorkerDispatcherOld(name: String, workersCount: Int) : CloseableCoroutineDispatcher() {
        private val tasksQueue = Channel<Runnable>(Channel.UNLIMITED)
        private val workers = Array(workersCount) { Worker.start(name = "$name-$it") }

        init {
            workers.forEach { w -> w.executeAfter(0L) { workerRunLoop() } }
        }

        private fun workerRunLoop() = runBlocking {
            for (task in tasksQueue) {
                task.run()
            }
        }

        override fun dispatch(context: CoroutineContext, block: Runnable) {
            tasksQueue.trySend(block)
        }

        override fun close() {
            tasksQueue.close()
            workers.forEach { it.requestTermination().result }
        }
    }

}
