package kotlinx.coroutines.channels

import kotlinx.atomicfu.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.ChannelResult.Companion.closed
import kotlinx.coroutines.channels.ChannelResult.Companion.failure
import kotlinx.coroutines.channels.ChannelResult.Companion.success
import kotlinx.coroutines.internal.*
import kotlinx.coroutines.selects.*
import kotlinx.coroutines.selects.TrySelectDetailedResult.*
import kotlin.coroutines.*
import kotlin.js.*
import kotlin.jvm.*
import kotlin.math.*
import kotlin.random.*
import kotlin.reflect.*

/**
 * The buffered channel implementation, which also serves as a rendezvous channel when the capacity is zero.
 * The high-level structure bases on a conceptually infinite array for storing elements and waiting requests,
 * separate counters of [send] and [receive] invocations that were ever performed, and an additional counter
 * that indicates the end of the logical buffer by counting the number of array cells it ever contained.
 * The key idea is that both [send] and [receive] start by incrementing their counters, assigning the array cell
 * referenced by the counter. In case of rendezvous channels, the operation either suspends and stores its continuation
 * in the cell or makes a rendezvous with the opposite request. Each cell can be processed by exactly one [send] and
 * one [receive]. As for buffered channels, [send]-s can also add elements without suspension if the logical buffer
 * contains the cell, while the [receive] operation updates the end of the buffer when its synchronization finishes.
 *
 * Please see the paper "Fast and Scalable Channels with Applications to Kotlin Coroutines"
 * by Nikita Koval, Roman Elizarov, and Dan Alistarh for the detailed algorithm description:
 * TODO: link to the arxiv paper
 */
internal open class BufferedChannel<E>(
    /**
     * Channel capacity, `0` for rendezvous channel
     * and `Channel.UNLIMITED` for unlimited capacity.
     */
    capacity: Int,
    @JvmField
    protected val onUndeliveredElement: OnUndeliveredElement<E>? = null
) : Channel<E> {
    init {
        require(capacity >= 0) { "Invalid channel capacity: $capacity, should be >=0" }
    }

    /*
      The counters and the segments for send, receive, and buffer expansion operations.
      The counters are incremented in the beginning of the corresponding operation;
      thus, acquiring a unique (for the operation type) cell to process.

      The counter for send is combined with the channel closing status for synchronization
      simplicity and performance reasons.

      The logical end of the buffer is initialized with the channel capacity. When the channel
      is rendezvous or unlimited, the counter equals `BUFFER_END_RENDEZVOUS` or `BUFFER_END_RENDEZVOUS`,
      correspondingly, and never updates.
     */
    private val sendersAndCloseStatus = atomic(0L)
    private val receivers = atomic(0L)
    private val bufferEnd = atomic(initialBufferEnd(capacity))

    private val rendezvousOrUnlimited
        get() = bufferEnd.value.let { it == BUFFER_END_RENDEZVOUS || it == BUFFER_END_UNLIMITED }

    private val sendSegment: AtomicRef<ChannelSegment<E>>
    private val receiveSegment: AtomicRef<ChannelSegment<E>>
    private val bufferEndSegment: AtomicRef<ChannelSegment<E>>

    init {
        val s = ChannelSegment<E>(0, null, 3)
        sendSegment = atomic(s)
        receiveSegment = atomic(s)
        @Suppress("UNCHECKED_CAST")
        bufferEndSegment = atomic(if (rendezvousOrUnlimited) (NULL_SEGMENT as ChannelSegment<E>) else s)
    }

    // #########################
    // ## The send operations ##
    // #########################

    override suspend fun send(element: E): Unit = sendImpl( // <-- this is an inline function
        element = element,
        // Do not create a continuation until it is required,
        // it is later created via [onNoWaiterSuspend] below, if needed.
        waiter = null,
        // Finish immediately if a rendezvous happens
        // or the element has been buffered.
        onRendezvousOrBuffered = {},
        // As no waiter is provided, suspension is impossible.
        onSuspend = { _, _ -> assert { false } },
        // According to the `send(e)` contract, we need to call
        // `onUndeliveredElement(..)` handler and throw exception
        // if the channel is already closed.
        onClosed = { onClosedSend(element, coroutineContext) },
        // When `send(e)` decides to suspend, the corresponding
        // `suspend` function that creates a continuation is called.
        // The tail-call optimization is applied here.
        onNoWaiterSuspend = { segm, i, elem, s -> sendOnNoWaiterSuspend(segm, i, elem, s) }
    )

    private fun onClosedSend(element: E, coroutineContext: CoroutineContext) {
        onUndeliveredElement?.callUndeliveredElement(element, coroutineContext)
        throw recoverStackTrace(sendException(getCloseCause()))
    }

    private suspend fun sendOnNoWaiterSuspend(
        // The working cell is specified via
        // the segment and the index in it.
        segment: ChannelSegment<E>,
        i: Int,
        // The element to be inserted.
        element: E,
        // The global index of the cell.
        s: Long
    ) = suspendCancellableCoroutineReusable<Unit> sc@{ cont ->
        sendImplOnNoWaiter(  // <-- this is an inline function
            segment = segment, index = i, element = element, s = s,
            // Store the created continuation as a waiter.
            waiter = cont,
            // If a rendezvous happens or the element has been buffered,
            // resume the continuation and finish. In case of prompt
            // cancellation, it is guaranteed that the element
            // has been already buffered or passed to receiver.
            onRendezvousOrBuffered = { cont.resume(Unit) },
            // Clean the cell on suspension and invoke
            // `onUndeliveredElement(..)` if needed.
            onSuspend = { segm, i -> cont.prepareSenderForSuspension(segm, i) },
            // If the channel is closed, call `onUndeliveredElement(..)` and complete the
            // continuation with the corresponding exception.
            onClosed = { onClosedSendOnNoWaiterSuspend(element, cont) },
        )
    }

    private fun CancellableContinuation<*>.prepareSenderForSuspension(
        // The working cell is specified via
        // the segment and the index in it.
        segment: ChannelSegment<E>,
        index: Int
    ) {
        invokeOnCancellation(SenderCancellationHandler(segment, index, context).asHandler)
    }

    private inner class SenderCancellationHandler(
        private val segment: ChannelSegment<E>,
        private val index: Int,
        private val context: CoroutineContext
    ) : BeforeResumeCancelHandler() {
        override fun invoke(cause: Throwable?) {
            segment.onCancellation(index, onUndeliveredElement, context)
        }
    }

    private fun onClosedSendOnNoWaiterSuspend(element: E, cont: CancellableContinuation<Unit>) {
        onUndeliveredElement?.callUndeliveredElement(element, cont.context)
        cont.resumeWithException(recoverStackTrace(sendException(getCloseCause()), cont))
    }

    override fun trySend(element: E): ChannelResult<Unit> {
        // Do not try to send the value when the plain `send(e)` operation should suspend.
        if (shouldSendSuspend(sendersAndCloseStatus.value)) return failure()
        // This channel either has waiting receivers or is closed.
        // Let's try to send the element!
        // The logic is similar to the plain `send(e)` operation, with
        // the only difference that we use a special `INTERRUPTED` token
        // as a waiter. Intuitively, in case of suspension (the checks above
        // can become outdated), we insert an already cancelled waiter by
        // putting `INTERRUPTED` to the cell.
        return sendImpl( // <-- this is an inline function
            element = element,
            // Use a special token that represents a cancelled waiter.
            // Receivers cannot resume it and skip the corresponding cell.
            waiter = INTERRUPTED_SEND,
            // Finish successfully when a rendezvous happens
            // or the element has been buffered.
            onRendezvousOrBuffered = { success(Unit) },
            // On suspension, the `INTERRUPTED` token has been installed,
            // and this `trySend(e)` fails. According to the contract,
            // we do not need to call [onUndeliveredElement] handler as
            // in the plain `send(e)` operation.
            onSuspend = { _, _ -> failure() },
            // When the channel is closed, return the corresponding result.
            onClosed = { onClosedTrySend() }
        )
    }

    private fun onClosedTrySend(): ChannelResult<Unit> {
        return closed(sendException(getCloseCause()))
    }

    /**
     * This is a special `send(e)` implementation that returns `true` if the element
     * has been successfully sent, and `false` if the channel is closed.
     *
     * In case of coroutine cancellation, the element may be undelivered --
     * the [onUndeliveredElement] feature is unsupported in this implementation.
     *
     * Note that this implementation always invokes [suspendCancellableCoroutineReusable],
     * as we do not care about broadcasts performance -- they are already deprecated.
     */
    internal open suspend fun sendBroadcast(element: E): Boolean = suspendCancellableCoroutineReusable { cont ->
        check(onUndeliveredElement == null) {
            "the `onUndeliveredElement` feature is unsupported for `sendBroadcast(e)`"
        }
        sendImpl(
            element = element,
            waiter = SendBroadcast(cont),
            onRendezvousOrBuffered = { cont.resume(true) },
            onSuspend = { segm, i -> cont.prepareSenderForSuspension(segm, i) },
            onClosed = { cont.resume(false) }
        )
    }

    /**
     * Specifies waiting [sendBroadcast] operation.
     */
    private class SendBroadcast(val cont: CancellableContinuation<Boolean>) : Waiter

    /**
     * Abstract `send(e)` implementation.
     */
    private inline fun <W, R> sendImpl(
        // The element to be sent.
        element: E,
        // The waiter to be stored in case of suspension,
        // or `null` if the waiter is not created yet.
        // In the latter case, if the algorithm decides
        // to suspend, [onNoWaiterSuspend] is called.
        waiter: W,
        // This lambda is invoked when the element has been
        // buffered or a rendezvous with a receiver happens.
        onRendezvousOrBuffered: () -> R,
        // This lambda is called when the operation suspends in the
        // cell specified by the segment `segm` and the index `i` in it.
        onSuspend: (segm: ChannelSegment<E>, i: Int) -> R,
        // This lambda is called when the channel
        // is observed in the closed state.
        onClosed: () -> R,
        // This lambda is called when the operation decides
        // to suspend, but the waiter is not provided (equals `null`).
        // It should create a waiter and delegate to `sendImplOnNoWaiter`.
        onNoWaiterSuspend: (
            segm: ChannelSegment<E>,
            i: Int,
            element: E,
            s: Long
        ) -> R = { _, _, _, _ -> error("unexpected") }
    ): R {
        // Read the segment reference before the counter increment.
        var segment = sendSegment.value
        while (true) {
            // Atomically increment the `senders` counter and obtain the
            // value before the increment and the close status.
            val sendersAndCloseStatusCur = sendersAndCloseStatus.getAndIncrement()
            val s = sendersAndCloseStatusCur.counter
            // Is the channel already closed? Keep this information.
            val closed = sendersAndCloseStatusCur.isClosedForSend0
            // Count the required segment id and the cell index in it.
            val id = s / SEGMENT_SIZE
            val i = (s % SEGMENT_SIZE).toInt()
            // Try to find the required segment if the initially obtained
            // segment (in the beginning of this function) has lower id.
            if (segment.id != id) {
                // Find the required segment.
                segment = findSegmentSend(id, segment) ?:
                    // The segment has not been found.
                    if (closed) return onClosed() else continue
            }
            // Update the cell according to the algorithm. Importantly, when
            // the channel is already closed, storing a waiter is illegal, so
            // the algorithm stores the `INTERRUPTED` token in this case.
            when(updateCellSend(segment, i, element, s, if (closed) INTERRUPTED_SEND else waiter)) {
                RESULT_RENDEZVOUS -> {
                    // A rendezvous with a receiver has happened.
                    // Also, the previous segments are no longer needed
                    // for the upcoming requests; thus, the algorithm
                    // resets the link to the previous segment.
                    segment.cleanPrev()
                    return onRendezvousOrBuffered()
                }
                RESULT_BUFFERED -> {
                    // The element has buffered.
                    return onRendezvousOrBuffered()
                }
                RESULT_SUSPEND -> {
                    // The operation has decided to suspend and stored the
                    // specified waiter. If the channel is already closed,
                    // and the `INTERRUPTED` token has been stored as a waiter,
                    // this request finishes with the `onClosed()` action.
                    if (closed) return onClosed()
                    return onSuspend(segment, i)
                }
                RESULT_FAILED -> {
                    // The current cell processing has failed
                    // (either the cell is poisoned, the stored waiter
                    // is cancelled, or the channel is already closed).
                    segment.cleanPrev() // the previous segments are no longer needed.
                    // If the channel is already closed, proceed correspondingly.
                    if (closed) return onClosed()
                    // Restart the operation from the beginning.
                    continue
                }
                RESULT_SUSPEND_NO_WAITER -> {
                    // The operation has decided to suspend,
                    // but no waiter has been provided.
                    return onNoWaiterSuspend(segment, i, element, s)
                }
            }
        }
    }

    private inline fun <R, W : Any> sendImplOnNoWaiter(
        // The working cell is specified via
        // the segment and the index in it.
        segment: ChannelSegment<E>,
        index: Int,
        // The element to be sent.
        element: E,
        // The global index of the cell.
        s: Long,
        // The waiter to be stored in case of suspension.
        waiter: W,
        // This lambda is invoked when the element has been
        // buffered or a rendezvous with a receiver happens.
        onRendezvousOrBuffered: () -> R,
        // This lambda is called when the operation suspends in the
        // cell specified by the segment `segm` and the index `i` in it.
        onSuspend: (segm: ChannelSegment<E>, i: Int) -> R,
        // This lambda is called when the channel
        // is observed in the closed state.
        onClosed: () -> R,
    ): R =
        // Update the cell with the non-null waiter,
        // restarting from the beginning on failure.
        // Check the `sendImpl(..)` function for the comments.
        when(updateCellSend(segment, index, element, s, waiter)) {
            RESULT_RENDEZVOUS -> {
                segment.cleanPrev()
                onRendezvousOrBuffered()
            }
            RESULT_BUFFERED -> {
                onRendezvousOrBuffered()
            }
            RESULT_SUSPEND -> {
                onSuspend(segment, index)
            }
            RESULT_FAILED -> {
                segment.cleanPrev()
                // On failure, restart the operation from the beginning.
                sendImpl(
                    element = element,
                    waiter = waiter,
                    onRendezvousOrBuffered = onRendezvousOrBuffered,
                    onSuspend = onSuspend,
                    onClosed = onClosed,
                )
            }
            else -> error("unexpected")
        }

    /**
     * The algorithm that updates the working cell for an abstract send operation.
     */
    private fun updateCellSend(
        // The working cell is specified via
        // the segment and the index in it.
        segment: ChannelSegment<E>,
        index: Int,
        // The element to be sent.
        element: E,
        // The global index of the cell.
        s: Long,
        // The waiter to be stored in case of suspension.
        waiter: Any?,
    ): Int {
        // First, the algorithm stores the element.
        segment.storeElement(index, element)
        // Then, the cell state should be updated
        // according to the state machine.
        // See the paper mentioned in the very beginning for
        // the cell life-cycle diagram and the algorithm details.
        while (true) {
            // Read the current cell state.
            val state = segment.getState(index)
            when {
                // The cell is empty.
                state === null -> {
                    // If the element should be buffered, ar a rendezvous should happen
                    // while the receiver is still coming, try to buffer the element.
                    // Otherwise, try to store the specified waiter in the cell.
                    if (bufferOrRendezvousSend(s)) {
                        // Move the cell state to `BUFFERED`.
                        if (segment.casState(index, null, BUFFERED)) {
                            // The element has been successfully buffered, finish.
                            return RESULT_BUFFERED
                        }
                    } else {
                        // This `send(e)` operation should suspend.
                        if (waiter === null) {
                            // The waiter is not specified;
                            // return the corresponding result.
                            return RESULT_SUSPEND_NO_WAITER
                        }
                        // Try to install the waiter.
                        if (segment.casState(index, null, waiter)) {
                            // The waiter has been successfully installed, finish.
                            return RESULT_SUSPEND
                        }
                    }
                }
                // This cell is in the logical buffer.
                state === IN_BUFFER -> {
                    // Try to buffer the element.
                    if (segment.casState(index, state, BUFFERED)) {
                        // The element has been successfully buffered, finish.
                        return RESULT_BUFFERED
                    }
                }
                // Fail if the cell is poisoned by a concurrent receiver,
                // or the receiver stored in this cell was cancelled,
                // or the channel is already closed.
                state === POISONED || state === INTERRUPTED || state === INTERRUPTED_EB || state === CHANNEL_CLOSED -> {
                    // Clean the element slot to avoid memory leaks and finish.
                    segment.cleanElement(index)
                    return RESULT_FAILED
                }
                // A waiting receiver is stored in the cell.
                else -> {
                    if (!(state is Waiter || state is WaiterEB)) error(state.toString())
                    assert { state is Waiter || state is WaiterEB }
                    // As the element will be passed directly to the waiter,
                    // the algorithm cleans the element slot in the cell.
                    segment.cleanElement(index)
                    // Unwrap the waiting receiver from `WaiterEB` if needed.
                    val receiver = if (state is WaiterEB) state.waiter else state
                    // Try to make a rendezvous with the receiver.
                    return if (receiver.tryResumeReceiver(element)) {
                        // Move the cell state to DONE.
                        segment.setStateLazy(index, DONE)
                        onReceiveDequeued()
                        RESULT_RENDEZVOUS
                    } else RESULT_FAILED
                }
            }
        }
    }

    /**
     * Checks whether a [send] invocation is bound to suspend if it is called
     * with the specified [sendersAndCloseStatus] value, the current [receivers]
     * and [bufferEnd] counters. When this channel is already closed, the function
     * returns `false`.
     *
     * Specifically, [send] suspends if the channel is NOT unlimited,
     * the number of receivers is greater than then index of the working cell of the
     * potential [send] invocation, and the buffer does not cover this cell
     * in case of buffered channel.
     */
    @JsName("shouldSendSuspend0")
    private fun shouldSendSuspend(curSendersAndCloseStatus: Long): Boolean {
        // Does not suspend if the channel is already closed.
        if (curSendersAndCloseStatus.isClosedForSend0) return false
        // Does not suspend if a rendezvous can happen or the buffer is not full.
        return !bufferOrRendezvousSend(curSendersAndCloseStatus.counter)
    }

    private fun bufferOrRendezvousSend(curSenders: Long): Boolean =
        curSenders < bufferEnd.value || curSenders < receivers.value

    /**
     * Checks whether a [send] invocation is bound to suspend if it is called
     * with the current counter and close status values. See [shouldSendSuspend] for details.
     *
     * Note that this implementation is _false positive_ in case of rendezvous channels,
     * and can `false` when a [send] invocation is bound to suspend. Specifically, when
     * the counter of `receive()` operations may indicate that there is a waiting receiver,
     * while it has already been cancelled and the rendezvous is bound to fail.
     */
    internal open fun shouldSendSuspend(): Boolean = shouldSendSuspend(sendersAndCloseStatus.value)

    /**
     * Tries to resume this receiver with the specified [element] as a result.
     * Returns `true` on success and `false` otherwise.
     */
    @Suppress("UNCHECKED_CAST")
    private fun Any.tryResumeReceiver(element: E): Boolean = when(this) {
        is SelectInstance<*> -> { // `onReceiveXXX` select clause
            trySelect(this@BufferedChannel, element)
        }
        is ReceiveCatching<*> -> {
            this as ReceiveCatching<E>
            cont.tryResume0(success(element), onUndeliveredElement?.bindCancellationFun(element, cont.context))
        }
        is BufferedChannel<*>.BufferedChannelIterator -> {
            this as BufferedChannel<E>.BufferedChannelIterator
            tryResumeHasNext(element)
        }
        is CancellableContinuation<*> -> { // plain `receive()`
            this as CancellableContinuation<E>
            tryResume0(element, onUndeliveredElement?.bindCancellationFun(element, context))
        }
        else -> error("Unexpected receiver type: $this")
    }

    // ##########################
    // # The receive operations #
    // ##########################

    /**
     * This function is invoked when a receiver is added as a waiter in this channel.
     */
    protected open fun onReceiveEnqueued() {}
    /**
     * This function is invoked when a waiting receiver is no longer stored in this channel;
     * independently on whether it is caused by rendezvous, cancellation, or channel closing.
     */
    protected open fun onReceiveDequeued() {}
    /**
     * This function is invoked when the receiving operation ([receive], [tryReceive],
     * [BufferedChannelIterator.hasNext], etc.) finishes its synchronization -- either
     * completing due to an element retrieving or discovering this channel in the closed state,
     * or deciding to suspend if this channel is empty and not closed.
     *
     * We use this function to protect all receive operations with global lock in [ConflatedBroadcastChannel],
     * by acquiring the lock in the beginning of each operation and releasing it when the synchronization
     * completes, via this function.
     */
    protected open fun onReceiveSynchronizationCompletion() {}

    override suspend fun receive(): E = receiveImpl(
        // Do not create a continuation until it is required,
        // it is later created via [onNoWaiterSuspend] below, if needed.
        waiter = null,
        // Return the received element if a rendezvous happens
        // or the element was buffered. Also, inform the `BufferedChannel`
        // extensions that the synchronization has been completed.
        onRendezvous = { element ->
            onReceiveSynchronizationCompletion()
            return element
        },
        // As no waiter is provided, suspension is impossible.
        onSuspend = { _, _ -> error("unexpected") },
        // Throw an exception if the channel is already closed.
        onClosed = { onClosedReceive() },
        // When `receive()` decides to suspend, the corresponding
        // `suspend` function that creates a continuation is called.
        // The tail-call optimization is applied here.
        onNoWaiterSuspend = { segm, i, r -> receiveOnNoWaiterSuspend(segm, i, r) }
    )

    private fun onClosedReceive(): E =
        throw recoverStackTrace(receiveException(getCloseCause()))
            .also { onReceiveSynchronizationCompletion() }

    private suspend fun receiveOnNoWaiterSuspend(
        // The working cell is specified via
        // the segment and the index in it.
        segm: ChannelSegment<E>,
        i: Int,
        // The global index of the cell.
        r: Long
    ) = suspendCancellableCoroutineReusable<E> { cont ->
        receiveImplOnNoWaiter(
            segm, i, r,
            waiter = cont,
            onRendezvous = { element ->
                onReceiveSynchronizationCompletion()
                cont.resume(element) {
                    onUndeliveredElement?.callUndeliveredElement(element, cont.context)
                }
            },
            onSuspend = { segm, i ->
                onReceiveEnqueued()
                onReceiveSynchronizationCompletion()
                cont.invokeOnCancellation {
                    segm.onCancellation(i)
                    onReceiveDequeued()
                }
            },
            onClosed = {
                onReceiveSynchronizationCompletion()
                cont.resumeWithException(receiveException(getCloseCause()))
            },
        )
    }

    override suspend fun receiveCatching(): ChannelResult<E> = receiveImpl(
        // Do not create a continuation until it is required,
        // it is later created via [onNoWaiterSuspend] below, if needed.
        waiter = null,
        // Finish successfully with the received element as a resul
        // if a rendezvous happens or the element was buffered.
        // Also, inform the `BufferedChannel` extensions that
        // the synchronization has been completed.
        onRendezvous = { element ->
            onReceiveSynchronizationCompletion()
            success(element)
        },
        // As no waiter is provided, suspension is impossible.
        onSuspend = { _, _ -> error("unexpected") },
        // If the channel is already closed, finish with the corresponding exception.
        onClosed = { onClosedReceiveCatching() },
        // When `receiveCatching()` decides to suspend, the corresponding
        // `suspend` function that creates a continuation is called.
        // The tail-call optimization is applied here.
        onNoWaiterSuspend = { segm, i, r -> receiveCatchingOnNoWaiterSuspend(segm, i, r) }
    )

    private fun onClosedReceiveCatching(): ChannelResult<E> =
        closed<E>(getCloseCause()).also { onReceiveSynchronizationCompletion() }

    private suspend fun receiveCatchingOnNoWaiterSuspend(
        // The working cell is specified via
        // the segment and the index in it.
        segm: ChannelSegment<E>,
        i: Int,
        // The global index of the cell.
        r: Long
    ) = suspendCancellableCoroutineReusable<ChannelResult<E>> { cont ->
        val waiter = ReceiveCatching(cont)
        receiveImplOnNoWaiter(
            segm, i, r,
            waiter = waiter,
            onRendezvous = { element ->
                onReceiveSynchronizationCompletion()
                cont.resume(success(element)) {
                    onUndeliveredElement?.callUndeliveredElement(element, cont.context)
                }
            },
            onSuspend = { segm, i ->
                onReceiveSynchronizationCompletion()
                onReceiveEnqueued()
                cont.invokeOnCancellation {
                    segm.onCancellation(i)
                    onReceiveDequeued()
                }
            },
            onClosed = {
                onReceiveSynchronizationCompletion()
                cont.resume(closed(getCloseCause()))
            },
        )
    }

    override fun tryReceive(): ChannelResult<E> =
        tryReceiveInternal().also { onReceiveSynchronizationCompletion() }

    protected fun tryReceiveInternal(): ChannelResult<E> {
        // Read `receivers` counter first.
        val r = receivers.value
        val sendersAndCloseStatusCur = sendersAndCloseStatus.value
        // Is this channel is closed for send?
        if (sendersAndCloseStatusCur.isClosedForReceive0) return onClosedTryReceive()
        // COMMENTS
        val s = sendersAndCloseStatusCur.counter
        if (r >= s) return failure()
        return receiveImpl(
            waiter = INTERRUPTED,
            onRendezvous = { element -> success(element) },
            onSuspend = { _, _ -> failure() },
            onClosed = { onClosedTryReceive() }
        )
    }

    private fun onClosedTryReceive(): ChannelResult<E> =
        closed(getCloseCause())


    /**
     * Abstract `receive()` implementation.
     */
    private inline fun <R> receiveImpl(
        // The waiter to be stored in case of suspension,
        // or `null` if the waiter is not created yet.
        // In the latter case, if the algorithm decides
        // to suspend, [onNoWaiterSuspend] is called.
        waiter: Any?,
        // This lambda is invoked when a rendezvous
        // with a waiting sender happens.
        onRendezvous: (element: E) -> R,
        // This lambda is called when the operation suspends in the
        // cell specified by the segment `segm` and the index `i` in it.
        onSuspend: (segm: ChannelSegment<E>, i: Int) -> R,
        // This lambda is called when the channel
        // is observed in the closed state and
        // no waiting senders is found.
        onClosed: () -> R,
        // This lambda is called when the operation decides
        // to suspend, but the waiter is not provided (equals `null`).
        // It should create a waiter and delegate to `sendImplOnNoWaiter`.
        onNoWaiterSuspend: (
            segm: ChannelSegment<E>,
            i: Int,
            r: Long
        ) -> R = { _, _, _ -> error("unexpected") }
    ): R {
        // Read the segment reference before the counter increment.
        var segm = receiveSegment.value
        while (true) {
            // First, the algorithm checks whether it is eligible
            // to perform the operation by checking that the channel
            // is not closed for `receive`, so it neither cancelled nor
            // closed without having waiting senders or buffered elements.
            if (sendersAndCloseStatus.value.isClosedForReceive0)
                return onClosed()
            // Atomically increment the `receivers` counter
            // and obtain the value before the increment.
            val r = this.receivers.getAndIncrement()
            // Count the required segment id and the cell index in it.
            val id = r / SEGMENT_SIZE
            val i = (r % SEGMENT_SIZE).toInt()
            // Try to find the required segment if the initially obtained
            // segment (in the beginning of this function) has lower id.
            if (segm.id != id) {
                // Find the required segment, restarting the operation
                // if it has not been found. If the channel is already
                // closed or cancelled, the operation will detect it
                // by reading the close status at the beginning.
                segm = findSegmentReceive(id, segm) ?: continue
            }
            // Update the cell according to the algorithm.
            val updCellResult = updateCellReceive(segm, i, r, waiter)
            return when {
                updCellResult === SUSPEND -> {
                    // The operation has decided to suspend and
                    // stored the specified waiter in the cell.
                    onSuspend(segm, i)
                }
                updCellResult === FAILED -> {
                    // The operation has tried to make a rendezvous
                    // but failed: either the opposite request has
                    // already been cancelled or the cell is poisoned.
                    // Restart from the beginning in this case.
                    continue
                }
                updCellResult === SUSPEND_NO_WAITER -> {
                    // The operation has decided to suspend,
                    // but no waiter has been provided.
                    onNoWaiterSuspend(segm, i, r)
                }
                else -> { // element
                    // Either a buffered element was retrieved from the cell
                    // or a rendezvous with a waiting sender has happened.
                    // Clean the reference to the previous segment before finishing.
                    segm.cleanPrev()
                    @Suppress("UNCHECKED_CAST")
                    onRendezvous(updCellResult as E)
                }
            }
        }
    }

    private inline fun <W, R> receiveImplOnNoWaiter(
        // The working cell is specified via
        // the segment and the index in it.
        segment: ChannelSegment<E>,
        i: Int,
        // The global index of the cell.
        r: Long,
        // The waiter to be stored in case of suspension.
        waiter: W,
        // This lambda is invoked when a rendezvous
        // with a waiting sender happens.
        onRendezvous: (element: E) -> R,
        // This lambda is called when the operation suspends in the
        // cell specified by the segment `segm` and the index `i` in it.
        onSuspend: (segm: ChannelSegment<E>, i: Int) -> R,
        // This lambda is called when the channel
        // is observed in the closed state and
        // no waiting senders is found.
        onClosed: () -> R
    ): R {
        // Update the cell with the non-null waiter,
        // restarting from the beginning on failure.
        // Check the `receiveImpl(..)` function for the comments.
        val updCellResult = updateCellReceive(segment, i, r, waiter)
        when {
            updCellResult === SUSPEND -> {
                return onSuspend(segment, i)
            }
            updCellResult === FAILED -> {
                return receiveImpl(
                    waiter = waiter,
                    onRendezvous = onRendezvous,
                    onSuspend = onSuspend,
                    onClosed = onClosed
                )
            }
            else -> {
                segment.cleanPrev()
                @Suppress("UNCHECKED_CAST")
                return onRendezvous(updCellResult as E)
            }
        }
    }

    private fun updateCellReceive(
        // The working cell is specified via
        // the segment and the index in it.
        segment: ChannelSegment<E>,
        i: Int,
        // The global index of the cell.
        r: Long,
        // The waiter to be stored in case of suspension.
        waiter: Any?,
    ): Any? {
        // Then, the cell state should be updated
        // according to the state machine.
        // See the paper mentioned in the very beginning for
        // the cell life-cycle diagram and the algorithm details.
        while (true) {
            // Read the current cell state.
            val state = segment.getState(i)
            when {
                // The cell is empty
                state === null || state === IN_BUFFER -> {
                    // If a rendezvous must happen, the operation does not wait
                    // until the cell stores a buffered element or a suspended
                    // sender, poisoning the cell and restarting instead.
                    // Otherwise, try to store the specified waiter in the cell.
                    val senders = sendersAndCloseStatus.value.counter
                    if (r < senders) {
                        // The cell is already covered by sender,
                        // so a rendezvous must happen. Unfortunately,
                        // the cell is empty, so the operation poisons it.
                        if (segment.casState(i, state, POISONED)) {
                            // When the cell becomes poisoned, it is essentially
                            // the same as storing an already cancelled receiver.
                            // Thus, the `expandBuffer()` procedure should be invoked.
                            expandBuffer()
                            return FAILED
                        }
                    } else {
                        // This `receive()` operation should suspend.
                        if (waiter === null) {
                            // The waiter is not specified;
                            // return the corresponding result.
                            return SUSPEND_NO_WAITER
                        }
                        // Try to install the waiter.
                        if (segment.casState(i, state, waiter)) {
                            // The waiter has been successfully installed.
                            // Invoke the `expandBuffer()` procedure and finish.
                            expandBuffer()
                            return SUSPEND
                        }
                    }
                }
                // The cell stores a buffered element.
                state === BUFFERED -> {
                    // Retrieve the buffered element.
                    val element = segment.retrieveElement(i)
                    // It is possible
//                    if (segment.getState(i) !== BUFFERED) {
//                        error("")
//                    }
                    expandBuffer()
                    return element
                }
                state === INTERRUPTED -> {
                    if (segment.casState(i, state, INTERRUPTED_SEND)) return FAILED
                }
                state === INTERRUPTED_EB -> {
                    expandBuffer()
                    return FAILED
                }
                state === INTERRUPTED_SEND -> return FAILED
                state === POISONED -> return FAILED
                state === CHANNEL_CLOSED -> return FAILED
                state === S_RESUMING_EB -> continue // spin-wait
                else -> {
                    if (segment.casState(i, state, S_RESUMING_RCV)) {
                        val helpExpandBuffer = state is WaiterEB
                        val sender = if (state is WaiterEB) state.waiter else state
                        if (sender.tryResumeSender()) {
                            segment.setState(i, DONE)
                            expandBuffer()
                            return segment.retrieveElement(i)
                        } else {
                            segment.setState(i, INTERRUPTED_SEND)
                            if (helpExpandBuffer) expandBuffer()
                            return FAILED
                        }
                    }
                }
            }
        }
    }

    /**
     * Tries to resume the suspended sender.
     */
    private fun Any.tryResumeSender(): Boolean = when (this) {
        is CancellableContinuation<*> -> { // suspended `send(e)` operation
            @Suppress("UNCHECKED_CAST")
            this as CancellableContinuation<Unit>
            tryResume0(Unit)
        }
        is SelectInstance<*> -> trySelect(clauseObject = this@BufferedChannel, result = Unit)
        is SendBroadcast -> cont.tryResume0(true) // // suspended `sendBroadcast(e)` operation
        else -> error("Unexpected waiter: $this")
    }

    // ################################
    // # The expandBuffer() procedure #
    // ################################

    private fun expandBuffer() {
        if (rendezvousOrUnlimited) return
        var segment = bufferEndSegment.value
        try_again@ while (true) {
            val b = bufferEnd.getAndIncrement()
            val s = sendersAndCloseStatus.value.counter
            val id = b / SEGMENT_SIZE
            if (s <= b) {
                if (segment.id < id) {
                    while (true) {
                        val ss = findSegmentBufferOrLast(id, segment)
                        if (bufferEndSegment.moveForward(ss)) return
                    }
                } // to avoid memory leaks
                return
            }
            val i = (b % SEGMENT_SIZE).toInt()
            if (segment.id != id) {
                segment = findSegmentBuffer(id, segment).let {
                    if (it.isClosed) return else it.segment
                }
            }
            if (segment.id != id) {
                if (receivers.value > b) return
                bufferEnd.compareAndSet(b + 1, segment.id * SEGMENT_SIZE)
                continue@try_again
            }
            if (updateCellExpandBuffer(segment, i, b)) return
        }
    }

    private fun findSegmentBufferOrLast(id: Long, startFrom: ChannelSegment<E>): ChannelSegment<E> {
        var cur: ChannelSegment<E> = startFrom
        while (cur.id < id) {
            cur = cur.next ?: break
        }
        while (cur.removed) {
            cur = cur.next ?: break
        }
        return cur
    }

    private fun updateCellExpandBuffer(
        segment: ChannelSegment<E>,
        i: Int,
        b: Long
    ): Boolean {
        while (true) {
            val state = segment.getState(i)
            when {
                state === null -> {
                    if (segment.casState(i, state, IN_BUFFER)) return true
                }
                state === BUFFERED || state === POISONED || state === DONE || state === CHANNEL_CLOSED -> return true
                state === S_RESUMING_RCV -> continue // spin wait
                state === INTERRUPTED -> {
                    if (b >= receivers.value) return false
                    if (segment.casState(i, state, INTERRUPTED_EB)) return true
                }
                state === INTERRUPTED_SEND -> return false
                else -> {
                    check(state is Waiter)
                    if (b < receivers.value) {
                        if (segment.casState(i, state, WaiterEB(waiter = state))) return true
                    } else {
                        if (segment.casState(i, state, S_RESUMING_EB)) {
                            return if (state.tryResumeSender()) {
                                segment.setState(i, BUFFERED)
                                true
                            } else {
                                segment.setState(i, INTERRUPTED_SEND)
                                false
                            }
                        }
                    }
                }
            }
        }
    }


    // #######################
    // ## Select Expression ##
    // #######################

    override val onSend: SelectClause2<E, BufferedChannel<E>>
        get() = SelectClause2Impl(
            clauseObject = this@BufferedChannel,
            regFunc = BufferedChannel<*>::registerSelectForSend as RegistrationFunction,
            processResFunc = BufferedChannel<*>::processResultSelectSend as ProcessResultFunction
        )

    protected open fun registerSelectForSend(select: SelectInstance<*>, element: Any?) {
        sendImpl(
            element = element as E,
            waiter = select,
            onRendezvousOrBuffered = { select.selectInRegistrationPhase(Unit) },
            onSuspend = { segm, i ->
                select.disposeOnCompletion {
                    segm.onCancellation(i, onUndeliveredElement, select.context)
                }
            },
            onClosed = {
                onUndeliveredElement?.callUndeliveredElement(element, select.context)
                select.selectInRegistrationPhase(CHANNEL_CLOSED)
            }
        )
    }

    private fun processResultSelectSend(ignoredParam: Any?, selectResult: Any?): Any? =
        if (selectResult === CHANNEL_CLOSED) throw sendException(getCloseCause())
        else this

    override val onReceive: SelectClause1<E>
        get() = SelectClause1Impl(
            clauseObject = this@BufferedChannel,
            regFunc = BufferedChannel<*>::registerSelectForReceive as RegistrationFunction,
            processResFunc = BufferedChannel<*>::processResultSelectReceive as ProcessResultFunction,
            onCancellationConstructor = onUndeliveredElementReceiveCancellationConstructor
        )

    override val onReceiveCatching: SelectClause1<ChannelResult<E>>
        get() = SelectClause1Impl(
            clauseObject = this@BufferedChannel,
            regFunc = BufferedChannel<*>::registerSelectForReceive as RegistrationFunction,
            processResFunc = BufferedChannel<*>::processResultSelectReceiveCatching as ProcessResultFunction,
            onCancellationConstructor = onUndeliveredElementReceiveCancellationConstructor
        )

    override val onReceiveOrNull: SelectClause1<E?>
        get() = SelectClause1Impl(
            clauseObject = this@BufferedChannel,
            regFunc = BufferedChannel<*>::registerSelectForReceive as RegistrationFunction,
            processResFunc = BufferedChannel<*>::processResultSelectReceiveOrNull as ProcessResultFunction,
            onCancellationConstructor = onUndeliveredElementReceiveCancellationConstructor
        )

    protected open fun registerSelectForReceive(select: SelectInstance<*>, ignoredParam: Any?) {
        receiveImpl(
            waiter = select,
            onRendezvous = { elem ->
                onReceiveSynchronizationCompletion()
                select.selectInRegistrationPhase(elem)
            },
            onSuspend = { segm, i ->
                onReceiveSynchronizationCompletion()
                onReceiveEnqueued()
                select.disposeOnCompletion { segm.onCancellation(i) }
            },
            onClosed = {
                onReceiveSynchronizationCompletion()
                select.selectInRegistrationPhase(CHANNEL_CLOSED)
            }
        )
    }

    private fun processResultSelectReceive(ignoredParam: Any?, selectResult: Any?): Any? =
        if (selectResult === CHANNEL_CLOSED) throw receiveException(getCloseCause())
        else selectResult

    private fun processResultSelectReceiveOrNull(ignoredParam: Any?, selectResult: Any?): Any? =
        if (selectResult === CHANNEL_CLOSED) {
            if (closeCause.value !== null) throw receiveException(getCloseCause())
            null
        } else selectResult

    private fun processResultSelectReceiveCatching(ignoredParam: Any?, selectResult: Any?): Any? =
        if (selectResult === CHANNEL_CLOSED) closed(closeCause.value as Throwable?)
        else success(selectResult as E)

    private val onUndeliveredElementReceiveCancellationConstructor: OnCancellationConstructor? = onUndeliveredElement?.let {
        { select: SelectInstance<*>, _: Any?, element: Any? ->
            { if (element !== CHANNEL_CLOSED) onUndeliveredElement.callUndeliveredElement(element as E, select.context) }
        }
    }

    // ##############################
    // ## Closing and Cancellation ##
    // ##############################

    /**
     * Indicates whether this channel is cancelled. In case it is cancelled,
     * it stores either an exception if it was cancelled with or `null` if
     * this channel was cancelled without error. Stores [NO_CLOSE_CAUSE] if this
     * channel is not cancelled.
     */
    private val closeCause = atomic<Any?>(NO_CLOSE_CAUSE)

    protected fun getCloseCause() = closeCause.value as Throwable?

    private fun receiveException(cause: Throwable?) =
        cause ?: ClosedReceiveChannelException(DEFAULT_CLOSE_MESSAGE)
    protected fun sendException(cause: Throwable?) =
        cause ?: ClosedSendChannelException(DEFAULT_CLOSE_MESSAGE)

    // Stores the close handler.
    private val closeHandler = atomic<Any?>(null)

    private fun markClosed(): Unit =
        sendersAndCloseStatus.update { cur ->
            when (cur.closeStatus) {
                CLOSE_STATUS_ACTIVE ->
                    constructSendersAndCloseStatus(cur.counter, CLOSE_STATUS_CLOSED)
                CLOSE_STATUS_CANCELLATION_STARTED ->
                    constructSendersAndCloseStatus(cur.counter, CLOSE_STATUS_CANCELLED)
                else -> return
            }
        }.also { check(closeCause.value is Throwable?) }

    private fun markCancelled(): Unit =
        sendersAndCloseStatus.update { cur ->
            constructSendersAndCloseStatus(cur.counter, CLOSE_STATUS_CANCELLED)
        }

    private fun markCancellationStarted(): Unit =
        sendersAndCloseStatus.update { cur ->
            if (cur.closeStatus == CLOSE_STATUS_ACTIVE)
                constructSendersAndCloseStatus(cur.counter, CLOSE_STATUS_CANCELLATION_STARTED)
            else return
        }

    private fun completeCloseOrCancel() {
        sendersAndCloseStatus.value.isClosedForSend0
    }

    override fun close(cause: Throwable?): Boolean = closeImpl(cause, false)

    protected open fun closeImpl(cause: Throwable?, cancel: Boolean): Boolean {
        if (cancel) markCancellationStarted()
        val closedByThisOperation = closeCause.compareAndSet(NO_CLOSE_CAUSE, cause)
        if (cancel) markCancelled() else markClosed()
        completeCloseOrCancel()
        return if (closedByThisOperation) {
            invokeCloseHandler()
            true
        } else false
    }

    private fun completeClose(sendersCur: Long) {
        val segm = closeQueue()
        removeWaitingRequests(segm, sendersCur)
        onClosedIdempotent()
    }

    private fun completeCancel(sendersCur: Long) {
        completeClose(sendersCur)
        removeRemainingBufferedElements()
    }

    private fun closeQueue(): ChannelSegment<E> {
        // Choose the last segment.
        var lastSegment = bufferEndSegment.value
        sendSegment.value.let {
            if (it.id > lastSegment.id) lastSegment = it
        }
        receiveSegment.value.let {
            if (it.id > lastSegment.id) lastSegment = it
        }
        // Close the linked list of segment for new segment addition
        // and return the last segment at the point of closing.
        return lastSegment.close()
    }

    private fun invokeCloseHandler() {
        val closeHandler = closeHandler.getAndUpdate {
            if (it === null) CLOSE_HANDLER_CLOSED
            else CLOSE_HANDLER_INVOKED
        } ?: return
        closeHandler as (cause: Throwable?) -> Unit
        val closeCause = closeCause.value as Throwable?
        closeHandler(closeCause)
    }

    override fun invokeOnClose(handler: (cause: Throwable?) -> Unit) {
        if (closeHandler.compareAndSet(null, handler)) {
            // Handler has been successfully set, finish the operation.
            return
        }
        // Either handler was set already or this channel is cancelled.
        // Read the value of [closeHandler] and either throw [IllegalStateException]
        // or invoke the handler respectively.
        when (val curHandler = closeHandler.value) {
            CLOSE_HANDLER_CLOSED -> {
                // In order to be sure that our handler is the only one, we have to change the
                // [closeHandler] value to `INVOKED`. If this CAS fails, another handler has been
                // executed and an [IllegalStateException] should be thrown.
                if (closeHandler.compareAndSet(CLOSE_HANDLER_CLOSED, CLOSE_HANDLER_INVOKED)) {
                    handler(closeCause.value as Throwable?)
                } else {
                    throw IllegalStateException("Another handler was already registered and successfully invoked")
                }
            }
            CLOSE_HANDLER_INVOKED -> {
                throw IllegalStateException("Another handler was already registered and successfully invoked")
            }
            else -> {
                throw IllegalStateException("Another handler was already registered: $curHandler")
            }
        }
    }

    /**
     * Invoked when channel is closed as the last action of [close] invocation.
     * This method should be idempotent and can be called multiple times.
     */
    protected open fun onClosedIdempotent() {}

    protected open fun onCancel(wasClosed: Boolean) {}

    final override fun cancel(cause: Throwable?): Boolean = cancelImpl(cause)
    final override fun cancel() { cancelImpl(null) }
    final override fun cancel(cause: CancellationException?) { cancelImpl(cause) }

    internal open fun cancelImpl(cause: Throwable?): Boolean {
        val cause = cause ?: CancellationException("Channel was cancelled")
        val wasClosed = closeImpl(cause, true)
        removeRemainingBufferedElements()
        onCancel(wasClosed)
        return wasClosed
    }

    private fun removeRemainingBufferedElements() {
        // clear buffer first, but do not wait for it in helpers
        val onUndeliveredElement = onUndeliveredElement
        var undeliveredElementException: UndeliveredElementException? = null // first cancel exception, others suppressed

        var segm: ChannelSegment<E> = sendSegment.value
        while (true) {
            segm = segm.next ?: break
        }
        while (true) {
            for (i in SEGMENT_SIZE - 1 downTo 0) {
                if (segm.id * SEGMENT_SIZE + i < receivers.value) return
                while (true) {
                    val state = segm.getState(i)
                    when {
                        state === BUFFERED -> if (segm.casState(i, state, CHANNEL_CLOSED)) {
                            if (segm.id * SEGMENT_SIZE + i < receivers.value) return
                            if (onUndeliveredElement != null) {
                                undeliveredElementException = onUndeliveredElement.callUndeliveredElementCatchingException(segm.retrieveElement(i), undeliveredElementException)
                            }
                            segm.onCancellation(i)
                            break
                        }
                        state === IN_BUFFER || state === null -> if (segm.casState(i, state, CHANNEL_CLOSED)) {
                            segm.onCancellation(i)
                            break
                        }
                        state is Waiter -> {
                            if (segm.casState(i, state, CHANNEL_CLOSED)) {
//                                if (onUndeliveredElement != null) {
//                                    undeliveredElementException = onUndeliveredElement.callUndeliveredElementCatchingException(segm.retrieveElement(i), undeliveredElementException)
//                                }
                                state.closeSender()
                                break
                            }
                        }
                        state is WaiterEB -> {
                            if (segm.casState(i, state, CHANNEL_CLOSED)) {
//                                if (onUndeliveredElement != null) {
//                                    undeliveredElementException = onUndeliveredElement.callUndeliveredElementCatchingException(segm.retrieveElement(i), undeliveredElementException)
//                                }
                                state.waiter.closeSender()
                                break
                            }
                        }
                        else -> break
                    }
                }
            }
            segm = segm.prev ?: break
        }
        undeliveredElementException?.let { throw it } // throw UndeliveredElementException at the end if there was one
    }

    private fun removeWaitingRequests(lastSegment: ChannelSegment<E>, sendersCur: Long) {
        var segm: ChannelSegment<E>? = lastSegment
        while (segm != null) {
            for (i in SEGMENT_SIZE - 1 downTo 0) {
                if (segm.id * SEGMENT_SIZE + i < sendersCur) return
                cell@while (true) {
                    val state = segm.getState(i)
                    when {
                        state === null || state === IN_BUFFER -> {
                            if (segm.casState(i, state, CHANNEL_CLOSED)) break@cell
                        }
                        state is WaiterEB -> {
                            if (segm.casState(i, state, CHANNEL_CLOSED)) {
                                if (state.waiter.closeReceiver()) expandBuffer()
                                break@cell
                            }
                        }
                        state is Waiter -> {
                            if (segm.casState(i, state, CHANNEL_CLOSED)) {
                                if (state.closeReceiver()) expandBuffer()
                                break@cell
                            }
                        }
                        else -> break@cell
                    }
                }
            }
            segm = segm.prev
        }
    }

    private fun Waiter.closeReceiver() = closeWaiter(receiver = true)
    private fun Any.closeSender() = closeWaiter(receiver = false)

    private fun Any.closeWaiter(receiver: Boolean): Boolean {
        val cause = getCloseCause()
        return when (this) {
            is SendBroadcast -> {
                this.cont.resume(false)
                true
            }
            is CancellableContinuation<*> -> {
                val exception = if (receiver) receiveException(cause) else sendException(cause)
                this.tryResumeWithException(exception)?.also { this.completeResume(it) }.let { it !== null }
            }
            is ReceiveCatching<*> -> {
                this.cont.tryResume(closed(cause))?.also { this.cont.completeResume(it) }.let { it !== null }
            }
            is BufferedChannel<*>.BufferedChannelIterator -> {
                this.tryResumeHasNextWithCloseException(cause)
                true
            }
            is SelectInstance<*> -> this.trySelect(this@BufferedChannel, CHANNEL_CLOSED)
            else -> error("Unexpected waiter: $this")
        }
    }


    // ######################
    // ## Iterator Support ##
    // ######################

    override fun iterator(): ChannelIterator<E> = BufferedChannelIterator()

    /**
     * The key idea is that an iterator is a special receiver type,
     * which should be resumed differently to [receive] and [onReceive]
     * operations, but can be served as a waiter in a way similar to
     * [CancellableContinuation] and [SelectInstance].
     *
     * Roughly, [hasNext] is a [receive] sibling, while [next] simply
     * returns the already retrieved element. From the implementation
     * side, [receiveResult] stores the element retrieved by [hasNext]
     * (or a special [ClosedChannel] token if the channel is closed).
     *
     * The [invoke] function is a [CancelHandler] implementation, the
     * implementation of which requires storing the [segment] and the
     * [index] in it that specify the location of the stored iterator.
     *
     * To resume the suspended [hasNext] call, a special [tryResumeHasNext]
     * function should be used in a way similar to [CancellableContinuation.tryResume]
     * and [SelectInstance.trySelect]. When the channel becomes closed,
     * [tryResumeHasNextWithCloseException] should be used instead.
     */
    protected open inner class BufferedChannelIterator : ChannelIterator<E>, BeforeResumeCancelHandler(), Waiter {
        private var receiveResult: Any? = NO_RECEIVE_RESULT

        private var cont: CancellableContinuation<Boolean>? = null

        private var segment: ChannelSegment<E>? = null
        private var index = -1

        // on cancellation
        override fun invoke(cause: Throwable?) {
            segment?.onCancellation(index)
            onReceiveDequeued()
        }

        override suspend fun hasNext(): Boolean = receiveImpl(
            waiter = null,
            onRendezvous = { element ->
                this.receiveResult = element
                onReceiveSynchronizationCompletion()
                true
            },
            onSuspend = { _, _ -> error("unreachable") },
            onClosed = { onCloseHasNext() },
            onNoWaiterSuspend = { segm, i, r -> hasNextOnNoWaiterSuspend(segm, i, r) }
        )

        private fun onCloseHasNext(): Boolean {
            val cause = getCloseCause()
            onReceiveSynchronizationCompletion()
            this.receiveResult = ClosedChannel(cause)
            if (cause == null) return false
            else throw recoverStackTrace(cause)
        }

        private suspend fun hasNextOnNoWaiterSuspend(
            segm: ChannelSegment<E>,
            i: Int,
            r: Long
        ): Boolean = suspendCancellableCoroutineReusable { cont ->
            this.cont = cont
            receiveImplOnNoWaiter(
                segm, i, r,
                waiter = this,
                onRendezvous = { element ->
                    this.receiveResult = element
                    this.cont = null
                    onReceiveSynchronizationCompletion()
                    cont.resume(true) {
                        onUndeliveredElement?.callUndeliveredElement(element, cont.context)
                    }
                },
                onSuspend = { segment, i ->
                    this.segment = segment
                    this.index = i
                    cont.invokeOnCancellation(this.asHandler)
                    onReceiveEnqueued()
                    onReceiveSynchronizationCompletion()
                },
                onClosed = {
                    this.cont = null
                    val cause = getCloseCause()
                    this.receiveResult = ClosedChannel(cause)
                    onReceiveSynchronizationCompletion()
                    if (cause == null) {
                        cont.resume(false)
                    } else {
                        cont.resumeWithException(recoverStackTrace(cause))
                    }
                }
            )
        }

        @Suppress("UNCHECKED_CAST")
        override fun next(): E {
            // Read the already received result, or [NO_RECEIVE_RESULT] if [hasNext] has not been invoked yet.
            check(receiveResult !== NO_RECEIVE_RESULT) { "`hasNext()` has not been invoked" }
            val result = receiveResult
            receiveResult = NO_RECEIVE_RESULT
            // Is this channel closed?
            if (result is ClosedChannel) throw recoverStackTrace(receiveException(result.cause))
            // Return the element.
            return result as E
        }

        fun tryResumeHasNext(element: E): Boolean {
            this.receiveResult = element
            val cont = this.cont!!
            this.cont = null
            return cont.tryResume(true, null, onUndeliveredElement?.bindCancellationFun(element, cont.context)).let {
                if (it !== null) {
                    cont.completeResume(it)
                    true
                } else false
            }
        }

        fun tryResumeHasNextWithCloseException(cause: Throwable?) {
            this.receiveResult = ClosedChannel(cause)
            val cont = cont!!
            if (cause == null) {
                cont.resume(false)
            } else {
                cont.resumeWithException(cause)
            }
        }
    }

    private class ClosedChannel(@JvmField val cause: Throwable?)

    // #################################################
    // # isClosedFor[Send,Receive] and isEmpty SUPPORT #
    // #################################################

    @ExperimentalCoroutinesApi
    override val isClosedForSend: Boolean
        get() = sendersAndCloseStatus.value.isClosedForSend0

    private val Long.isClosedForSend0 get() =
        isClosed(this, sendersCur = this.counter, isClosedForReceive = false)

    @ExperimentalCoroutinesApi
    override val isClosedForReceive: Boolean
        get() = sendersAndCloseStatus.value.isClosedForReceive0

    private val Long.isClosedForReceive0 get() =
        isClosed(this, sendersCur = this.counter, isClosedForReceive = true)

    private fun isClosed(
        sendersAndCloseStatusCur: Long,
        sendersCur: Long,
        isClosedForReceive: Boolean
    ) = when (sendersAndCloseStatusCur.closeStatus) {
        // This channel is active and has not been closed.
        CLOSE_STATUS_ACTIVE -> false
        // The cancellation procedure has been started but
        // not linearized yet, so this channel should be
        // considered as active.
        CLOSE_STATUS_CANCELLATION_STARTED -> false
        // This channel has been successfully closed.
        // Help to complete the closing procedure to
        // guarantee linearizability, and return `true`
        // for senders or the flag whether there still
        // exist elements to retrieve for receivers.
        CLOSE_STATUS_CLOSED -> {
            completeClose(sendersCur)
            // When `isClosedForReceive` is `false`, always return `true`.
            // Otherwise, it is possible that the channel is closed but
            // still has elements to retrieve.
            if (isClosedForReceive) !hasElements() else true
        }
        // This channel has been successfully cancelled.
        // Help to complete the cancellation procedure to
        // guarantee linearizability and return `true`.
        CLOSE_STATUS_CANCELLED -> {
            completeCancel(sendersCur)
            true
        }
        else -> error("unexpected close status: ${sendersAndCloseStatusCur.closeStatus}")
    }

    @ExperimentalCoroutinesApi
    override val isEmpty: Boolean get() =
        if (sendersAndCloseStatus.value.isClosedForReceive0) false
        else if (hasElements()) false
        else !sendersAndCloseStatus.value.isClosedForReceive0

    /**
     * Checks whether this channel contains elements to retrieve.
     * Unfortunately, simply comparing the counters is not sufficient,
     * as there can be cells in INTERRUPTED state due to cancellation.
     * Therefore, this function tries to fairly find the first element,
     * updating the `receivers` counter correspondingly.
     */
    internal fun hasElements(): Boolean {
        // Read the segment before accessing `receivers` counter.
        var segm = receiveSegment.value
        while (true) {
            // Is there a chance that this channel has elements?
            val r = receivers.value
            val s = sendersAndCloseStatus.value.counter
            if (s <= r) return false // no elements
            // Try to access the `r`-th cell.
            // Get the corresponding segment first.
            val id = r / SEGMENT_SIZE
            if (segm.id != id) {
                // Find the required segment, and retry the operation when
                // the segment with the specified id has not been found
                // due to be full of cancelled cells. Also, when the segment
                // has not been found and the channel is already closed,
                // complete with `false`.
                segm = findSegmentHasElements(id, segm).let {
                    if (it.isClosed) return false
                    if (it.segment.id != id) {
                        updateReceiversIfLower(it.segment.id * SEGMENT_SIZE)
                        null
                    } else it.segment
                } ?: continue
            }
            // Does the `r`-th cell contain waiting sender or buffered element?
            val i = (r % SEGMENT_SIZE).toInt()
            if (!isCellEmpty(segm, i, r)) return true
            // The cell is empty. Update `receivers` counter and try again.
            receivers.compareAndSet(r, r + 1)
        }
    }

    /**
     * Checks whether this cell contains a buffered element
     * or a waiting sender, returning `false` in this case.
     * Otherwise, if this cell is empty (due to waiter cancellation,
     * channel closing, or marking it as `POISONED`), the operation
     * returns `true`.
     */
    private fun isCellEmpty(
        segm: ChannelSegment<E>,
        i: Int, // the cell index in `segm`
        r: Long // the global cell index
    ): Boolean {
        // The logic is similar to `updateCellReceive` with the only difference
        // that this operation does not change the state and retrieve the element.
        // TODO: simplify the conditions and document them.
        while (true) {
            val state = segm.getState(i)
            when {
                state === null || state === IN_BUFFER -> {
                    if (segm.casState(i, state, POISONED)) {
                        expandBuffer()
                        return true
                    }
                }
                state === BUFFERED -> {
                    return false
                }
                state === INTERRUPTED -> return true
                state === INTERRUPTED_EB -> return true
                state === INTERRUPTED_SEND -> return true
                state === CHANNEL_CLOSED -> return true
                state === DONE -> return true
                state === POISONED -> return true
                state === S_RESUMING_EB || state === S_RESUMING_RCV -> continue // spin-wait
                else -> return receivers.value != r
            }
        }
    }

    // #######################
    // # SEGMENTS MANAGEMENT #
    // #######################

    private fun findSegmentSend(id: Long, start: ChannelSegment<E>) =
        sendSegment.findSegmentAndMoveForward(id, start, ::createSegment).let {
            if (it.isClosed) {
                completeCloseOrCancel()
                null
            } else {
                val segm = it.segment
                if (segm.id != id) {
                    assert { segm.id > id }
                    updateSendersIfLower(segm.id * SEGMENT_SIZE)
                    null
                } else segm
            }
        }

    private fun updateSendersIfLower(value: Long): Unit =
        sendersAndCloseStatus.loop { cur ->
            val curCounter = cur.counter
            if (curCounter >= value) return
            val update = constructSendersAndCloseStatus(curCounter, cur.closeStatus)
            if (sendersAndCloseStatus.compareAndSet(cur, update)) return
        }

    private fun updateReceiversIfLower(value: Long): Unit =
        receivers.loop { cur ->
            if (cur >= value) return
            if (receivers.compareAndSet(cur, value)) return
        }

    private fun findSegmentReceive(id: Long, start: ChannelSegment<E>) =
        receiveSegment.findSegmentAndMoveForward(id, start, ::createSegment).let {
            if (it.isClosed) {
                completeCloseOrCancel()
                null
            } else {
                val segm = it.segment
                if (segm.id != id) {
                    assert { segm.id > id }
                    updateReceiversIfLower(segm.id * SEGMENT_SIZE)
                    null
                } else segm
            }
        }

    private fun findSegmentHasElements(id: Long, start: ChannelSegment<E>) =
        receiveSegment.findSegmentAndMoveForward(id, start, ::createSegment)

    private fun findSegmentBuffer(id: Long, start: ChannelSegment<E>) =
        bufferEndSegment.findSegmentAndMoveForward(id, start, ::createSegment)

    // ##################
    // # FOR DEBUG INFO #
    // ##################

    internal val receiversCounter: Long get() = receivers.value
    internal val sendersCounter: Long get() = sendersAndCloseStatus.value.counter

    // Returns a debug representation of this channel,
    // which we actively use in Lincheck tests.
    override fun toString(): String {
        val data = arrayListOf<String>()
        val head = if (receiveSegment.value.id < sendSegment.value.id) receiveSegment.value else sendSegment.value
        var cur = head
        while (true) {
            repeat(SEGMENT_SIZE) { i ->
                val w = cur.getState(i)
                val e = cur.getElement(i)
                val wString = when (w) {
                    is CancellableContinuation<*> -> "cont"
                    is SelectInstance<*> -> "select"
                    is ReceiveCatching<*> -> "receiveCatching"
                    is SendBroadcast -> "send(broadcast)"
                    else -> w.toString()
                }
                val eString = e.toString()
                data += "($wString,$eString)"
            }
            cur = cur.next ?: break
        }
        var dataStartIndex = head.id * SEGMENT_SIZE
        while (data.isNotEmpty() && data.first() == "(null,null)") {
            data.removeFirst()
            dataStartIndex++
        }
        while (data.isNotEmpty() && data.last() == "(null,null)") data.removeLast()
        return "S=${sendersAndCloseStatus.value.counter},R=${receivers.value},B=${bufferEnd.value}," +
               "C=${sendersAndCloseStatus.value.closeStatus},data=${data},dataStartIndex=$dataStartIndex," +
               "S_SegmId=${sendSegment.value.id},R_SegmId=${receiveSegment.value.id},B_SegmId=${bufferEndSegment.value?.id}"
    }
}

/**
 * The channel is represented as a list of segments, which simulates an infinite array.
 * Each segment has its own [id], which increase from the beginning. These [id]s help
 * to update [BufferedChannel.sendSegment], [BufferedChannel.receiveSegment],
 * and [BufferedChannel.bufferEndSegment] correctly.
 */
internal class ChannelSegment<E>(id: Long, prev: ChannelSegment<E>?, pointers: Int) : Segment<ChannelSegment<E>>(id, prev, pointers) {
    private val data = atomicArrayOfNulls<Any?>(SEGMENT_SIZE * 2) // 2 registers per slot: state + element
    override val numberOfSlots: Int get() = SEGMENT_SIZE

    // ########################################
    // # Manipulation with the Element Fields #
    // ########################################

    inline fun storeElement(index: Int, element: E) {
        setElementLazy(index, element)
    }

    @Suppress("UNCHECKED_CAST")
    inline fun getElement(index: Int) = data[index * 2].value as E

    inline fun retrieveElement(index: Int): E = getElement(index).also { cleanElement(index) }

    inline fun cleanElement(index: Int) {
        setElementLazy(index, null)
    }

    private inline fun setElementLazy(index: Int, value: Any?) {
        data[index * 2].lazySet(value)
    }

    // ######################################
    // # Manipulation with the State Fields #
    // ######################################

    inline fun getState(index: Int): Any? = data[index * 2 + 1].value

    inline fun setState(index: Int, value: Any?) {
        data[index * 2 + 1].value = value
    }

    inline fun setStateLazy(index: Int, value: Any?) {
        data[index * 2 + 1].lazySet(value)
    }

    inline fun casState(index: Int, from: Any?, to: Any?) = data[index * 2 + 1].compareAndSet(from, to)

    // ########################
    // # Cancellation Support #
    // ########################

    fun onCancellation(index: Int, onUndeliveredElement: OnUndeliveredElement<E>? = null, context: CoroutineContext? = null) {
        // Update the cell state first.
        while (true) {
            val cur = data[index * 2 + 1].value
            val update = when {
                cur is Waiter -> INTERRUPTED
                cur is WaiterEB -> INTERRUPTED_EB
                cur === S_RESUMING_EB -> continue
                cur === S_RESUMING_RCV -> continue
                cur === INTERRUPTED_SEND -> INTERRUPTED_SEND
                cur === DONE || cur === BUFFERED || cur === CHANNEL_CLOSED -> return
                else -> error("unexpected: $cur")
            }
            if (data[index * 2 + 1].compareAndSet(cur, update)) break
        }
        // Call `onUndeliveredElement` if required.
        onUndeliveredElement?.callUndeliveredElement(getElement(index), context!!)
        // Clean the element slot.
        cleanElement(index)
        // Inform the segment that one more slot has been cleaned.
        onSlotCleaned()
    }
}
private fun <E> createSegment(id: Long, prev: ChannelSegment<E>?) = ChannelSegment(id, prev, 0)
private val NULL_SEGMENT = createSegment<Any?>(-1, null)
/**
 * Number of cells in each segment.
 */
private val SEGMENT_SIZE = systemProp("kotlinx.coroutines.bufferedChannel.segmentSize", 32)

/**
 * Tries to resume this continuation with the specified
 * value. Returns `true` on success and `false` on failure.
 */
private fun <T> CancellableContinuation<T>.tryResume0(
    value: T,
    onCancellation: ((cause: Throwable) -> Unit)? = null
): Boolean =
    tryResume(value, null, onCancellation).let { token ->
        if (token != null) {
            completeResume(token)
            true
        } else false
    }

/*
  When the channel is rendezvous or unlimited, the `bufferEnd` counter
  should be initialized with the corresponding value below and never change.
  In this case, the `expandBuffer(..)` operation does nothing.
 */
private const val BUFFER_END_RENDEZVOUS = 0L // no buffer
private const val BUFFER_END_UNLIMITED = Long.MAX_VALUE // infinite buffer
private fun initialBufferEnd(capacity: Int): Long = when (capacity) {
    Channel.RENDEZVOUS -> BUFFER_END_RENDEZVOUS
    Channel.UNLIMITED -> BUFFER_END_UNLIMITED
    else -> capacity.toLong()
}

/*
  Cell states. The initial "empty" state is represented
  with `null`, and waiting operations are represented with
  [Waiter] instances. Please see the [BufferedChannel]
  documentation for more details.
 */

// The cell stores a buffered element.
private val BUFFERED = Symbol("BUFFERED")
// Concurrent `expandBuffer(..)` can inform the
// upcoming sender that it should buffer the element.
private val IN_BUFFER = Symbol("SHOULD_BUFFER")
// Indicates that a receiver (R suffix) is resuming
// the suspended sender; after that, it should update
// the state to either `DONE` (on success) or
// `INTERRUPTED_R` (on failure).
private val S_RESUMING_RCV = Symbol("RESUMING_R")
// Indicates that `expandBuffer(..)` is resuming the
// suspended sender; after that, it should update the
// state to either `BUFFERED` (on success) or
// `INTERRUPTED_EB` (on failure).
private val S_RESUMING_EB = Symbol("RESUMING_EB")
// When a receiver comes to the cell already covered by
// a sender (according to the counters), but the cell
// is still in `EMPTY` or `IN_BUFFER` state, it poisons
// the cell by changing its state to `POISONED`.
private val POISONED = Symbol("POISONED")
// When the element is successfully transferred (possibly,
// through buffering), the cell moves to `DONE` state.
private val DONE = Symbol("DONE")
// When the waiter is cancelled, it moves the cell to
// `INTERRUPTED` state; thus, informing other parties
// that may come to the cell and avoiding memory leaks.
private val INTERRUPTED = Symbol("INTERRUPTED")
// TODO
private val INTERRUPTED_SEND = Symbol("INTERRUPTED_SEND")
// When the cell is already covered by both sender and
// receiver (`sender` and `receivers` counters are greater
// than the cell number), the `expandBuffer(..)` procedure
// cannot distinguish which kind of operation is stored
// in the cell. Thus, it wraps the waiter with this descriptor,
// informing the possibly upcoming receiver that it should
// complete the `expandBuffer(..)` procedure if the waiter
// stored in the cell is sender. In turn, senders ignore this
// information.
private class WaiterEB(@JvmField val waiter: Waiter) {
    override fun toString() = "WaiterEB($waiter)"
}
// Similarly to the situation described for [WaiterEB],
// the `expandBuffer(..)` procedure cannot distinguish
// whether sender or receiver was stored in the cell when
// its state is `INTERRUPTED`. Thus, it updates the state
// to `INTERRUPTED_EB`, informing the possibly upcoming
// receiver that it should complete the `expandBuffer(..)`
// procedure if the cancelled waiter stored in the cell
// was sender.
private val INTERRUPTED_EB = Symbol("INTERRUPTED_EB")
// Indicates that the channel is already closed,
// and no more operation should not touch this cell.
internal val CHANNEL_CLOSED = Symbol("CHANNEL_CLOSED")


/**
 * To distinguish suspended [BufferedChannel.receive] and
 * [BufferedChannel.receiveCatching] operation, the last
 * is wrapped with this class.
 */
private class ReceiveCatching<E>(
    @JvmField val cont: CancellableContinuation<ChannelResult<E>>
) : Waiter

/*
  Internal results for [BufferedChannel.updateCellReceive].
  On successful rendezvous with waiting sender or
  buffered element retrieval, the corresponding element
  is returned as result of [BufferedChannel.updateCellReceive].
 */
private val SUSPEND = Symbol("SUSPEND")
private val SUSPEND_NO_WAITER = Symbol("SUSPEND_NO_WAITER")
private val FAILED = Symbol("FAILED")

/*
  Internal results for [BufferedChannel.updateCellSend]
 */
private const val RESULT_RENDEZVOUS = 0
private const val RESULT_BUFFERED = 1
private const val RESULT_SUSPEND = 2
private const val RESULT_SUSPEND_NO_WAITER = 3
private const val RESULT_FAILED = 4

/**
 * Special value for [BufferedChannel.BufferedChannelIterator.receiveResult]
 * that indicates the absence of pre-received result.
 */
private val NO_RECEIVE_RESULT = Symbol("NO_RECEIVE_RESULT")


/*
  The channel close statuses. The transition scheme is the following:
    +--------+   +----------------------+   +-----------+
    | ACTIVE |-->| CANCELLATION_STARTED |-->| CANCELLED |
    +--------+   +----------------------+   +-----------+
        |                                         ^
        |             +--------+                  |
        +------------>| CLOSED |------------------+
                      +--------+
  We need `CANCELLATION_STARTED` to synchronize
  concurrent closing and cancellation.
 */
private const val CLOSE_STATUS_ACTIVE = 0
private const val CLOSE_STATUS_CANCELLATION_STARTED = 1
private const val CLOSE_STATUS_CLOSED = 2
private const val CLOSE_STATUS_CANCELLED = 3

/*
  The `senders` counter and the channel close status
  are stored in a single 64-bit register to save the space
  and reduce the number of reads in sending operations.
  The code below encapsulates the required bit arithmetics.
 */
private const val CLOSE_STATUS_SHIFT = 60
private const val COUNTER_MASK = (1L shl CLOSE_STATUS_SHIFT) - 1
private inline val Long.counter get() = this and COUNTER_MASK
private inline val Long.closeStatus: Int get() = (this shr CLOSE_STATUS_SHIFT).toInt()
private inline fun constructSendersAndCloseStatus(counter: Long, closeStatus: Int): Long =
    (closeStatus.toLong() shl CLOSE_STATUS_SHIFT) + counter

/*
  As [BufferedChannel.invokeOnClose] can be invoked concurrently
  with channel closing, we have to synchronize them. These two
  markers help with the synchronization. The resulting
  [BufferedChannel.closeHandler] state diagram is presented below:

    +------+ install handler +---------+  close  +---------+
    | null |---------------->| handler |-------->| INVOKED |
    +------+                 +---------+         +---------+
       |             +--------+
       +------------>| CLOSED |
           close     +--------+
 */
private val CLOSE_HANDLER_CLOSED = Symbol("CLOSE_HANDLER_CLOSED")
private val CLOSE_HANDLER_INVOKED = Symbol("CLOSE_HANDLER_INVOKED")

/**
 * Specifies the absence of closing cause, stored in [BufferedChannel.closeCause].
 * When the channel is closed or cancelled without exception, this [NO_CLOSE_CAUSE]
 * marker should be replaced with `null`.
 */
private val NO_CLOSE_CAUSE = Symbol("NO_CLOSE_CAUSE")

/**
 * All waiters, such as [CancellableContinuationImpl], [SelectInstance], and
 * [BufferedChannel.BufferedChannelIterator], should be marked with this interface
 * to make the code faster and easier to read.
 */
internal interface Waiter