/*
 * Copyright 2016-2022 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

@file:JvmMultifileClass
@file:JvmName("FlowKt")

package kotlinx.coroutines.flow

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.flow.internal.*
import kotlin.coroutines.*
import kotlin.jvm.*

//TODO: naming is hard

//TODO: may be fusibleFlow can be made public, so this implementation will go in rsocket-kotlin instead of coroutines?

//TODO: can we allow somehow to expose installed onElementDrop action to custom operators?

public inline fun <reified T> Flow<T>.onCurrentElementDrop(crossinline action: (T) -> Unit): Flow<T> =
    onAnyElementDrop { if (it is T) action(it) }

public inline fun <T, reified R> Flow<T>.onOtherElementDrop(crossinline action: (R) -> Unit): Flow<T> =
    onAnyElementDrop { if (it is R) action(it) }

public fun <T> Flow<T>.onAnyElementDrop(action: (Any?) -> Unit): Flow<T> {
    if (this is FusibleFlow) {
        //TODO: we can just fuse action, and call action in ChannelFlow both for channel and optional channel cases
    }

    //TODO: this can also return fusible flow
    return unsafeFlow {
        val newElementDrop = when (val oldElementDrop = currentCoroutineContext().onElementDropAction()) {
            null -> action
            else -> {
                {
                    oldElementDrop(it)
                    action(it)
                }
            }
        }
        emitAllWithElementDrop(flowOn(ElementDropContextElement(newElementDrop)), action)
    }
}

internal fun CoroutineContext.onElementDropAction(): ((Any?) -> Unit)? {
    return get(ElementDropContextElement)?.action
}

internal suspend fun <T> FlowCollector<T>.emitAllWithElementDrop(flow: Flow<T>, action: (Any?) -> Unit) {
    ElementDropFlowCollector(action, this).collect(flow)
}

/*
Same as this code, but don't need to box `var`:

var last: Any? = NULL
try {
    flow.collect { value ->
        last = value
        emit(value)
        last = NULL
    }
} catch (cause: Throwable) {
    if (last != NULL) action(last)
    throw cause
}
*/
private class ElementDropFlowCollector<T>(
    private val action: (Any?) -> Unit,
    private val collector: FlowCollector<T>
) : FlowCollector<T> {
    private var last: Any? = NULL
    override suspend fun emit(value: T) {
        last = value
        collector.emit(value)
        last = NULL
    }

    suspend inline fun collect(flow: Flow<T>) {
        try {
            emitAll(flow)
        } catch (cause: CancellationException) { //TODO: or cause should be Throwable?
            if (last != NULL) action(last)
            throw cause
        }
    }
}

private class ElementDropContextElement(
    val action: (Any?) -> Unit
) : CoroutineContext.Element {
    override val key: CoroutineContext.Key<*> get() = Key

    companion object Key : CoroutineContext.Key<ElementDropContextElement>
}
