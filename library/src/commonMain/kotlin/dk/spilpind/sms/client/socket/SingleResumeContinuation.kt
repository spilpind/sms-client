package dk.spilpind.sms.client.socket

import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.flow.MutableStateFlow
import kotlin.coroutines.resume

/**
 * Wraps [continuation] such that it is only ever resumed once, no matter how many times (and from how many coroutines)
 * [resume] is called.
 *
 * This is needed because the ownership of a request continuation is shared: the coroutine waiting for a request to be
 * handed over to the web socket might be cancelled and thus want to resume the continuation itself at the very same
 * time as whoever received the request resumes it after having sent it. Resuming the same continuation twice throws an
 * IllegalStateException ("Already resumed") and as that happens inside a launch it would crash the app instead of just
 * failing the request
 */
internal class SingleResumeContinuation(private val continuation: CancellableContinuation<Unit>) {

    // A state flow is used as it gives us an atomic compare-and-set on all platforms without having to pull in any
    // additional dependencies
    private val isResumed = MutableStateFlow(false)

    /**
     * Invokes [handler] in case the underlying continuation is cancelled. Note that this, just like for the
     * continuation itself, can only be called once
     */
    fun invokeOnCancellation(handler: (cause: Throwable?) -> Unit) {
        continuation.invokeOnCancellation(handler)
    }

    /**
     * Resumes the underlying continuation, unless it was already resumed - in which case this does nothing. Note that
     * this deliberately returns nothing: a return value would make the type of a select clause calling this depend on
     * it, which is surprising in a function that exists to be called from wherever
     */
    fun resume() {
        if (!isResumed.compareAndSet(expect = false, update = true)) {
            return
        }

        continuation.resume(Unit)
    }
}
