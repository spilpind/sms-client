package dk.spilpind.sms.client.socket

import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.launch
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals

class SingleResumeContinuationTest {

    @Test
    fun resumeResumesTheContinuation() = runTest {
        var resumeCount = 0

        launch {
            suspendCancellableCoroutine { rawContinuation: CancellableContinuation<Unit> ->
                SingleResumeContinuation(rawContinuation).resume()
            }

            ++resumeCount
        }.join()

        assertEquals(1, resumeCount, message = "Expected the coroutine to continue exactly once")
    }

    @Test
    fun resumeOnlyResumesTheContinuationOnce() = runTest {
        var resumeCount = 0

        launch {
            suspendCancellableCoroutine { rawContinuation: CancellableContinuation<Unit> ->
                val continuation = SingleResumeContinuation(rawContinuation)

                // Without the single-resume guarantee the calls after the first one would throw an
                // IllegalStateException ("Already resumed"), which is what used to crash the app
                continuation.resume()
                continuation.resume()
                continuation.resume()
            }

            ++resumeCount
        }.join()

        assertEquals(1, resumeCount, message = "Expected the coroutine to continue exactly once")
    }
}
