package dk.spilpind.sms.client.socket

import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.launch
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SingleResumeContinuationTest {

    @Test
    fun resumeResumesTheContinuation() = runTest {
        var resumeCount = 0

        launch {
            suspendCancellableCoroutine { rawContinuation: CancellableContinuation<Unit> ->
                assertTrue(
                    SingleResumeContinuation(rawContinuation).resume(),
                    message = "Expected the continuation to be resumed"
                )
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
                assertTrue(continuation.resume(), message = "Expected the first resume to resume the continuation")
                assertFalse(continuation.resume(), message = "Did not expect the second resume to do anything")
                assertFalse(continuation.resume(), message = "Did not expect the third resume to do anything")
            }

            ++resumeCount
        }.join()

        assertEquals(1, resumeCount, message = "Expected the coroutine to continue exactly once")
    }
}
