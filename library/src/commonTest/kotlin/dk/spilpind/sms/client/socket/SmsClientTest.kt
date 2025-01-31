package dk.spilpind.sms.client.socket

import dk.spilpind.sms.client.socket.util.TestSocketConnection
import dk.spilpind.sms.api.Request
import dk.spilpind.sms.api.RequestSerializerInterceptor
import dk.spilpind.sms.api.Response
import dk.spilpind.sms.api.ResponseSerializerInterceptor
import dk.spilpind.sms.api.action.ReactionData
import dk.spilpind.sms.api.action.TeamAction
import dk.spilpind.sms.api.action.TournamentAction
import dk.spilpind.sms.api.action.TournamentReaction
import dk.spilpind.sms.api.common.Reaction
import dk.spilpind.sms.api.core.Status
import dk.spilpind.sms.core.model.Context
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.headersOf
import io.ktor.utils.io.ByteReadChannel
import io.ktor.websocket.Frame
import io.ktor.websocket.readText
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ChannelResult
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.toCollection
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * Note that a lot of tests are using 100ms as a threshold for delay/timeouts. This could probably be lowered, but we
 * thought it didn't matter too much (100ms+/- is an okay slack in this case) and thus it didn't make sense to test how
 * low we could go
 */
@OptIn(ExperimentalCoroutinesApi::class)
class SmsClientTest {

    private val testDispatcher = StandardTestDispatcher()

    private val mockEngine = MockEngine.create {

        // If we use default runCurrent() won't work
        dispatcher = testDispatcher

        requestHandlers.add {
            respond(
                content = ByteReadChannel(
                    Json.encodeToString(
                        Status(
                            type = Status.Type.Open,
                            localizedMessage = null
                        )
                    )
                ),
                status = HttpStatusCode.OK,
                headers = headersOf(HttpHeaders.ContentType, "application/json")
            )
        }
    }

    @Test
    fun sendReturnsSuccessfullyOnCorrectMessage() = runTest(testDispatcher) {
        val socketConnection = TestSocketConnection()

        val client = createSmsClient(
            scope = backgroundScope,
            socketConnection = socketConnection
        )

        var result: RequestResult<ReactionData>? = null
        launch {
            result = client.send(TournamentAction.Add("dummy"))
        }

        runCurrent()
        assertEquals(1, socketConnection.startCount, message = "Expected socket connection created at this point")
        val outgoingFrameResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(outgoingFrameResult.isSuccess, message = "Expected message")

        val addRequest = extractRequest(outgoingFrameResult)
        assertEquals(TournamentAction.Add("dummy"), addRequest.data)
        val actionId = addRequest.actionId

        assertNull(result)

        val wrongResponseWithNullActionId = Response(
            context = Context.Tournament.contextKey,
            reaction = Reaction.Added.reactionKey,
            actionId = null,
            data = TournamentReaction.Added(
                tournament = TournamentReaction.Tournament(
                    tournamentId = 0,
                    name = "dummy-tournament",
                    isPublic = false,
                    tags = listOf(),
                )
            )
        )

        socketConnection.clientIncoming.sendTestResponse(wrongResponseWithNullActionId)
        runCurrent()
        assertNull(result)

        val wrongResponseWithWrongActionId = Response(
            context = Context.Tournament.contextKey,
            reaction = Reaction.Added.reactionKey,
            actionId = "wrongId",
            data = TournamentReaction.Added(
                tournament = TournamentReaction.Tournament(
                    tournamentId = 0,
                    name = "dummy-tournament",
                    isPublic = false,
                    tags = listOf(),
                )
            )
        )

        socketConnection.clientIncoming.sendTestResponse(wrongResponseWithWrongActionId)
        runCurrent()
        assertNull(result)

        val correctResponse = Response(
            context = Context.Tournament.contextKey,
            reaction = Reaction.Added.reactionKey,
            actionId = actionId,
            data = TournamentReaction.Added(
                tournament = TournamentReaction.Tournament(
                    tournamentId = 0,
                    name = "dummy-tournament",
                    isPublic = false,
                    tags = listOf("tigtag"),
                )
            )
        )

        socketConnection.clientIncoming.sendTestResponse(correctResponse)
        runCurrent()

        val finalResult = result
        assertIs<RequestResult.Result<ReactionData>>(finalResult)
        assertEquals(
            TournamentReaction.Added(
                tournament = TournamentReaction.Tournament(
                    tournamentId = 0,
                    name = "dummy-tournament",
                    isPublic = false,
                    tags = listOf("tigtag"),
                )
            ),
            finalResult.data
        )

        val remainingOutgoingResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(remainingOutgoingResult.isFailure, message = "Did not expect any remaining messages")
        assertEquals(1, socketConnection.startCount, message = "Did not expect more created socket connections")
    }

    @Test
    fun sendReturnsSuccessfullyOnMessageReturnedRightAfterSendReceivedWithOtherSubscriber() =
        sendReturnsSuccessfullyOnMessageReturnedRightAfterSendReceived(otherSubscriber = true)

    @Test
    fun sendReturnsSuccessfullyOnMessageReturnedRightAfterSendReceivedWithoutOtherSubscriber() =
        sendReturnsSuccessfullyOnMessageReturnedRightAfterSendReceived(otherSubscriber = false)

    private fun sendReturnsSuccessfullyOnMessageReturnedRightAfterSendReceived(
        otherSubscriber: Boolean
    ) = runTest(testDispatcher) {
        val socketConnection = TestSocketConnection()

        val client = createSmsClient(
            scope = backgroundScope,
            socketConnection = socketConnection
        )

        if (otherSubscriber) {
            // This is to make sure some other part is also receiving messages and thus might "discard" messages
            // before they are able to get to the real receiver below
            backgroundScope.launch {
                client.subscribe(
                    subscribeAction = { TournamentAction.Subscribe() },
                    unsubscribeAction = { TournamentAction.Unsubscribe() }
                ).collect()
            }
        }

        runCurrent()

        if (otherSubscriber) {
            assertEquals(
                1,
                socketConnection.startCount,
                message = "Expected socket connection created at this point"
            )

            // Start dummy subscription
            val subscriptionMessage = socketConnection.clientOutgoing.tryReceive()
            assertTrue(subscriptionMessage.isSuccess, message = "Expected subscription message")
        }

        // Wait for the send message and respond right away
        launch {
            val outgoingFrameResult = socketConnection.clientOutgoing.receive()
            val outgoingRequest = extractRequest(outgoingFrameResult)
            assertEquals(TournamentAction.Add("dummy"), outgoingRequest.data)

            val correctResponse = Response(
                context = Context.Tournament.contextKey,
                reaction = Reaction.Added.reactionKey,
                actionId = outgoingRequest.actionId,
                data = TournamentReaction.Added(
                    tournament = TournamentReaction.Tournament(
                        tournamentId = 0,
                        name = "dummy-tournament",
                        isPublic = false,
                        tags = listOf("taggy"),
                    )
                )
            )

            socketConnection.clientIncoming.sendTestResponse(correctResponse)
        }

        runCurrent() // Make sure the dummy-response job is ready before we call send

        val result = client.send(TournamentAction.Add("dummy"))
        assertEquals(1, socketConnection.startCount, message = "Expected socket connection created at this point")
        assertIs<RequestResult.Result<ReactionData>>(result)
        assertEquals(
            TournamentReaction.Added(
                tournament = TournamentReaction.Tournament(
                    tournamentId = 0,
                    name = "dummy-tournament",
                    isPublic = false,
                    tags = listOf("taggy"),
                )
            ),
            result.data
        )

        val remainingOutgoingResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(remainingOutgoingResult.isFailure, message = "Did not expect any remaining messages")
        assertEquals(1, socketConnection.startCount, message = "Did not expect more created socket connections")
    }

    @Test
    fun sendReturnsSuccessfullyOnMessageReturnedBeforeSendReceivedWithOtherSubscriber() =
        sendReturnsSuccessfullyOnMessageReturnedBeforeSendReceived(otherSubscriber = true)

    @Test
    fun sendReturnsSuccessfullyOnMessageReturnedBeforeSendReceivedWithoutOtherSubscriber() =
        sendReturnsSuccessfullyOnMessageReturnedBeforeSendReceived(otherSubscriber = false)

    private fun sendReturnsSuccessfullyOnMessageReturnedBeforeSendReceived(
        otherSubscriber: Boolean
    ) = runTest(testDispatcher) {
        val socketConnection = TestSocketConnection()

        val client = createSmsClient(
            scope = backgroundScope,
            socketConnection = socketConnection
        )

        if (otherSubscriber) {
            backgroundScope.launch {
                // This is to make sure some other part is also receiving messages and thus might "discard" messages
                // before they are able to get to the real receiver below
                client.subscribe(
                    subscribeAction = { TournamentAction.Subscribe() },
                    unsubscribeAction = { TournamentAction.Unsubscribe() }
                ).collect()
            }
        }

        runCurrent()

        if (otherSubscriber) {
            assertEquals(
                1,
                socketConnection.startCount,
                message = "Expected socket connection created at this point"
            )

            // Start dummy subscription
            val subscriptionMessage = socketConnection.clientOutgoing.tryReceive()
            assertTrue(subscriptionMessage.isSuccess, message = "Expected subscription message")
        }

        var result: RequestResult<ReactionData>? = null
        launch {
            result = client.send(TournamentAction.Add("dummy"))
        }

        runCurrent()
        assertEquals(1, socketConnection.startCount, message = "Expected socket connection created at this point")

        val correctResponse = Response(
            context = Context.Tournament.contextKey,
            reaction = Reaction.Added.reactionKey,
            actionId = if (otherSubscriber) "2" else "1", // If this ever change we just need to update it
            data = TournamentReaction.Added(
                tournament = TournamentReaction.Tournament(
                    tournamentId = 0,
                    name = "dummy-tournament",
                    isPublic = false,
                    tags = listOf("tags-some"),
                )
            )
        )

        // Fake response sent after send has started, but before the outgoing message is even received
        socketConnection.clientIncoming.sendTestResponse(correctResponse)
        runCurrent()
        assertNull(result)

        val outgoingFrameResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(outgoingFrameResult.isSuccess, message = "Expected message")

        runCurrent()
        val finalResult = result
        assertIs<RequestResult.Result<ReactionData>>(finalResult)
        assertEquals(
            TournamentReaction.Added(
                tournament = TournamentReaction.Tournament(
                    tournamentId = 0,
                    name = "dummy-tournament",
                    isPublic = false,
                    tags = listOf("tags-some"),
                )
            ),
            finalResult.data
        )

        val remainingOutgoingResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(remainingOutgoingResult.isFailure, message = "Did not expect any remaining messages")
        assertEquals(1, socketConnection.startCount, message = "Did not expect more created socket connections")
    }

    @Test
    fun sendReturnsErrorIfWebSocketCannotStart() = runTest(testDispatcher) {
        val client = SmsClient(
            scope = backgroundScope,
            useBetaEndpoint = false,
            createSocket = createSocket@{ _, _ ->
                throw IllegalArgumentException("This is just a random picked exception type")
            },
            engine = mockEngine
        )

        val result = client.send(TournamentAction.Add("dummy"))

        assertIs<RequestResult.Error>(result)
    }

    @Test
    fun sendTimesOutAfter20secondsIfSocketTakesTooLongToStart() = runTest(testDispatcher) {
        val client = SmsClient(
            scope = backgroundScope,
            useBetaEndpoint = false,
            createSocket = createSocket@{ _, _ ->
                delay(100_000)
            },
            engine = mockEngine
        )

        var result: RequestResult<ReactionData>? = null
        backgroundScope.launch {
            result = client.send(TournamentAction.Add("dummy"))
        }

        assertNull(result)
        advanceTimeBy(19900)
        assertNull(result)
        advanceTimeBy(200)
        assertIs<RequestResult.Error>(result)
    }

    @Test
    fun sendTimesOutAfter20secondsIfMessageDoesNotGetSent() = runTest(testDispatcher) {
        val socketConnection = TestSocketConnection()

        val client = createSmsClient(
            scope = backgroundScope,
            socketConnection = socketConnection
        )

        var result: RequestResult<ReactionData>? = null
        backgroundScope.launch {
            result = client.send(TournamentAction.Add("dummy"))
        }

        assertNull(result)
        advanceTimeBy(19900)
        assertEquals(1, socketConnection.startCount, message = "Expected socket connection created at this point")
        assertNull(result)
        advanceTimeBy(200)
        assertIs<RequestResult.Error>(result)

        val remainingOutgoingResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(remainingOutgoingResult.isFailure, message = "Did not expect any remaining messages")
        assertEquals(1, socketConnection.startCount, message = "Did not expect more created socket connections")
    }

    @Test
    fun sendTimesOutAfter5secondsIfNoResponseIsReceived() = runTest(testDispatcher) {
        val socketConnection = TestSocketConnection()

        val client = createSmsClient(
            scope = backgroundScope,
            socketConnection = socketConnection
        )

        var result: RequestResult<ReactionData>? = null
        backgroundScope.launch {
            result = client.send(TournamentAction.Add("dummy"))
        }

        runCurrent()
        assertEquals(1, socketConnection.startCount, message = "Expected socket connection created at this point")
        assertTrue(socketConnection.clientOutgoing.tryReceive().isSuccess, message = "Expected message")

        assertNull(result)
        advanceTimeBy(4900)
        assertNull(result)
        advanceTimeBy(200)
        assertIs<RequestResult.Error>(result)

        val remainingOutgoingResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(remainingOutgoingResult.isFailure, message = "Did not expect any remaining messages")
        assertEquals(1, socketConnection.startCount, message = "Did not expect more created socket connections")
    }

    @Test
    fun sendReturnsErrorIfWebSocketClosesNicelyBeforeSent() = runTest(testDispatcher) {
        val socketConnection = TestSocketConnection()

        val client = createSmsClient(
            scope = backgroundScope,
            socketConnection = socketConnection
        )

        var result: RequestResult<ReactionData>? = null
        backgroundScope.launch {
            result = client.send(TournamentAction.Add("dummy"))
        }

        runCurrent()
        assertEquals(1, socketConnection.startCount, message = "Expected socket connection created at this point")

        assertNull(result)

        socketConnection.clientIncoming.send(Frame.Close())
        runCurrent()
        assertIs<RequestResult.Error>(result)

        val remainingOutgoingResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(remainingOutgoingResult.isFailure, message = "Did not expect any remaining messages")
        assertEquals(1, socketConnection.startCount, message = "Did not expect more created socket connections")
    }

    @Test
    fun sendReturnsErrorIfWebSocketClosesNicelyAfterSent() = runTest(testDispatcher) {
        val socketConnection = TestSocketConnection()


        val client = createSmsClient(
            scope = backgroundScope,
            socketConnection = socketConnection
        )

        var result: RequestResult<ReactionData>? = null
        backgroundScope.launch {
            result = client.send(TournamentAction.Add("dummy"))
        }

        runCurrent()
        assertEquals(1, socketConnection.startCount, message = "Expected socket connection created at this point")
        assertTrue(socketConnection.clientOutgoing.tryReceive().isSuccess, message = "Expected message")

        runCurrent()
        assertNull(result)

        socketConnection.clientIncoming.send(Frame.Close())
        runCurrent()
        assertIs<RequestResult.Error>(result)

        val remainingOutgoingResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(remainingOutgoingResult.isFailure, message = "Did not expect any remaining messages")
        assertEquals(1, socketConnection.startCount, message = "Did not expect more created socket connections")
    }

    @Test
    fun sendReturnsErrorIfWebSocketClosesHardBeforeSent() = runTest(testDispatcher) {
        val socketConnection = TestSocketConnection()

        val client = createSmsClient(
            scope = backgroundScope,
            socketConnection = socketConnection
        )

        var result: RequestResult<ReactionData>? = null
        backgroundScope.launch {
            result = client.send(TournamentAction.Add("dummy"))
        }

        assertNull(result)

        runCurrent()
        assertEquals(1, socketConnection.startCount, message = "Expected socket connection created at this point")
        socketConnection.clientIncoming.close()

        runCurrent()
        assertIs<RequestResult.Error>(result)

        val remainingOutgoingResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(remainingOutgoingResult.isFailure, message = "Did not expect any remaining messages")
        assertEquals(1, socketConnection.startCount, message = "Did not expect more created socket connections")
    }

    @Test
    fun sendReturnsErrorIfWebSocketClosesHardAfterSent() = runTest(testDispatcher) {
        val socketConnection = TestSocketConnection()

        val client = createSmsClient(
            scope = backgroundScope,
            socketConnection = socketConnection
        )

        var result: RequestResult<ReactionData>? = null
        backgroundScope.launch {
            result = client.send(TournamentAction.Add("dummy"))
        }

        runCurrent()
        assertEquals(1, socketConnection.startCount, message = "Expected socket connection created at this point")
        assertTrue(socketConnection.clientOutgoing.tryReceive().isSuccess, message = "Expected message")

        runCurrent()
        assertNull(result)

        socketConnection.clientIncoming.close()
        runCurrent()
        assertIs<RequestResult.Error>(result)

        val remainingOutgoingResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(remainingOutgoingResult.isFailure, message = "Did not expect any remaining messages")
        assertEquals(1, socketConnection.startCount, message = "Did not expect more created socket connections")
    }

    @Test
    fun sendReturnsErrorIfScopeIsCancelledBeforeSent() = runTest(testDispatcher) {
        val socketConnection = TestSocketConnection()

        lateinit var client: SmsClient
        val clientJob = launch {
            client = createSmsClient(
                scope = this,
                socketConnection = socketConnection
            )
        }

        runCurrent()
        assertEquals(0, socketConnection.startCount, message = "Did not expect socket connection created at this point")

        var result: RequestResult<ReactionData>? = null
        backgroundScope.launch {
            result = client.send(TournamentAction.Add("dummy"))
        }

        runCurrent()
        assertEquals(1, socketConnection.startCount, message = "Expected socket connection created at this point")
        assertNull(result)

        clientJob.cancel()
        runCurrent()
        assertIs<RequestResult.Error>(result)

        val remainingOutgoingResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(remainingOutgoingResult.isFailure, message = "Did not expect any remaining messages")
        assertEquals(1, socketConnection.startCount, message = "Did not expect more created socket connections")
    }

    @Test
    fun sendReturnsErrorIfScopeIsCancelledAfterSent() = runTest(testDispatcher) {
        val socketConnection = TestSocketConnection()

        lateinit var client: SmsClient

        val clientJob = launch {
            client = createSmsClient(
                scope = this,
                socketConnection = socketConnection
            )
        }

        runCurrent()
        assertEquals(0, socketConnection.startCount, message = "Did not expect socket connection created at this point")

        var result: RequestResult<ReactionData>? = null
        backgroundScope.launch {
            result = client.send(TournamentAction.Add("dummy"))
        }

        runCurrent()
        assertEquals(1, socketConnection.startCount, message = "Expected socket connection created at this point")
        assertTrue(socketConnection.clientOutgoing.tryReceive().isSuccess, message = "Expected message")

        runCurrent()
        assertNull(result)

        clientJob.cancel()
        runCurrent()
        assertIs<RequestResult.Error>(result)

        val remainingOutgoingResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(remainingOutgoingResult.isFailure, message = "Did not expect any remaining messages")
        assertEquals(1, socketConnection.startCount, message = "Did not expect more created socket connections")
    }

    @Test
    fun sendReturnsErrorIfScopeIsCancelledEvenBeforeStartingToSend() = runTest(testDispatcher) {
        val socketConnection = TestSocketConnection()

        lateinit var client: SmsClient

        val clientJob = launch {
            client = createSmsClient(
                scope = this,
                socketConnection = socketConnection
            )
        }

        runCurrent()
        clientJob.cancel()
        runCurrent()
        assertEquals(0, socketConnection.startCount, message = "Did not expect socket connection created at this point")

        var result: RequestResult<ReactionData>? = null
        backgroundScope.launch {
            result = client.send(TournamentAction.Add("dummy"))
        }

        runCurrent()
        assertIs<RequestResult.Error>(result)

        val remainingOutgoingResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(remainingOutgoingResult.isFailure, message = "Did not expect any remaining messages")
        assertEquals(0, socketConnection.startCount, message = "Did not expect created socket connection")
    }

    @Test
    fun sendThrowsCancelIfSendScopeIsCancelledBeforeSend() = runTest(testDispatcher) {
        val socketConnection = TestSocketConnection()

        val client = createSmsClient(
            scope = backgroundScope,
            socketConnection = socketConnection
        )

        val sendJob = launch {
            client.send(TournamentAction.Add("dummy"))
        }

        runCurrent()
        assertEquals(1, socketConnection.startCount, message = "Expected socket connection created at this point")

        sendJob.cancel()

        assertTrue(sendJob.isCancelled, message = "Expected send job to be cancelled")

        val remainingOutgoingResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(remainingOutgoingResult.isFailure, message = "Did not expect any remaining messages")
        assertEquals(1, socketConnection.startCount, message = "Did not expect more created socket connections")
    }

    @Test
    fun sendThrowsCancelIfSendScopeIsCancelledAfterSend() = runTest(testDispatcher) {
        val socketConnection = TestSocketConnection()

        val client = createSmsClient(
            scope = backgroundScope,
            socketConnection = socketConnection
        )

        val sendJob = launch {
            client.send(TournamentAction.Add("dummy"))
        }

        runCurrent()
        assertEquals(1, socketConnection.startCount, message = "Expected socket connection created at this point")

        assertTrue(socketConnection.clientOutgoing.tryReceive().isSuccess, message = "Expected message")

        runCurrent()

        sendJob.cancel()

        assertTrue(sendJob.isCancelled, message = "Expected send job to be cancelled")

        val remainingOutgoingResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(remainingOutgoingResult.isFailure, message = "Did not expect any remaining messages")
        assertEquals(1, socketConnection.startCount, message = "Did not expect more created socket connections")
    }

    @Test
    fun subscribeReturnsCorrectMessages() = runTest(testDispatcher) {
        val socketConnection = TestSocketConnection()

        val reactions = mutableListOf<ReactionData>()

        val clientJob = launch {
            val client = createSmsClient(
                scope = this,
                socketConnection = socketConnection
            )

            client.subscribe(
                subscribeAction = { TournamentAction.Subscribe() },
                unsubscribeAction = { TournamentAction.Unsubscribe() }
            ).toCollection(reactions)
        }

        runCurrent()
        assertEquals(1, socketConnection.startCount, message = "Expected socket connection created at this point")
        val outgoingFrameResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(outgoingFrameResult.isSuccess, message = "Expected message")

        val subscribeRequest = extractRequest(outgoingFrameResult)
        assertIs<TournamentAction.Subscribe>(subscribeRequest.data)
        val actionId = subscribeRequest.actionId

        runCurrent()
        assertEquals(0, reactions.size)

        val wrongSubscribedReactionWithNullActionId = Response(
            context = Context.Tournament.contextKey,
            reaction = Reaction.Subscribed.reactionKey,
            actionId = null,
            data = TournamentReaction.Subscribed()
        )

        socketConnection.clientIncoming.sendTestResponse(wrongSubscribedReactionWithNullActionId)
        runCurrent()
        assertEquals(0, reactions.size)

        val wrongSubscribedReactionWithWrongActionId = Response(
            context = Context.Tournament.contextKey,
            reaction = Reaction.Subscribed.reactionKey,
            actionId = "wrongId",
            data = TournamentReaction.Subscribed()
        )

        socketConnection.clientIncoming.sendTestResponse(wrongSubscribedReactionWithWrongActionId)
        runCurrent()
        assertEquals(0, reactions.size)

        val correctSubscribedReaction = Response(
            context = Context.Tournament.contextKey,
            reaction = Reaction.Subscribed.reactionKey,
            actionId = actionId,
            data = TournamentReaction.Subscribed()
        )

        socketConnection.clientIncoming.sendTestResponse(correctSubscribedReaction)
        runCurrent()
        assertEquals(1, reactions.size)
        val subscriptionReaction = reactions.removeFirst()
        assertIs<TournamentReaction.Subscribed>(subscriptionReaction)

        val wrongUpdatedReactionWithNullActionId = Response(
            context = Context.Tournament.contextKey,
            reaction = Reaction.Updated.reactionKey,
            actionId = null,
            data = TournamentReaction.Updated(allTournaments = false, tournaments = emptyList())
        )

        socketConnection.clientIncoming.sendTestResponse(wrongUpdatedReactionWithNullActionId)
        runCurrent()
        assertEquals(0, reactions.size)

        val wrongUpdatedReactionWithWrongActionId = Response(
            context = Context.Tournament.contextKey,
            reaction = Reaction.Updated.reactionKey,
            actionId = "wrongId",
            data = TournamentReaction.Updated(allTournaments = false, tournaments = emptyList())
        )

        socketConnection.clientIncoming.sendTestResponse(wrongUpdatedReactionWithWrongActionId)
        runCurrent()
        assertEquals(0, reactions.size)

        val correctUpdatedReaction = Response(
            context = Context.Tournament.contextKey,
            reaction = Reaction.Updated.reactionKey,
            actionId = actionId,
            data = TournamentReaction.Updated(
                allTournaments = false,
                tournaments = listOf(
                    TournamentReaction.Tournament(
                        tournamentId = 1234,
                        name = "dummy",
                        isPublic = false,
                        tags = listOf("some-tags")
                    )
                )
            )
        )

        socketConnection.clientIncoming.sendTestResponse(correctUpdatedReaction)
        runCurrent()
        assertEquals(1, reactions.size)
        val updatedReaction = reactions.removeFirst()
        assertEquals(
            TournamentReaction.Updated(
                allTournaments = false,
                tournaments = listOf(
                    TournamentReaction.Tournament(
                        tournamentId = 1234,
                        name = "dummy",
                        isPublic = false,
                        tags = listOf("some-tags")
                    )
                )
            ),
            updatedReaction
        )

        val remainingOutgoingResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(remainingOutgoingResult.isFailure, message = "Did not expect any remaining messages")
        clientJob.cancel()
        runCurrent()
        assertEquals(0, reactions.size, message = "Did not expect any more reactions")
        assertEquals(1, socketConnection.startCount, message = "Did not expect more created socket connections")
    }

    @Test
    fun subscribeReceivesMessageReturnedRightAfterSendReceivedWithOtherSubscriber() =
        subscribeReceivesMessageReturnedRightAfterSendReceived(otherSubscriber = true)

    @Test
    fun subscribeReceivesMessageReturnedRightAfterSendReceivedWithoutOtherSubscriber() =
        subscribeReceivesMessageReturnedRightAfterSendReceived(otherSubscriber = false)

    private fun subscribeReceivesMessageReturnedRightAfterSendReceived(
        otherSubscriber: Boolean
    ) = runTest(testDispatcher) {
        val socketConnection = TestSocketConnection()
        lateinit var client: SmsClient

        val reactions = mutableListOf<ReactionData>()

        val clientJob = launch {
            client = createSmsClient(
                scope = this,
                socketConnection = socketConnection
            )

            if (otherSubscriber) {
                // This is to make sure some other part is also receiving messages and thus might "discard" messages
                // before they are able to get to the real receiver below
                client.subscribe(
                    subscribeAction = { TeamAction.Subscribe(tournamentId = 123) },
                    unsubscribeAction = { TeamAction.Unsubscribe(tournamentId = 123) }
                ).collect()
            }
        }

        runCurrent()

        if (otherSubscriber) {
            assertEquals(
                1,
                socketConnection.startCount,
                message = "Expected socket connection created at this point"
            )

            // Start dummy subscription
            val subscriptionMessage = socketConnection.clientOutgoing.tryReceive()
            assertTrue(subscriptionMessage.isSuccess, message = "Expected subscription message")
        }

        // Wait for the send message and respond right away
        launch {
            val outgoingFrameResult = socketConnection.clientOutgoing.receive()
            val outgoingRequest = extractRequest(outgoingFrameResult)
            assertIs<TournamentAction.Subscribe>(outgoingRequest.data)

            val correctResponse = Response(
                context = Context.Tournament.contextKey,
                reaction = Reaction.Subscribed.reactionKey,
                actionId = outgoingRequest.actionId,
                data = TournamentReaction.Subscribed()
            )

            socketConnection.clientIncoming.sendTestResponse(correctResponse)
        }

        runCurrent() // Make sure the dummy-response job is ready before we call send

        val subscriptionJob = launch {
            client.subscribe(
                subscribeAction = { TournamentAction.Subscribe() },
                unsubscribeAction = { TournamentAction.Unsubscribe() }
            ).toCollection(reactions)
        }

        runCurrent()
        assertEquals(1, socketConnection.startCount, message = "Expected socket connection created at this point")
        assertEquals(1, reactions.size)
        assertIs<TournamentReaction.Subscribed>(reactions.removeFirst())

        val remainingOutgoingResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(remainingOutgoingResult.isFailure, message = "Did not expect any remaining messages")
        subscriptionJob.cancel()
        clientJob.cancel()
        runCurrent()
        assertEquals(0, reactions.size, message = "Did not expect any more reactions")
        assertEquals(1, socketConnection.startCount, message = "Did not expect more created socket connections")
    }

    @Test
    fun subscribeReceivesMessageReturnedBeforeSendReceivedWithOtherSubscriber() =
        subscribeReceivesMessageReturnedBeforeSendReceived(otherSubscriber = true)

    @Test
    fun subscribeReceivesMessageReturnedBeforeSendReceivedWithoutOtherSubscriber() =
        subscribeReceivesMessageReturnedBeforeSendReceived(otherSubscriber = false)

    private fun subscribeReceivesMessageReturnedBeforeSendReceived(otherSubscriber: Boolean) = runTest(testDispatcher) {
        val socketConnection = TestSocketConnection()
        lateinit var client: SmsClient

        val reactions = mutableListOf<ReactionData>()

        val clientJob = launch {
            client = createSmsClient(
                scope = this,
                socketConnection = socketConnection
            )

            if (otherSubscriber) {
                // This is to make sure some other part is also receiving messages and thus might "discard" messages
                // before they are able to get to the real receiver below
                client.subscribe(
                    subscribeAction = { TeamAction.Subscribe(tournamentId = 123) },
                    unsubscribeAction = { TeamAction.Unsubscribe(tournamentId = 123) }
                ).collect()
            }
        }

        runCurrent()

        if (otherSubscriber) {
            assertEquals(1, socketConnection.startCount, message = "Expected socket connection created at this point")

            // Start dummy subscription
            val subscriptionMessage = socketConnection.clientOutgoing.tryReceive()
            assertTrue(subscriptionMessage.isSuccess, message = "Expected subscription message")
        }

        val subscriptionJob = launch {
            client.subscribe(
                subscribeAction = { TournamentAction.Subscribe() },
                unsubscribeAction = { TournamentAction.Unsubscribe() }
            ).toCollection(reactions)
        }

        runCurrent()
        assertEquals(1, socketConnection.startCount, message = "Expected socket connection created at this point")
        assertEquals(0, reactions.size, message = "Expected no reactions before anything received")

        val correctResponse = Response(
            context = Context.Tournament.contextKey,
            reaction = Reaction.Subscribed.reactionKey,
            actionId = if (otherSubscriber) "2" else "1", // If this ever change we just need to update it
            data = TournamentReaction.Subscribed()
        )

        // Fake response sent after send has started, but before the outgoing message is even received
        socketConnection.clientIncoming.sendTestResponse(correctResponse)
        runCurrent()

        // At this point we could check reactions and somehow expect 0 (as we didn't receive outgoing request yet), but
        // as the implementation is at the moment it does react to it and that's not really wrong either

        val outgoingFrameResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(outgoingFrameResult.isSuccess, message = "Expected message")

        runCurrent()
        assertEquals(1, reactions.size)
        val updatedReaction = reactions.removeFirst()
        assertIs<TournamentReaction.Subscribed>(updatedReaction)

        val remainingOutgoingResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(remainingOutgoingResult.isFailure, message = "Did not expect any remaining messages")
        subscriptionJob.cancel()
        clientJob.cancel()
        runCurrent()
        assertEquals(0, reactions.size, message = "Did not expect any more reactions")
        assertEquals(1, socketConnection.startCount, message = "Did not expect more created socket connections")
    }

    @Test
    fun subscribeSubscribesAndUnsubscribesOnlyOnce() = runTest(testDispatcher) {
        val socketConnection = TestSocketConnection()

        val client = createSmsClient(
            scope = backgroundScope,
            socketConnection = socketConnection
        )

        val subscriptionFlow = client.subscribe(
            subscribeAction = { TournamentAction.Subscribe() },
            unsubscribeAction = { TournamentAction.Unsubscribe() }
        )

        runCurrent()
        assertEquals(0, socketConnection.startCount, message = "Did not expect socket connection created at this point")

        val beforeSubscribedOutgoingFrameResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(beforeSubscribedOutgoingFrameResult.isFailure, message = "Expected no messages before subscribing")

        val collectJob1 = launch { subscriptionFlow.collect() }
        runCurrent()
        assertEquals(1, socketConnection.startCount, message = "Expected socket connection created at this point")

        val firstSubscriptionOutgoingFrameResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(firstSubscriptionOutgoingFrameResult.isSuccess, message = "Expected messages after subscribing")

        val subscribeRequest = extractRequest(firstSubscriptionOutgoingFrameResult)
        assertIs<TournamentAction.Subscribe>(subscribeRequest.data)
        val subscribedActionId = subscribeRequest.actionId

        val collectJob2 = launch { subscriptionFlow.collect() }
        runCurrent()
        assertEquals(1, socketConnection.startCount, message = "Did not expect more created socket connections")

        val secondSubscribedOutgoingFrameResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(
            secondSubscribedOutgoingFrameResult.isFailure,
            message = "Expected no messages after second collection"
        )

        // Sms client don't really need this response to work at the moment, but it's usually a part of the flow so it
        // makes sense to fake it
        val correctSubscribedReaction = Response(
            context = Context.Tournament.contextKey,
            reaction = Reaction.Subscribed.reactionKey,
            actionId = subscribedActionId,
            data = TournamentReaction.Subscribed()
        )

        socketConnection.clientIncoming.sendTestResponse(correctSubscribedReaction)
        runCurrent()

        collectJob1.cancel()
        runCurrent()

        val firstUnsubscribedOutgoingFrameResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(
            firstUnsubscribedOutgoingFrameResult.isFailure,
            message = "Expected no messages after first collection is cancelled"
        )

        collectJob2.cancel()
        runCurrent()

        val secondUnsubscribedOutgoingFrameResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(secondUnsubscribedOutgoingFrameResult.isSuccess, message = "Expected messages after unsubscribing")

        val unsubscribeRequest = extractRequest(secondUnsubscribedOutgoingFrameResult)
        assertIs<TournamentAction.Unsubscribe>(unsubscribeRequest.data)

        assertEquals(subscribedActionId, unsubscribeRequest.actionId)

        val remainingOutgoingResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(remainingOutgoingResult.isFailure, message = "Did not expect any remaining messages")
        assertEquals(1, socketConnection.startCount, message = "Did not expect more created socket connections")
    }

    @Test
    fun subscribeSendDoesNotTimeOutIfSocketTakesLongToStart() = runTest(testDispatcher) {
        val socketConnection = TestSocketConnection()

        val client = SmsClient(
            scope = backgroundScope,
            useBetaEndpoint = false,
            createSocket = createSocket@{ _, block ->
                delay(10_000)
                socketConnection.startFakeSocket(client = this, block = block)
            },
            engine = mockEngine
        )

        backgroundScope.launch {
            client.subscribe(
                subscribeAction = { TournamentAction.Subscribe() },
                unsubscribeAction = { TournamentAction.Unsubscribe() }
            ).collect()
        }

        runCurrent()
        assertEquals(0, socketConnection.startCount, message = "Did not expected socket connection at this point")

        advanceTimeBy(11_000)
        assertEquals(1, socketConnection.startCount, message = "Expected socket connection created at this point")

        val firstSubscriptionOutgoingFrameResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(firstSubscriptionOutgoingFrameResult.isSuccess, message = "Did expect a subscribe message")
        assertIs<TournamentAction.Subscribe>(extractRequest(firstSubscriptionOutgoingFrameResult).data)

        val remainingOutgoingResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(remainingOutgoingResult.isFailure, message = "Did not expect any remaining messages")
        assertEquals(1, socketConnection.startCount, message = "Did not expect more created socket connections")
    }

    @Test
    fun subscribeSendDoesNotTimeOutBeforeSubscribeSent() = runTest(testDispatcher) {
        val socketConnection = TestSocketConnection()

        val client = createSmsClient(
            scope = backgroundScope,
            socketConnection = socketConnection
        )

        backgroundScope.launch {
            client.subscribe(
                subscribeAction = { TournamentAction.Subscribe() },
                unsubscribeAction = { TournamentAction.Unsubscribe() }
            ).collect()
        }

        runCurrent()
        assertEquals(1, socketConnection.startCount, message = "Expected socket connection created at this point")

        advanceTimeBy(1_000_000) // This is a long time!

        val firstSubscriptionOutgoingFrameResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(firstSubscriptionOutgoingFrameResult.isSuccess, message = "Did expect a subscribe message")
        assertIs<TournamentAction.Subscribe>(extractRequest(firstSubscriptionOutgoingFrameResult).data)

        val remainingOutgoingResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(remainingOutgoingResult.isFailure, message = "Did not expect any remaining messages")
        assertEquals(1, socketConnection.startCount, message = "Did not expect more created socket connections")
    }

    @Test
    fun subscribeSendTimesOutAfterSubscribeSent() = runTest(testDispatcher) {
        val socketConnection = TestSocketConnection()

        val client = createSmsClient(
            scope = backgroundScope,
            socketConnection = socketConnection
        )

        backgroundScope.launch {
            client.subscribe(
                subscribeAction = { TournamentAction.Subscribe() },
                unsubscribeAction = { TournamentAction.Unsubscribe() }
            ).collect()
        }

        runCurrent()
        assertEquals(1, socketConnection.startCount, message = "Expected socket connection created at this point")

        val firstSubscriptionOutgoingFrameResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(firstSubscriptionOutgoingFrameResult.isSuccess, message = "Did expect a subscribe message")
        val firstRequest = extractRequest(firstSubscriptionOutgoingFrameResult)
        assertIs<TournamentAction.Subscribe>(firstRequest.data)

        advanceTimeBy(4_900)

        val outgoingFrameResultBeforeFirstTimeout = socketConnection.clientOutgoing.tryReceive()
        assertTrue(outgoingFrameResultBeforeFirstTimeout.isFailure, message = "Did not expect a subscribe message")

        advanceTimeBy(200)

        val outgoingFrameResultBeforeSecondTimeout = socketConnection.clientOutgoing.tryReceive()
        assertTrue(outgoingFrameResultBeforeSecondTimeout.isFailure, message = "Did not expect a subscribe message")
        assertEquals(1, socketConnection.startCount, message = "Did not expect more created socket connections")
    }

    @Test
    fun subscribesAgainIfSocketCouldNotStartFirstTime() = runTest(testDispatcher) {
        lateinit var socketConnection: TestSocketConnection

        val reactions = mutableListOf<ReactionData>()
        var startCount = 0

        val client = SmsClient(
            scope = backgroundScope,
            useBetaEndpoint = false,
            createSocket = createSocket@{ _, block ->
                val firstTime = startCount == 0
                ++startCount

                if (firstTime) {
                    throw IllegalArgumentException("This is just a random picked exception type")
                } else {
                    socketConnection = TestSocketConnection()
                    socketConnection.startFakeSocket(client = this, block = block)
                }
            },
            engine = mockEngine
        )

        backgroundScope.launch {
            client.subscribe(
                subscribeAction = { TournamentAction.Subscribe() },
                unsubscribeAction = { TournamentAction.Unsubscribe() }
            ).toCollection(reactions)
        }

        runCurrent()
        assertEquals(1, startCount, message = "Expected socket connection tried to be started")

        advanceTimeBy(50.seconds - 100.milliseconds)
        assertEquals(1, startCount, message = "Did not expect a socket connection retry yet")
        assertEquals(0, reactions.size, message = "Did not expect any reactions at this point")

        advanceTimeBy(20.seconds + 200.milliseconds) // We expect it to be some random amount between 50 and 70 seconds

        assertEquals(2, startCount, message = "Did expect created socket connection")
        val outgoingFrameResultAfterRetry = socketConnection.clientOutgoing.tryReceive()
        assertTrue(outgoingFrameResultAfterRetry.isSuccess, message = "Did expect a subscribe message")

        val subscriptionRequest = extractRequest(outgoingFrameResultAfterRetry)
        assertIs<TournamentAction.Subscribe>(subscriptionRequest.data)

        assertEquals(0, reactions.size, message = "Did not expect any reactions at this point")

        val secondSubscriptionReaction = Response(
            context = Context.Tournament.contextKey,
            reaction = Reaction.Subscribed.reactionKey,
            actionId = subscriptionRequest.actionId,
            data = TournamentReaction.Subscribed()
        )

        socketConnection.clientIncoming.sendTestResponse(secondSubscriptionReaction)
        runCurrent()
        assertEquals(1, reactions.size, message = "Did expect an subscription reaction")

        val remainingOutgoingResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(remainingOutgoingResult.isFailure, message = "Did not expect any remaining messages")
        assertEquals(1, socketConnection.startCount, message = "Did not expect more created socket connections")
    }

    @Test
    fun subscribesAgainIfSocketIsClosedNicelyBeforeSubscribeSent() = runTest(testDispatcher) {
        val socketConnection = TestSocketConnection()

        val reactions = mutableListOf<ReactionData>()

        val client = createSmsClient(
            scope = backgroundScope,
            socketConnection = socketConnection
        )

        backgroundScope.launch {
            client.subscribe(
                subscribeAction = { TournamentAction.Subscribe() },
                unsubscribeAction = { TournamentAction.Unsubscribe() }
            ).toCollection(reactions)
        }

        runCurrent()
        assertEquals(1, socketConnection.startCount, message = "Expected socket connection created at this point")

        socketConnection.clientIncoming.send(Frame.Close())
        runCurrent()

        val outgoingFrameResultBeforeClose = socketConnection.clientOutgoing.tryReceive()
        assertTrue(outgoingFrameResultBeforeClose.isFailure, message = "Did not expect a subscribe message")

        advanceTimeBy(4_900)

        val outgoingFrameResultAfterClose = socketConnection.clientOutgoing.tryReceive()
        assertTrue(outgoingFrameResultAfterClose.isFailure, message = "Did not expect a subscribe message")
        assertEquals(1, socketConnection.startCount, message = "Did not expect more created socket connections")

        advanceTimeBy(200)

        assertEquals(2, socketConnection.startCount, message = "Did expect new created socket connection")
        val outgoingFrameResultAfterReopen = socketConnection.clientOutgoing.tryReceive()
        assertTrue(outgoingFrameResultAfterReopen.isSuccess, message = "Did expect a subscribe message")
        val subscriptionRequest = extractRequest(outgoingFrameResultAfterReopen)
        assertIs<TournamentAction.Subscribe>(subscriptionRequest.data)

        assertEquals(0, reactions.size, message = "Did not expect any reactions at this point")

        val subscriptionReaction = Response(
            context = Context.Tournament.contextKey,
            reaction = Reaction.Subscribed.reactionKey,
            actionId = subscriptionRequest.actionId,
            data = TournamentReaction.Subscribed()
        )

        socketConnection.clientIncoming.sendTestResponse(subscriptionReaction)
        runCurrent()
        assertEquals(1, reactions.size, message = "Did expect an subscription reaction")

        val remainingOutgoingResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(remainingOutgoingResult.isFailure, message = "Did not expect any remaining messages")
        assertEquals(2, socketConnection.startCount, message = "Did not expect more created socket connections")
    }

    @Test
    fun subscribesAgainIfSocketIsClosedNicelyAfterSubscribeSent() = runTest(testDispatcher) {
        val socketConnection = TestSocketConnection()

        val reactions = mutableListOf<ReactionData>()

        val client = createSmsClient(
            scope = backgroundScope,
            socketConnection = socketConnection
        )

        backgroundScope.launch {
            client.subscribe(
                subscribeAction = { TournamentAction.Subscribe() },
                unsubscribeAction = { TournamentAction.Unsubscribe() }
            ).toCollection(reactions)
        }

        runCurrent()
        assertEquals(1, socketConnection.startCount, message = "Expected socket connection created at this point")

        val firstSubscriptionOutgoingFrameResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(firstSubscriptionOutgoingFrameResult.isSuccess, message = "Did expect a subscribe message")
        val firstRequest = extractRequest(firstSubscriptionOutgoingFrameResult)
        assertIs<TournamentAction.Subscribe>(firstRequest.data)

        runCurrent()

        socketConnection.clientIncoming.send(Frame.Close())
        runCurrent()

        val outgoingFrameResultBeforeClose = socketConnection.clientOutgoing.tryReceive()
        assertTrue(outgoingFrameResultBeforeClose.isFailure, message = "Did not expect a subscribe message")

        advanceTimeBy(4_900)

        val outgoingFrameResultAfterClose = socketConnection.clientOutgoing.tryReceive()
        assertTrue(outgoingFrameResultAfterClose.isFailure, message = "Did not expect a subscribe message")
        assertEquals(1, socketConnection.startCount, message = "Did not expect more created socket connections")

        advanceTimeBy(200)

        assertEquals(2, socketConnection.startCount, message = "Did expect new created socket connection")
        val outgoingFrameResultAfterReopen = socketConnection.clientOutgoing.tryReceive()
        assertTrue(outgoingFrameResultAfterReopen.isSuccess, message = "Did expect a subscribe message")
        val secondRequest = extractRequest(outgoingFrameResultAfterReopen)
        assertIs<TournamentAction.Subscribe>(secondRequest.data)

        assertEquals(firstRequest.actionId, secondRequest.actionId)

        assertEquals(0, reactions.size, message = "Did not expect any reactions at this point")

        val subscriptionReaction = Response(
            context = Context.Tournament.contextKey,
            reaction = Reaction.Subscribed.reactionKey,
            actionId = secondRequest.actionId,
            data = TournamentReaction.Subscribed()
        )

        socketConnection.clientIncoming.sendTestResponse(subscriptionReaction)
        runCurrent()
        assertEquals(1, reactions.size, message = "Did expect an subscription reaction")

        val remainingOutgoingResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(remainingOutgoingResult.isFailure, message = "Did not expect any remaining messages")
        assertEquals(2, socketConnection.startCount, message = "Did not expect more created socket connections")
    }

    @Test
    fun subscribesAgainIfSocketIsClosedNicelyAfterSubscribedReceived() = runTest(testDispatcher) {
        val socketConnection = TestSocketConnection()

        val reactions = mutableListOf<ReactionData>()

        val client = createSmsClient(
            scope = backgroundScope,
            socketConnection = socketConnection
        )

        backgroundScope.launch {
            client.subscribe(
                subscribeAction = { TournamentAction.Subscribe() },
                unsubscribeAction = { TournamentAction.Unsubscribe() }
            ).toCollection(reactions)
        }

        runCurrent()
        assertEquals(1, socketConnection.startCount, message = "Expected socket connection created at this point")

        val firstSubscriptionOutgoingFrameResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(firstSubscriptionOutgoingFrameResult.isSuccess, message = "Did expect a subscribe message")
        val firstSubscriptionRequest = extractRequest(firstSubscriptionOutgoingFrameResult)
        assertIs<TournamentAction.Subscribe>(firstSubscriptionRequest.data)
        assertEquals(0, reactions.size, message = "Did not expect any reactions at this point")

        val firstSubscriptionReaction = Response(
            context = Context.Tournament.contextKey,
            reaction = Reaction.Subscribed.reactionKey,
            actionId = firstSubscriptionRequest.actionId,
            data = TournamentReaction.Subscribed()
        )

        socketConnection.clientIncoming.sendTestResponse(firstSubscriptionReaction)
        runCurrent()
        assertEquals(1, reactions.size, message = "Did expect an subscription reaction")

        socketConnection.clientIncoming.send(Frame.Close())
        runCurrent()

        val outgoingFrameResultBeforeClose = socketConnection.clientOutgoing.tryReceive()
        assertTrue(outgoingFrameResultBeforeClose.isFailure, message = "Did not expect a subscribe message")

        advanceTimeBy(4_900)

        val outgoingFrameResultAfterClose = socketConnection.clientOutgoing.tryReceive()
        assertTrue(outgoingFrameResultAfterClose.isFailure, message = "Did not expect a subscribe message")
        assertEquals(1, socketConnection.startCount, message = "Did not expect more created socket connections")

        advanceTimeBy(200)

        assertEquals(2, socketConnection.startCount, message = "Did expect new created socket connection")
        val outgoingFrameResultAfterReopen = socketConnection.clientOutgoing.tryReceive()
        assertTrue(outgoingFrameResultAfterReopen.isSuccess, message = "Did expect a subscribe message")

        val secondSubscriptionRequest = extractRequest(outgoingFrameResultAfterReopen)
        assertIs<TournamentAction.Subscribe>(secondSubscriptionRequest.data)
        assertEquals(firstSubscriptionRequest.actionId, secondSubscriptionRequest.actionId)

        assertEquals(1, reactions.size, message = "Did not expect any new reactions at this point")

        val secondSubscriptionReaction = Response(
            context = Context.Tournament.contextKey,
            reaction = Reaction.Subscribed.reactionKey,
            actionId = secondSubscriptionRequest.actionId,
            data = TournamentReaction.Subscribed()
        )

        socketConnection.clientIncoming.sendTestResponse(secondSubscriptionReaction)
        runCurrent()
        assertEquals(2, reactions.size, message = "Did expect an subscription reaction")

        val remainingOutgoingResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(remainingOutgoingResult.isFailure, message = "Did not expect any remaining messages")
        assertEquals(2, socketConnection.startCount, message = "Did not expect more created socket connections")
    }

    @Test
    fun subscribesAgainIfSocketIsClosedHardBeforeSubscribeSent() = runTest(testDispatcher) {
        lateinit var socketConnection: TestSocketConnection
        var startCount = 0

        val reactions = mutableListOf<ReactionData>()

        val client = SmsClient(
            scope = backgroundScope,
            useBetaEndpoint = false,
            createSocket = createSocket@{ _, block ->
                ++startCount
                socketConnection = TestSocketConnection()
                socketConnection.startFakeSocket(client = this, block = block)
            },
            engine = mockEngine
        )

        backgroundScope.launch {
            client.subscribe(
                subscribeAction = { TournamentAction.Subscribe() },
                unsubscribeAction = { TournamentAction.Unsubscribe() }
            ).toCollection(reactions)
        }

        runCurrent()
        assertEquals(1, startCount, message = "Expected socket to have connected once")

        socketConnection.clientIncoming.close()
        runCurrent()

        val outgoingFrameResultBeforeClose = socketConnection.clientOutgoing.tryReceive()
        assertTrue(outgoingFrameResultBeforeClose.isFailure, message = "Did not expect a subscribe message")

        advanceTimeBy(4_900)

        assertEquals(1, startCount, message = "Did not expect socket to have reconnected")

        val outgoingFrameResultAfterClose = socketConnection.clientOutgoing.tryReceive()
        assertTrue(outgoingFrameResultAfterClose.isFailure, message = "Did not expect a subscribe message")

        advanceTimeBy(200)

        assertEquals(2, startCount, message = "Expect new socket connection")

        val outgoingFrameResultAfterReopen = socketConnection.clientOutgoing.tryReceive()
        assertTrue(outgoingFrameResultAfterReopen.isSuccess, message = "Did expect a subscribe message")
        val subscriptionRequest = extractRequest(outgoingFrameResultAfterReopen)
        assertIs<TournamentAction.Subscribe>(subscriptionRequest.data)

        assertEquals(0, reactions.size, message = "Did not expect any reactions at this point")

        val subscriptionReaction = Response(
            context = Context.Tournament.contextKey,
            reaction = Reaction.Subscribed.reactionKey,
            actionId = subscriptionRequest.actionId,
            data = TournamentReaction.Subscribed()
        )

        socketConnection.clientIncoming.sendTestResponse(subscriptionReaction)
        runCurrent()
        assertEquals(1, reactions.size, message = "Did expect an subscription reaction")

        val remainingOutgoingResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(remainingOutgoingResult.isFailure, message = "Did not expect any remaining messages")
        assertEquals(2, startCount, message = "Did not expect any new socket connection")
    }

    @Test
    fun subscribesAgainIfSocketIsClosedHardAfterSubscribeSent() = runTest(testDispatcher) {
        lateinit var socketConnection: TestSocketConnection
        var startCount = 0

        val reactions = mutableListOf<ReactionData>()

        val client = SmsClient(
            scope = backgroundScope,
            useBetaEndpoint = false,
            createSocket = createSocket@{ _, block ->
                ++startCount
                socketConnection = TestSocketConnection()
                socketConnection.startFakeSocket(client = this, block = block)
            },
            engine = mockEngine
        )

        backgroundScope.launch {
            client.subscribe(
                subscribeAction = { TournamentAction.Subscribe() },
                unsubscribeAction = { TournamentAction.Unsubscribe() }
            ).toCollection(reactions)
        }

        runCurrent()
        assertEquals(1, startCount, message = "Expected socket to have connected once")

        val firstSubscriptionOutgoingFrameResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(firstSubscriptionOutgoingFrameResult.isSuccess, message = "Did expect a subscribe message")
        val firstRequest = extractRequest(firstSubscriptionOutgoingFrameResult)
        assertIs<TournamentAction.Subscribe>(firstRequest.data)

        runCurrent()

        socketConnection.clientIncoming.close()
        runCurrent()

        val outgoingFrameResultBeforeClose = socketConnection.clientOutgoing.tryReceive()
        assertTrue(outgoingFrameResultBeforeClose.isFailure, message = "Did not expect a subscribe message")

        advanceTimeBy(4_900)

        assertEquals(1, startCount, message = "Did not expect socket to have reconnected")

        val outgoingFrameResultAfterClose = socketConnection.clientOutgoing.tryReceive()
        assertTrue(outgoingFrameResultAfterClose.isFailure, message = "Did not expect a subscribe message")

        advanceTimeBy(200)

        assertEquals(2, startCount, message = "Expect new socket connection")

        val outgoingFrameResultAfterReopen = socketConnection.clientOutgoing.tryReceive()
        assertTrue(outgoingFrameResultAfterReopen.isSuccess, message = "Did expect a subscribe message")
        val secondRequest = extractRequest(outgoingFrameResultAfterReopen)
        assertIs<TournamentAction.Subscribe>(secondRequest.data)

        assertEquals(firstRequest.actionId, secondRequest.actionId)

        assertEquals(0, reactions.size, message = "Did not expect any reactions at this point")

        val subscriptionReaction = Response(
            context = Context.Tournament.contextKey,
            reaction = Reaction.Subscribed.reactionKey,
            actionId = secondRequest.actionId,
            data = TournamentReaction.Subscribed()
        )

        socketConnection.clientIncoming.sendTestResponse(subscriptionReaction)
        runCurrent()
        assertEquals(1, reactions.size, message = "Did expect an subscription reaction")

        val remainingOutgoingResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(remainingOutgoingResult.isFailure, message = "Did not expect any remaining messages")
        assertEquals(2, startCount, message = "Did not expect any new socket connection")
    }

    @Test
    fun subscribesAgainIfSocketIsClosedHardAfterSubscribedReceived() = runTest(testDispatcher) {
        lateinit var socketConnection: TestSocketConnection
        val socketConnectionCreated = Channel<Unit>(capacity = Channel.UNLIMITED)
        var startCount = 0

        val reactions = mutableListOf<ReactionData>()

        val client = SmsClient(
            scope = backgroundScope,
            useBetaEndpoint = false,
            createSocket = createSocket@{ _, block ->
                ++startCount
                socketConnection = TestSocketConnection()
                socketConnectionCreated.send(Unit)
                socketConnection.startFakeSocket(client = this, block = block)
            },
            engine = mockEngine
        )

        backgroundScope.launch {
            client.subscribe(
                subscribeAction = { TournamentAction.Subscribe() },
                unsubscribeAction = { TournamentAction.Unsubscribe() }
            ).toCollection(reactions)
        }

        socketConnectionCreated.receive()

        runCurrent()

        val firstSubscriptionOutgoingFrameResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(firstSubscriptionOutgoingFrameResult.isSuccess, message = "Did expect a subscribe message")
        val firstSubscriptionRequest = extractRequest(firstSubscriptionOutgoingFrameResult)
        assertIs<TournamentAction.Subscribe>(firstSubscriptionRequest.data)
        assertEquals(0, reactions.size, message = "Did no expect any reactions at this point")

        val firstSubscriptionReaction = Response(
            context = Context.Tournament.contextKey,
            reaction = Reaction.Subscribed.reactionKey,
            actionId = firstSubscriptionRequest.actionId,
            data = TournamentReaction.Subscribed()
        )

        socketConnection.clientIncoming.sendTestResponse(firstSubscriptionReaction)
        runCurrent()
        assertEquals(1, reactions.size, message = "Did expect an subscription reaction")

        socketConnection.clientIncoming.close()

        runCurrent()

        val outgoingFrameResultBeforeClose = socketConnection.clientOutgoing.tryReceive()
        assertTrue(outgoingFrameResultBeforeClose.isFailure, message = "Did not expect a subscribe message")

        advanceTimeBy(4_900)

        assertEquals(1, startCount, message = "Did not expect socket to have reconnected")

        val outgoingFrameResultAfterClose = socketConnection.clientOutgoing.tryReceive()
        assertTrue(outgoingFrameResultAfterClose.isFailure, message = "Did not expect a subscribe message")

        advanceTimeBy(200)

        assertEquals(2, startCount, message = "Expect new socket connection")

        val outgoingFrameResultAfterReopen = socketConnection.clientOutgoing.tryReceive()
        assertTrue(outgoingFrameResultAfterReopen.isSuccess, message = "Did expect a subscribe message")

        val secondSubscriptionRequest = extractRequest(outgoingFrameResultAfterReopen)
        assertIs<TournamentAction.Subscribe>(secondSubscriptionRequest.data)
        assertEquals(firstSubscriptionRequest.actionId, secondSubscriptionRequest.actionId)

        assertEquals(1, reactions.size, message = "Did not expect any new reactions at this point")

        val secondSubscriptionReaction = Response(
            context = Context.Tournament.contextKey,
            reaction = Reaction.Subscribed.reactionKey,
            actionId = secondSubscriptionRequest.actionId,
            data = TournamentReaction.Subscribed()
        )

        socketConnection.clientIncoming.sendTestResponse(secondSubscriptionReaction)
        runCurrent()
        assertEquals(2, reactions.size, message = "Did expect an subscription reaction")

        val remainingOutgoingResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(remainingOutgoingResult.isFailure, message = "Did not expect any remaining messages")
        assertEquals(2, startCount, message = "Did not expect any new socket connection")
    }

    @Test
    fun subscribeStopsIfSocketIsCancelledBeforeSubscribeSent() = runTest(testDispatcher) {
        val socketConnection = TestSocketConnection()

        lateinit var client: SmsClient

        val clientJob = launch {
            client = createSmsClient(
                scope = this,
                socketConnection = socketConnection
            )
        }

        runCurrent()

        backgroundScope.launch {
            client.subscribe(
                subscribeAction = { TournamentAction.Subscribe() },
                unsubscribeAction = { TournamentAction.Unsubscribe() }
            ).collect()
        }

        runCurrent()

        assertEquals(1, socketConnection.startCount, message = "Expected socket connection created at this point")

        clientJob.cancel()

        runCurrent()

        val outgoingFrameResultBeforeClose = socketConnection.clientOutgoing.tryReceive()
        assertTrue(outgoingFrameResultBeforeClose.isFailure, message = "Did not expect a subscribe message")

        advanceTimeBy(5_100)

        val outgoingFrameResultAfterClose = socketConnection.clientOutgoing.tryReceive()
        assertTrue(outgoingFrameResultAfterClose.isFailure, message = "Did not expect a subscribe message")
        assertEquals(1, socketConnection.startCount, message = "Did not expect more created socket connections")
    }

    @Test
    fun subscribeStopsIfSocketIsCancelledAfterSubscribeSent() = runTest(testDispatcher) {
        val socketConnection = TestSocketConnection()

        lateinit var client: SmsClient

        val clientJob = launch {
            client = createSmsClient(
                scope = this,
                socketConnection = socketConnection
            )
        }

        runCurrent()

        backgroundScope.launch {
            client.subscribe(
                subscribeAction = { TournamentAction.Subscribe() },
                unsubscribeAction = { TournamentAction.Unsubscribe() }
            ).collect()
        }

        runCurrent()

        assertEquals(1, socketConnection.startCount, message = "Expected socket connection created at this point")

        val firstSubscriptionOutgoingFrameResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(firstSubscriptionOutgoingFrameResult.isSuccess, message = "Did expect a subscribe message")
        assertIs<TournamentAction.Subscribe>(extractRequest(firstSubscriptionOutgoingFrameResult).data)

        runCurrent()

        clientJob.cancel()

        runCurrent()

        val outgoingFrameResultBeforeClose = socketConnection.clientOutgoing.tryReceive()
        assertTrue(outgoingFrameResultBeforeClose.isFailure, message = "Did not expect a subscribe message")

        advanceTimeBy(5_100)

        val outgoingFrameResultAfterClose = socketConnection.clientOutgoing.tryReceive()
        assertTrue(outgoingFrameResultAfterClose.isFailure, message = "Did not expect a subscribe message")
        assertEquals(1, socketConnection.startCount, message = "Did not expect more created socket connections")
    }

    @Test
    fun subscribeStopsIfSocketIsCancelledAfterSubscribedReceived() = runTest(testDispatcher) {
        val socketConnection = TestSocketConnection()

        lateinit var client: SmsClient

        val clientJob = launch {
            client = createSmsClient(
                scope = this,
                socketConnection = socketConnection
            )
        }

        runCurrent()

        backgroundScope.launch {
            client.subscribe(
                subscribeAction = { TournamentAction.Subscribe() },
                unsubscribeAction = { TournamentAction.Unsubscribe() }
            ).collect()
        }

        runCurrent()

        assertEquals(1, socketConnection.startCount, message = "Expected socket connection created at this point")

        val subscriptionOutgoingFrameResult = socketConnection.clientOutgoing.tryReceive()
        assertTrue(subscriptionOutgoingFrameResult.isSuccess, message = "Did expect a subscribe message")
        val subscriptionRequest = extractRequest(subscriptionOutgoingFrameResult)
        assertIs<TournamentAction.Subscribe>(subscriptionRequest.data)

        val subscriptionReaction = Response(
            context = Context.Tournament.contextKey,
            reaction = Reaction.Subscribed.reactionKey,
            actionId = subscriptionRequest.actionId,
            data = TournamentReaction.Subscribed()
        )

        socketConnection.clientIncoming.sendTestResponse(subscriptionReaction)
        runCurrent()

        clientJob.cancel()

        runCurrent()

        val outgoingFrameResultBeforeClose = socketConnection.clientOutgoing.tryReceive()
        assertTrue(outgoingFrameResultBeforeClose.isFailure, message = "Did not expect a subscribe message")

        advanceTimeBy(5_100)

        val outgoingFrameResultAfterClose = socketConnection.clientOutgoing.tryReceive()
        assertTrue(outgoingFrameResultAfterClose.isFailure, message = "Did not expect a subscribe message")
        assertEquals(1, socketConnection.startCount, message = "Did not expect more created socket connections")
    }

    private fun createSmsClient(
        scope: CoroutineScope,
        socketConnection: TestSocketConnection
    ): SmsClient {
        return SmsClient(
            scope = scope,
            useBetaEndpoint = false,
            createSocket = createSocket@{ _, block ->
                socketConnection.startFakeSocket(client = this, block = block)
            },
            engine = mockEngine
        )
    }

    private fun extractRequest(outgoingFrameResult: ChannelResult<Frame>): Request {
        return extractRequest(outgoingFrame = outgoingFrameResult.getOrThrow())
    }

    private fun extractRequest(outgoingFrame: Frame): Request {
        val textFrame = outgoingFrame as Frame.Text
        val message = textFrame.readText()
        return Json.decodeFromString(RequestSerializerInterceptor, message)
    }

    private suspend fun SendChannel<Frame>.sendTestResponse(response: Response) {
        send(Frame.Text(Json.encodeToString(ResponseSerializerInterceptor, response)))
    }
}
