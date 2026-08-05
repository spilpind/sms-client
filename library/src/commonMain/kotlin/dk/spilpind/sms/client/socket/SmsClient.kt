package dk.spilpind.sms.client.socket

import co.touchlab.kermit.Logger
import dk.anigif.kmp.flow.WhileSubscribedWithMinimumLifetime
import dk.anigif.kmp.flow.onUnsubscription
import dk.anigif.kmp.log.KermitExtension.unexpected
import dk.spilpind.sms.api.Request
import dk.spilpind.sms.api.RequestSerializerInterceptor
import dk.spilpind.sms.api.Response
import dk.spilpind.sms.api.ResponseSerializerInterceptor
import dk.spilpind.sms.api.action.ContextAction
import dk.spilpind.sms.api.action.ReactionData
import dk.spilpind.sms.api.core.Status
import dk.spilpind.sms.core.TimeHelper
import io.ktor.client.HttpClient
import io.ktor.client.HttpClientConfig
import io.ktor.client.call.body
import io.ktor.client.engine.HttpClientEngine
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.plugins.websocket.DefaultClientWebSocketSession
import io.ktor.client.plugins.websocket.WebSockets
import io.ktor.client.plugins.websocket.wss
import io.ktor.client.request.get
import io.ktor.serialization.ContentConvertException
import io.ktor.serialization.kotlinx.json.json
import io.ktor.websocket.Frame
import io.ktor.websocket.readReason
import io.ktor.websocket.readText
import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.combineTransform
import kotlinx.coroutines.flow.drop
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onSubscription
import kotlinx.coroutines.flow.shareIn
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withTimeout
import kotlinx.io.IOException
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.Json
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Handles all interaction with the SMS server
 */
class SmsClient(
    private val scope: CoroutineScope,
    private var useBetaEndpoint: Boolean,
    private val createSocket: suspend HttpClient.(
        config: WebsocketConfig,
        block: suspend DefaultClientWebSocketSession.() -> Unit
    ) -> Unit = { config, block ->
        wss(host = config.host, path = config.path, block = block)
    },
    engine: HttpClientEngine? = null
) {

    companion object {
        private const val SERVER_PROD_HOST = "sms.spilpind.dk"
        private const val SERVER_BETA_HOST = "sms-beta.spilpind.dk"

        private const val SERVER_STATUS_PATH = "/api/v1/status"
        private const val SERVER_STREAM_PATH = "/api/v1/stream"

        private const val INCOMING_RESPONSES_REPLAY = 1

        private val CONNECTION_CLOSED_RETRY_DELAY = 5.seconds
        private val AFTER_REQUEST_SENT_TIMEOUT = 5.seconds

        // This is "last resort" timeout - if we somehow for some reason has to wait a loong time. But we want it to big
        // as the server might (at the moment of writing) answer slowly for initial calls
        private val TOTAL_REQUEST_TIMEOUT = (AFTER_REQUEST_SENT_TIMEOUT + CONNECTION_CLOSED_RETRY_DELAY) * 2


        private fun generateLongerRetryDelay(): Duration {
            // Let the server do its work - we don't expect this happening that often, but if it do we don't think
            // waiting a few seconds will do a difference. We however want to make it a bit random to ensure that not
            // all clients will call the endpoint again at the same time
            return Random.nextInt(50, 70).seconds
        }

        // We want to keep this high enough to complete a set of requests, but also low enough so it doesn't influence
        // the extra time the connection is active (e.g. if app is put in the background)
        private const val STARTUP_MINIMUM_LIFETIME_MS = 5_000L

        private val JSON_CONFIGURATION = Json {
            // Ensure decoding doesn't fail if there's new keys we didn't know about (e.g. keys that represents
            // additional info that isn't relevant for older versions of the client)
            ignoreUnknownKeys = true

            // We (at least for now) want to be explicit about all fields
            encodeDefaults = true
        }
    }

    data class WebsocketConfig(val host: String, val path: String)

    private sealed interface ConnectionResult {
        data class ServerStatus(val serverStatus: Status) : ConnectionResult
        object CouldNotConnect : ConnectionResult
        data class ConnectionBroke(val connectionDuration: Duration) : ConnectionResult
        object UnexpectedError : ConnectionResult
    }

    private val client = if (engine != null) {
        HttpClient(engine = engine) {
            installPlugins()
        }
    } else {
        HttpClient {
            installPlugins()
        }
    }

    private var lastActionId = 0

    private sealed interface IncomingResponseEvent {
        data class RegularResponse(val response: Response) : IncomingResponseEvent
        object NotStarted : IncomingResponseEvent
        object Ready : IncomingResponseEvent
        object Closed : IncomingResponseEvent
    }

    private val outgoingRequests = Channel<Pair<Request, SingleResumeContinuation>>()
    private val incomingResponses: SharedFlow<IncomingResponseEvent> = channelFlow {

        // Mainly to make sure send can drop this event safely and still get notified about an early close
        send(IncomingResponseEvent.NotStarted)

        while (true) {
            waitForServerStartAllowed(incomingResponses = this)

            val connectionResult = runServerConnection(incomingResponses = this)

            // We want to emit close no matter what to make sure subscribe works properly - even though it might impact
            // the experience with single-send calls (like calling send with an add action), as they (at the moment of
            // writing) will be cancelled with an error when we emit close
            send(IncomingResponseEvent.Closed)

            val resultingServerStatus = when (connectionResult) {
                is ConnectionResult.ServerStatus -> connectionResult.serverStatus
                is ConnectionResult.CouldNotConnect -> null
                is ConnectionResult.ConnectionBroke -> {
                    if (connectionResult.connectionDuration > CONNECTION_CLOSED_RETRY_DELAY) {
                        // This could be caused by either a hiccup in the network connection, the server being
                        // temporarily down or because we switched host, so we try again right away. The duration check
                        // however makes sure we won't spam if this is called regularly
                        Logger.i(
                            "Connection has been broken after roughly ${connectionResult.connectionDuration}, so we " +
                                "just try a fast reconnect to see if it solves the issue"
                        )

                        _state.value = SmsClientState.ReadyToStart

                        continue
                    } else {
                        null
                    }
                }
                is ConnectionResult.UnexpectedError -> null
            }

            _state.value = SmsClientState.WaitingForReconnect(serverStatus = resultingServerStatus)

            val timeout = when (connectionResult) {
                is ConnectionResult.ServerStatus ->
                    // Non-open status - give the server time to fix its stuff
                    generateLongerRetryDelay()
                is ConnectionResult.CouldNotConnect,
                is ConnectionResult.ConnectionBroke ->
                    // Both of these cases is most likely just missing internet, so we'll just try again in a bit
                    CONNECTION_CLOSED_RETRY_DELAY
                is ConnectionResult.UnexpectedError ->
                    // We don't expect this will be fixed right away
                    generateLongerRetryDelay()
            }

            Logger.w(
                "Web socket connection closed (or never opened) due to $connectionResult. " +
                    "Trying to open it again after $timeout"
            )

            delay(timeout)
        }
    }.onCompletion {
        Logger.i("Web socket connection was closed as no more was subscribing to it.")

        // Even if it was in failed state we don't want to keep showing an error/warning to the user as we won't keep
        // trying to connect at this point and the user can thus not fix it anymore
        _state.update { state ->
            when (state) {
                SmsClientState.StopRequested,
                SmsClientState.Stopped -> SmsClientState.Stopped
                SmsClientState.ReadyToStart,
                SmsClientState.Starting,
                SmsClientState.Ready,
                is SmsClientState.WaitingForReconnect -> SmsClientState.ReadyToStart
            }
        }

        // Note that it doesn't make sense to emit IncomingResponseEvent.Closed here as the shared flow isn't collecting
        // anymore and thus that value would never be picked up
    }.shareIn(
        scope = scope,
        // We indirectly have a delay in the end as we expect the subscribers to set one, but we have some startup
        // buffer in case the callers aren't having any subscribers (i.e. a view that's not showing any updatable info).
        // This could have resulted in an issue where the user for instance couldn't sign in if there at the same time
        // wasn't any subscribers, as the connection would be closed right after sign in was completed
        started = WhileSubscribedWithMinimumLifetime(minimumMilliseconds = STARTUP_MINIMUM_LIFETIME_MS),
        // Subscription needs to know latest state in order to combine with other states in subscribe()
        replay = INCOMING_RESPONSES_REPLAY
    )

    private val _state = MutableStateFlow<SmsClientState>(SmsClientState.ReadyToStart)

    val state = _state.asStateFlow()

    /**
     * Sends the [action] to the endpoint and returns the reaction to that request. This operation is limited by a
     * timeout which will result in [Answer.Error] being returned. Note that a timeout might not always mean that the
     * server is slow at responding but could also mean that some error happened such that we couldn't map the response
     * to the request (e.g. if the request or response was malformed)
     */
    suspend fun send(action: ContextAction): Answer<ReactionData> {
        val request = action.toRequest(actionId = "${++lastActionId}")

        val data = try {
            withTimeout(TOTAL_REQUEST_TIMEOUT) {
                send(request)
            }
        } catch (exception: TimeoutCancellationException) {
            Logger.w(
                messageString = "Request not sent or response not received within $TOTAL_REQUEST_TIMEOUT. " +
                    "Connection state: ${_state.value}; request: $request",
                throwable = exception
            )

            null
        }

        return if (data != null) {
            Answer.Data(data = data)
        } else {
            Answer.Error(localizedMessage = null)
        }
    }

    /**
     * Subscribes to some information in the endpoint based on what is returned by [subscribeAction] (and
     * [unsubscribeAction] should then of course unsubscribe that information). The subscription is expected to keep
     * trying to get information which means it won't stop in case of broken connection or alike, but it will just wait
     * until the connection is established (again) and retry. In cases of no connection it might be good to keep an eye
     * on [state] so the user know something is up
     */
    fun subscribe(
        subscribeAction: () -> ContextAction,
        unsubscribeAction: () -> ContextAction,
        preSubscribeCheck: suspend () -> Unit = {}
    ): Flow<ReactionData> {
        val actionId = "${++lastActionId}"

        var isSubscribed = false
        val hasSubscribers = MutableStateFlow(false)

        return incomingResponses.onSubscription {
            hasSubscribers.value = true
        }.onUnsubscription(scope = scope) {
            // Note that we can't just use [onCompletion] for several reasons. We wouldn't be able to call send as the
            // scope would be about to cancel. That means we won't be listening for incoming messages and the web socket
            // potentially is closed before unsubscribe is sent/handled (as there wouldn't be any more subscribers of
            // the shared web socket incoming messages flow). However, if we launch send in another scope there's as
            // well a chance for the incoming messages to shortly be subscriber-less (if this was the last subscriber)
            // and thus the socket is closed anyway. So using this custom extension seems to be the best solution

            hasSubscribers.value = false
            isSubscribed = false // We rather want to subscribe twice than not at all next time we subscribe

            val stopAction = unsubscribeAction().toRequest(actionId = actionId)
            send(stopAction)
        }.combineTransform(hasSubscribers) { event, shouldSubscribe ->
            val makesSenseToSubscribe = when (event) {
                is IncomingResponseEvent.RegularResponse -> {
                    if (event.response.actionId == actionId) {
                        emit(event.response.data)
                    }

                    // If something else started the websocket we might not get a ready event and thus we should check
                    // for the need of subscription in this case as well
                    true
                }
                is IncomingResponseEvent.NotStarted -> false
                is IncomingResponseEvent.Ready -> true
                is IncomingResponseEvent.Closed -> {
                    isSubscribed = false

                    false
                }
            }

            if (makesSenseToSubscribe && !isSubscribed && shouldSubscribe) {
                preSubscribeCheck()

                isSubscribed = startSubscription(
                    subscribeAction = subscribeAction,
                    actionId = actionId
                )
            }
        }
    }

    suspend fun changeEndpoint(
        useBeta: Boolean,
        onClearCache: () -> Unit
    ) {
        if (useBetaEndpoint == useBeta) {
            Logger.w("Trying to change to/from beta endpoint when it won't make a difference (useBeta=$useBeta)")
            return
        }

        // Requesting to stop (if it makes sense)
        _state.update { state ->
            Logger.d("Stopping SmsClient while in state $state")

            when (state) {
                SmsClientState.StopRequested,
                SmsClientState.Stopped -> {
                    Logger.unexpected("Sms client was stopping ($state) while getting yet another stop request")

                    return
                }
                SmsClientState.ReadyToStart -> SmsClientState.Stopped // We aren't started, no need to wait
                SmsClientState.Starting -> SmsClientState.StopRequested
                SmsClientState.Ready -> SmsClientState.StopRequested
                is SmsClientState.WaitingForReconnect -> SmsClientState.StopRequested
            }
        }

        // Wait until the client has fully stopped
        _state.first { state ->
            when (state) {
                SmsClientState.StopRequested -> false
                SmsClientState.Stopped -> true
                SmsClientState.ReadyToStart,
                SmsClientState.Starting,
                SmsClientState.Ready,
                is SmsClientState.WaitingForReconnect -> {
                    // We can see a few corner cases where this could happen (e.g. if the a new subscription was just
                    // about to happen when we set the stop request state)
                    Logger.w("During stop request, sms client continued with state $state")

                    _state.value = SmsClientState.StopRequested
                    false
                }
            }
        }

        onClearCache()

        useBetaEndpoint = useBeta

        Logger.d("Finished restarting SmsClient and setting it to ready again")

        _state.value = SmsClientState.ReadyToStart
    }

    private suspend fun send(request: Request): ReactionData? {
        // Important to do start the collection of incoming responses first as it both establishes the web socket connection if it is not already
        // established and makes sure we receive our message, even if it's received right after sending. Note that we use scope here to make sure
        // we're notified if the scope gets cancelled - otherwise we won't be able to get any signals as there won't be emitted an
        // IncomingResponseEvent.Closed as the incomingResponses is, well... closed (see comment in onCompletion)
        val resultDeferred = scope.async {
            val event = incomingResponses
                // We need to drop replays, mainly because we don't want to react to an old closed event
                .drop(INCOMING_RESPONSES_REPLAY)
                .firstOrNull { event ->
                    when (event) {
                        is IncomingResponseEvent.RegularResponse ->
                            event.response.actionId == request.actionId
                        is IncomingResponseEvent.NotStarted -> false
                        is IncomingResponseEvent.Ready -> false
                        is IncomingResponseEvent.Closed -> {
                            Logger.i("Server connection seems to have closed while processing $request: $event")

                            true
                        }
                    }
                }

            (event as? IncomingResponseEvent.RegularResponse)?.response
        }

        // Note that we use coroutineScope { } here instead of the scope of the client as we can't be sure whether the scope is cancelled or will be.
        // In this way we make sure we will catch any cancellations via select/onAwait and still return something instead of rethrowing cancellation
        coroutineScope {
            suspendCancellableCoroutine { rawContinuation: CancellableContinuation<Unit> ->
                // Once the request has been handed over via outgoingRequests, the receiver of it owns the continuation
                // and will resume it when the request has been sent. We might however be cancelled in exactly that
                // window, in which case we want to resume it ourselves (see below) - so both of us could end up
                // resuming the very same continuation, which SingleResumeContinuation makes safe
                val continuation = SingleResumeContinuation(rawContinuation)

                launch {
                    try {
                        select {
                            resultDeferred.onAwait {
                                continuation.resume()
                            }
                            outgoingRequests.onSend(Pair(request, continuation)) {}
                        }
                    } catch (scopeCancellation: CancellationException) {
                        // The client scope got cancelled, but we want to return something to the caller of send()
                        // instead of rethrowing the cancellation. Note that this is also reached if we get cancelled
                        // right after the request was handed over, in which case the resume is a no-op
                        continuation.resume()
                    }
                }
            }
        }

        val result = try {
            withTimeout(AFTER_REQUEST_SENT_TIMEOUT) {
                try {
                    resultDeferred.await()
                } catch (scopeCancellation: CancellationException) {
                    // The client scope got cancelled, but we want to return this to the caller of send() instead of
                    // rethrowing the cancellation
                    null
                }
            }
        } catch (exception: TimeoutCancellationException) {
            Logger.w(
                messageString = "Server did not respond within $AFTER_REQUEST_SENT_TIMEOUT. " +
                    "Connection state: ${_state.value}; request: $request",
                throwable = exception
            )

            null
        }

        return result?.data
    }

    private suspend fun startSubscription(
        subscribeAction: () -> ContextAction,
        actionId: String
    ): Boolean {
        val startAction = subscribeAction()
        val subscribeRequest = startAction.toRequest(actionId = actionId)

        val result = send(subscribeRequest)

        return result != null
    }


    private suspend fun waitForServerStartAllowed(incomingResponses: SendChannel<IncomingResponseEvent>) {
        when (val state = state.value) {
            SmsClientState.StopRequested,
            SmsClientState.Stopped -> {
                // This can happen, but we don't want to start until whatever stopped (or requested stop of) the
                // server is resolved so we'll let the caller know we're closing down until further notice
                while (true) {
                    val (_, continuation) = outgoingRequests.receive()
                    incomingResponses.send(IncomingResponseEvent.Closed)
                    continuation.resume() // TODO: Could we avoid this by handling it in send?
                }
            }
            SmsClientState.ReadyToStart -> {}
            SmsClientState.Starting,
            SmsClientState.Ready -> {
                // We don't expect to get into this case, but we hope it won't look funky to the user
                Logger.unexpected("Sms client had unexpected state while starting: $state")
            }
            is SmsClientState.WaitingForReconnect -> {}
        }
    }

    private suspend fun runServerConnection(
        incomingResponses: SendChannel<IncomingResponseEvent>
    ): ConnectionResult {
        val host = if (!useBetaEndpoint) {
            SERVER_PROD_HOST
        } else {
            SERVER_BETA_HOST
        }

        Logger.i("Connecting to SMS server with host $host")

        val connectionResult = checkServerStatus(host = host)

        Logger.i("Check server status result: $connectionResult")

        when (connectionResult) {
            is ConnectionResult.ServerStatus -> when (connectionResult.serverStatus.type) {
                Status.Type.Open -> {} // We can continue
                Status.Type.Restricted,
                Status.Type.Maintenance,
                Status.Type.Error,
                Status.Type.Outdated,
                Status.Type.Unknown -> {
                    return connectionResult
                }
            }
            is ConnectionResult.CouldNotConnect,
            is ConnectionResult.ConnectionBroke,
            is ConnectionResult.UnexpectedError -> {
                return connectionResult
            }
        }

        return try {
            val startTime = TimeHelper.now
            runWebSocket(
                host = host,
                incomingResponses = incomingResponses
            )

            ConnectionResult.ConnectionBroke(connectionDuration = TimeHelper.now - startTime)
        } catch (exception: CancellationException) {
            throw exception
        } catch (exception: IOException) {
            Logger.w("Got exception while running web socket", exception)
            // TODO - analytics

            ConnectionResult.CouldNotConnect
        } catch (exception: Exception) {
            Logger.unexpected("Got unknown exception while running web socket", exception)

            ConnectionResult.UnexpectedError
        }
    }

    private suspend fun checkServerStatus(host: String): ConnectionResult {
        return try {
            val result = client.get("https://$host$SERVER_STATUS_PATH")
            if (result.status.value >= 400) {
                Logger.unexpected("Got unexpected result from endpoint while getting status: ${result.status}")

                return ConnectionResult.UnexpectedError
            }

            val status = result.body<Status>()
            if (status.type == Status.Type.Unknown) {
                Logger.unexpected("Got unknown status type with message: ${status.localizedMessage}")
            }

            ConnectionResult.ServerStatus(serverStatus = status)
        } catch (exception: CancellationException) {
            throw exception
        } catch (exception: IOException) {
            Logger.w("Got exception while getting sms status", exception)
            // TODO - analytics
            ConnectionResult.CouldNotConnect
        } catch (exception: ContentConvertException) {
            Logger.unexpected("Could not deserialize sms status", exception)

            ConnectionResult.UnexpectedError
        } catch (exception: Exception) {
            Logger.unexpected("Got unknown exception while getting sms status", exception)

            ConnectionResult.UnexpectedError
        }
    }

    private suspend fun runWebSocket(host: String, incomingResponses: SendChannel<IncomingResponseEvent>) {
        Logger.i("Starting connect to web socket")

        _state.value = SmsClientState.Starting
        client.createSocket(
            WebsocketConfig(
                host = host,
                path = SERVER_STREAM_PATH
            )
        ) {
            val outgoingRequestJob = launch {
                Logger.i("Started listening for outgoing messages")
                while (true) {
                    val (request, continuation) = outgoingRequests.receive()

                    Logger.d("Sending request: $request")

                    try {
                        val text = Json.encodeToString(RequestSerializerInterceptor, request)

                        val sendJob = launch {
                            outgoing.send(Frame.Text(text))
                        }

                        continuation.invokeOnCancellation {
                            sendJob.cancel()
                        }

                        sendJob.join()
                    } catch (exception: SerializationException) {
                        Logger.unexpected("Could not serialize outgoing message: $request", exception)
                        continue
                    } finally {
                        continuation.resume()
                    }

                }
            }

            incomingResponses.send(IncomingResponseEvent.Ready)
            _state.value = SmsClientState.Ready

            Logger.i("Web socket client ready. Started listening for incoming messages")
            try {
                while (true) {
                    val result = incoming.receiveCatching()
                    if (result.isClosed) {
                        Logger.w("Web socket closed without close frame: $result")

                        // TODO - log to analytics?
                        return@createSocket
                    } else if (result.isFailure) {
                        Logger.unexpected("Web socket was in an error state while reading from socket: $result")
                        continue
                    }

                    val message = when (val frame = result.getOrNull()) {
                        is Frame.Binary -> {
                            Logger.unexpected("Received binary frame from web socket")
                            continue
                        }

                        is Frame.Close -> {
                            Logger.i("Web socket closed with close frame. Reason: ${frame.readReason()}")
                            return@createSocket
                        }

                        is Frame.Ping -> {
                            Logger.i("Received ping: $frame")
                            continue
                        }

                        is Frame.Pong -> {
                            Logger.i("Received pong: $frame")
                            continue
                        }

                        is Frame.Text -> frame
                        else -> {
                            Logger.unexpected("Got unknown type of frame: $frame")
                            continue
                        }
                    }

                    val messageText = message.readText()
                    val response = try {
                        // Decode message
                        JSON_CONFIGURATION.decodeFromString(
                            deserializer = ResponseSerializerInterceptor,
                            string = messageText
                        )
                    } catch (exception: ResponseSerializerInterceptor.ConversionException) {
                        Logger.unexpected(
                            messageString = "Could not serialize incoming response: $messageText",
                            throwable = exception
                        )
                        continue
                    } catch (exception: SerializationException) {
                        Logger.unexpected(
                            messageString = "Could not serialize (parts of) incoming message: $messageText",
                            throwable = exception
                        )
                        continue
                    }

                    Logger.d("Received response: $response")

                    incomingResponses.send(IncomingResponseEvent.RegularResponse(response))
                }
            } finally {
                outgoingRequestJob.cancel()
            }
        }
    }

    private fun HttpClientConfig<*>.installPlugins() {
        install(WebSockets)
        install(ContentNegotiation) {
            json(JSON_CONFIGURATION)
        }
    }

    private fun ContextAction.toRequest(actionId: String) = Request(
        context = context.contextKey,
        action = action.actionKey,
        actionId = actionId,
        data = this
    )
}
