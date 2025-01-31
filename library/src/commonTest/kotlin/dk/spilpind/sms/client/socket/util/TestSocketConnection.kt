package dk.spilpind.sms.client.socket.util

import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.plugins.websocket.*
import io.ktor.util.*
import io.ktor.websocket.*
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.GlobalScope.coroutineContext
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.coroutineScope
import kotlin.coroutines.CoroutineContext

class TestSocketConnection {
    private val _incoming = Channel<Frame>()
    private val _outgoing = Channel<Frame>()

    val clientIncoming: SendChannel<Frame> = _incoming

    val clientOutgoing: ReceiveChannel<Frame> = _outgoing

    var startCount = 0
        private set

    suspend fun startFakeSocket(
        client: HttpClient,
        block: suspend DefaultClientWebSocketSession.() -> Unit
    ) = coroutineScope {
        ++startCount

        val coroutineContext = coroutineContext
        val webSocketSession = object : DefaultWebSocketSession {
            override val incoming: ReceiveChannel<Frame> = _incoming
            override val outgoing: SendChannel<Frame> = _outgoing
            override val coroutineContext: CoroutineContext = coroutineContext

            override var pingIntervalMillis: Long
                get() = throw NotImplementedError()
                set(_) {}
            override var timeoutMillis: Long
                get() = throw NotImplementedError()
                set(_) {}
            override val closeReason: Deferred<CloseReason?>
                get() = throw NotImplementedError()
            override val extensions: List<WebSocketExtension<*>>
                get() = throw NotImplementedError()
            override var masking: Boolean
                get() = throw NotImplementedError()
                set(_) = throw NotImplementedError()
            override var maxFrameSize: Long
                get() = throw NotImplementedError()
                set(_) = throw NotImplementedError()

            override suspend fun flush() {
                throw NotImplementedError()
            }

            @InternalAPI
            override fun start(negotiatedExtensions: List<WebSocketExtension<*>>) {
                throw NotImplementedError()
            }

            @Deprecated("See super class", replaceWith = ReplaceWith("cancel()", "kotlinx.coroutines.cancel"))
            override fun terminate() {
                throw IllegalStateException("Deprecated")
            }

        }

        DefaultClientWebSocketSession(
            call = HttpClientCall(client),
            delegate = webSocketSession
        ).block()
    }
}
