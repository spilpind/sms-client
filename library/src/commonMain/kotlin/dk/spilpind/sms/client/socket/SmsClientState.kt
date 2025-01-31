package dk.spilpind.sms.client.socket

import dk.spilpind.sms.api.core.Status

/**
 * State of the SMS client connection
 */
sealed interface SmsClientState {

    /**
     * A stop has been requested and all resources related to the current state of the sms client are expected to be
     * released
     */
    object StopRequested : SmsClientState

    /**
     * The sms client is stopped until it's manually started again with [ReadyToStart]
     */
    object Stopped : SmsClientState

    /**
     * The sms client is ready to be started. If it was previously closed because of an error or because the server
     * didn't allow a connection, [WaitingForReconnect] is used instead
     */
    object ReadyToStart : SmsClientState

    /**
     * The socket is starting up, it isn't ready for consuming requests yet
     */
    object Starting : SmsClientState

    /**
     * The connection is up and running and ready to consume requests
     */
    object Ready : SmsClientState

    /**
     * The socket is closed due to an error or due to the server not allowing a connection at the moment. [serverStatus]
     * can be used to determine the cause - if it's null it most likely means the app is missing internet connection.
     * During this state, the sms client will wait for a certain period of time before attempting a reconnect
     */
    data class WaitingForReconnect(val serverStatus: Status?) : SmsClientState
}
