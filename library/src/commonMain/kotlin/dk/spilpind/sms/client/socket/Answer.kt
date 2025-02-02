package dk.spilpind.sms.client.socket

/**
 * A generic answer as response to a request
 */
sealed interface Answer<out DataType> {

    /**
     * An answer with some [data]. This usually means the request was successful, but in the end that also depends on
     * what the [data] represents
     */
    data class Data<DataType>(val data: DataType) : Answer<DataType>

    /**
     * Getting an answer failed for some reason. If provided, [localizedMessage] can be shown to the user to explain
     * what went wrong - otherwise a more generic error could be used
     */
    data class Error(val localizedMessage: String?) : Answer<Nothing>
}
