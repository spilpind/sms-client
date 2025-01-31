package dk.spilpind.sms.client.socket

/**
 * A generic result as response to a request
 */
sealed interface RequestResult<out ResultDataType> {

    /**
     * The result of the request. This is not necessarily a successful result
     */
    data class Result<ResultDataType>(val data: ResultDataType) : RequestResult<ResultDataType>

    /**
     * It wasn't possible to get a result. If provided, [localizedMessage] can be shown to the user to explain what went
     * wrong - otherwise a more generic error should probably be used
     */
    data class Error(val localizedMessage: String?) : RequestResult<Nothing>
}
