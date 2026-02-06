package org.funfix.delayedqueue.jvm

/**
 * Strategy for serializing and deserializing message payloads to/from strings.
 *
 * This is used by JDBC implementations to store message payloads in the database.
 *
 * @param A the type of message payloads
 */
public interface MessageSerializer<A> {
    /**
     * Returns the fully-qualified type name of the messages this serializer handles.
     *
     * This is used for queue partitioning and message routing.
     *
     * @return the fully-qualified type name (e.g., "java.lang.String")
     */
    public fun getTypeName(): String

    /**
     * Serializes a payload to a string.
     *
     * @param payload the payload to serialize
     * @return the serialized string representation
     */
    public fun serialize(payload: A): String

    /**
     * Deserializes a payload from a string.
     *
     * @param serialized the serialized string
     * @return the deserialized payload
     * @throws IllegalArgumentException if the serialized string cannot be parsed
     */
    @Throws(IllegalArgumentException::class) public fun deserialize(serialized: String): A

    public companion object {
        /** Creates a serializer for String payloads (identity serialization). */
        @JvmStatic
        public fun forStrings(): MessageSerializer<String> =
            object : MessageSerializer<String> {
                override fun getTypeName(): String = "java.lang.String"

                override fun serialize(payload: String): String = payload

                override fun deserialize(serialized: String): String = serialized
            }
    }
}
