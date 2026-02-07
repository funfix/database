/*
 * Copyright 2026 Alexandru Nedelcu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.funfix.delayedqueue.jvm

/**
 * Strategy for serializing and deserializing message payloads to/from binary data.
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
     * Serializes a payload to a byte array.
     *
     * @param payload the payload to serialize
     * @return the serialized byte representation
     */
    public fun serialize(payload: A): ByteArray

    /**
     * Deserializes a payload from a byte array.
     *
     * @param serialized the serialized bytes
     * @return the deserialized payload
     * @throws IllegalArgumentException if the serialized string cannot be parsed
     */
    @Throws(IllegalArgumentException::class) public fun deserialize(serialized: ByteArray): A

    public companion object {
        /** Creates a serializer for String payloads (identity serialization). */
        @JvmStatic
        public fun forStrings(): MessageSerializer<String> =
            object : MessageSerializer<String> {
                override fun getTypeName(): String = "java.lang.String"

                override fun serialize(payload: String): ByteArray =
                    payload.toByteArray(Charsets.UTF_8)

                override fun deserialize(serialized: ByteArray): String =
                    String(serialized, Charsets.UTF_8)
            }
    }
}
