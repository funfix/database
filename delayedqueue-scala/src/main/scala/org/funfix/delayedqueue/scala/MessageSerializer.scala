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

package org.funfix.delayedqueue.scala

import org.funfix.delayedqueue.jvm

/** Strategy for serializing and deserializing message payloads to/from binary data.
  *
  * This is used by JDBC implementations to store message payloads in the database.
  *
  * @tparam A
  *   the type of message payloads
  */
trait MessageSerializer[A] {

  /** Returns the fully-qualified type name of the messages this serializer handles.
    *
    * This is used for queue partitioning and message routing.
    *
    * @return
    *   the fully-qualified type name (e.g., "java.lang.String")
    */
  def getTypeName: String

  /** Serializes a payload to a byte array.
    *
    * @param payload
    *   the payload to serialize
    * @return
    *   the serialized byte representation
    */
  def serialize(payload: A): Array[Byte]

  /** Deserializes a payload from a byte array.
    *
    * @param serialized
    *   the serialized bytes
    * @return
    *   the deserialized payload
    * @throws IllegalArgumentException
    *   if the serialized string cannot be parsed
    */
  @throws[IllegalArgumentException]
  def deserialize(serialized: Array[Byte]): A

  /** Converts this Scala MessageSerializer to a JVM MessageSerializer. */
  def asJava: jvm.MessageSerializer[A] =
    new jvm.MessageSerializer[A] {
      override def getTypeName(): String = MessageSerializer.this.getTypeName
      override def serialize(payload: A): Array[Byte] = MessageSerializer.this.serialize(payload)
      override def deserialize(serialized: Array[Byte]): A =
        MessageSerializer.this.deserialize(serialized)
    }
}

object MessageSerializer {

  /** Creates a serializer for String payloads (UTF-8 encoding). */
  def forStrings: MessageSerializer[String] =
    new MessageSerializer[String] {
      override def getTypeName: String = "java.lang.String"
      override def serialize(payload: String): Array[Byte] = payload.getBytes("UTF-8")
      override def deserialize(serialized: Array[Byte]): String = new String(serialized, "UTF-8")
    }

  /** Wraps a JVM MessageSerializer to provide a Scala interface. */
  def fromJava[A](javaSerializer: jvm.MessageSerializer[A]): MessageSerializer[A] =
    new MessageSerializer[A] {
      override def getTypeName: String = javaSerializer.getTypeName()
      override def serialize(payload: A): Array[Byte] = javaSerializer.serialize(payload)
      override def deserialize(serialized: Array[Byte]): A = javaSerializer.deserialize(serialized)
    }
}
