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
import scala.util.control.NonFatal

/** Type-class for encoding and decoding message payloads to/from binary data.
  *
  * This is used by JDBC implementations to store message payloads in the
  * database.
  *
  * @tparam A
  *   the type of message payloads
  */
trait PayloadCodec[A] {

  /** Returns the fully-qualified type name of the messages this codec handles.
    *
    * This is used for queue partitioning and message routing.
    *
    * @return
    *   the fully-qualified type name (e.g., "java.lang.String")
    */
  def typeName: String

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
    *   Either the deserialized payload or an IllegalArgumentException if
    *   parsing fails
    */
  def deserialize(serialized: Array[Byte]): Either[IllegalArgumentException, A]

  /** Converts this Scala PayloadCodec to a JVM MessageSerializer. */
  def asJava: jvm.MessageSerializer[A] =
    new jvm.MessageSerializer[A] {
      override def getTypeName: String =
        PayloadCodec.this.typeName
      override def serialize(payload: A): Array[Byte] =
        PayloadCodec.this.serialize(payload)
      override def deserialize(serialized: Array[Byte]): A =
        PayloadCodec.this.deserialize(serialized) match {
          case Right(value) => value
          case Left(error)  => throw error
        }
    }
}

object PayloadCodec {

  /** Given instance for String payloads using UTF-8 encoding.
    *
    * This is based on the JVM MessageSerializer.forStrings implementation.
    */
  given forStrings: PayloadCodec[String] =
    fromJava(jvm.MessageSerializer.forStrings())

  /** Wraps a JVM MessageSerializer to provide a Scala PayloadCodec interface.
    */
  def fromJava[A](javaSerializer: jvm.MessageSerializer[A]): PayloadCodec[A] =
    new PayloadCodec[A] {
      override def typeName: String =
        javaSerializer.getTypeName
      override def serialize(payload: A): Array[Byte] =
        javaSerializer.serialize(payload)
      override def deserialize(serialized: Array[Byte]): Either[IllegalArgumentException, A] =
        try
          Right(javaSerializer.deserialize(serialized))
        catch {
          case e: IllegalArgumentException => Left(e)
          case NonFatal(e)                 => Left(new IllegalArgumentException(e.getMessage, e))
        }
    }
}
