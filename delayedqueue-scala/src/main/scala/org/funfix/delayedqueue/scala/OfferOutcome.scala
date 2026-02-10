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

/** Outcome of offering a message to the delayed queue.
  *
  * This sealed trait represents the possible results when adding or updating a
  * message in the queue.
  */
sealed trait OfferOutcome {

  /** Returns true if the offer was ignored (message already exists and cannot
    * be updated).
    */
  def isIgnored: Boolean =
    this == OfferOutcome.Ignored

  /** Converts this Scala OfferOutcome to a JVM OfferOutcome. */
  def asJava: jvm.OfferOutcome =
    this match {
      case OfferOutcome.Created => jvm.OfferOutcome.Created.INSTANCE
      case OfferOutcome.Updated => jvm.OfferOutcome.Updated.INSTANCE
      case OfferOutcome.Ignored => jvm.OfferOutcome.Ignored.INSTANCE
    }
}

object OfferOutcome {

  /** Message was successfully created (new entry). */
  case object Created extends OfferOutcome

  /** Message was successfully updated (existing entry modified). */
  case object Updated extends OfferOutcome

  /** Message offer was ignored (already exists and canUpdate was false). */
  case object Ignored extends OfferOutcome

  /** Conversion extension for JVM OfferOutcome. */
  extension (javaOutcome: jvm.OfferOutcome) {

    /** Converts a JVM OfferOutcome to a Scala OfferOutcome. */
    def asScala: OfferOutcome =
      javaOutcome match {
        case _: jvm.OfferOutcome.Created => OfferOutcome.Created
        case _: jvm.OfferOutcome.Updated => OfferOutcome.Updated
        case _: jvm.OfferOutcome.Ignored => OfferOutcome.Ignored
      }
  }
}
