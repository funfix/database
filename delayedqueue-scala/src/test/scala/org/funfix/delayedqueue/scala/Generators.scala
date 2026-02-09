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

import org.scalacheck.{Arbitrary, Gen}
import scala.concurrent.duration.*
import java.time.{Instant, LocalTime, ZoneId}

/** Common ScalaCheck generators for DelayedQueue data structures. */
object Generators {

  implicit val arbFiniteDuration: Arbitrary[FiniteDuration] = Arbitrary(
    Gen.choose(0L, 1000000L).map(millis => FiniteDuration(millis, MILLISECONDS))
  )

  implicit val arbInstant: Arbitrary[Instant] = Arbitrary(
    Gen.choose(0L, System.currentTimeMillis() * 2).map(Instant.ofEpochMilli)
  )

  implicit val arbLocalTime: Arbitrary[LocalTime] = Arbitrary(
    for {
      hour <- Gen.choose(0, 23)
      minute <- Gen.choose(0, 59)
    } yield LocalTime.of(hour, minute)
  )

  implicit val arbZoneId: Arbitrary[ZoneId] = Arbitrary(
    Gen.oneOf(ZoneId.of("UTC"), ZoneId.of("America/New_York"), ZoneId.of("Europe/London"))
  )
}
