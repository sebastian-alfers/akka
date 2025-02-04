/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.typed.scaladsl

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.typed.{ PersistenceId, RecoveryCompleted }

//import akka.persistence.typed.RecoveryCompleted

object EventSourcedSequenceNumberBasicSpec {

  private val conf = ConfigFactory.parseString(s"""
      akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
      akka.persistence.journal.inmem.test-serialization = on
      akka.persistence.snapshot-store.plugin = "slow-snapshot-store"
      slow-snapshot-store.class = "${classOf[SlowInMemorySnapshotStore].getName}"
    """)

}

class EventSourcedSequenceNumberBasicSpec
    extends ScalaTestWithActorTestKit(EventSourcedSequenceNumberBasicSpec.conf)
    with AnyWordSpecLike
    with LogCapturing {

  private def behavior(
      pid: PersistenceId,
      probe: ActorRef[String],
      sendOnRecoveryCompleted: Boolean): Behavior[String] =
    Behaviors.setup(ctx =>
      EventSourcedBehavior[String, String, String](pid, "", { (_, _) =>
        val nr = EventSourcedBehavior.lastSequenceNumber(ctx)
        println(s"command: ******* $nr")
        probe ! s"$nr command"
        Effect.persist("event")
      }, { (_, evt) =>
        val nr = EventSourcedBehavior.lastSequenceNumber(ctx)
        println(s"event: ******* $nr")
        probe ! s"$nr event"
        evt
      }).receiveSignal {
        case (_, RecoveryCompleted) =>
          if (sendOnRecoveryCompleted) {
            val nr = EventSourcedBehavior.lastSequenceNumber(ctx)
            println(s"recov: ******* $nr")
            probe ! s"${EventSourcedBehavior.lastSequenceNumber(ctx)} onRecoveryComplete"
          }
      })

  "The sequence number" must {

    def runTheSpec(testProbe: Boolean) = {
      val probe = TestProbe[String]()

      val ref = spawn(behavior(PersistenceId.ofUniqueId("ess-1"), probe.ref, testProbe))

      if (testProbe) {
        probe.expectMessage("0 onRecoveryComplete")
      }

      ref ! "cmd"
      probe.expectMessage("0 command")
      probe.expectMessage("1 event")

      ref ! "cmd"
      probe.expectMessage("1 command")
      probe.expectMessage("2 event")
    }

    "work" in {
      runTheSpec(testProbe = true)
    }

    "also work" in {
      runTheSpec(testProbe = false)
    }
  }
}
