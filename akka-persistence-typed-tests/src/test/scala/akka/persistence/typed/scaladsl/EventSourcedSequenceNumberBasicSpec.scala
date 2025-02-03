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
import akka.persistence.typed.PersistenceId

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

  private def behavior(pid: PersistenceId, probe: ActorRef[String]): Behavior[String] =
    Behaviors.setup(
      ctx =>
        EventSourcedBehavior[String, String, String](
          pid,
          "", { (_, _) =>
            println(s"command: ******* ${EventSourcedBehavior.lastSequenceNumber(ctx)}")
            probe ! s"${EventSourcedBehavior.lastSequenceNumber(ctx)} command"
            Effect.persist("event")
          }, { (_, evt) =>
            println(s"event: ******* ${EventSourcedBehavior.lastSequenceNumber(ctx)}")
            probe ! s"${EventSourcedBehavior.lastSequenceNumber(ctx)} event"
            evt
          })
//        .receiveSignal {
//        case (_, RecoveryCompleted) =>
//          probe ! s"${EventSourcedBehavior.lastSequenceNumber(ctx)} onRecoveryComplete"
//      }
    )

  "The sequence number" must {

    "be accessible in the handlers" in {
      val probe = TestProbe[String]()
      val ref = spawn(behavior(PersistenceId.ofUniqueId("ess-1"), probe.ref))

      //probe.expectMessage("0 onRecoveryComplete")

      ref ! "cmd"
      probe.expectMessage("0 command")
      probe.expectMessage("1 event")

      ref ! "cmd"
      probe.expectMessage("1 command")
      probe.expectMessage("2 event")
    }
  }
}
