package abacus.actors

import akka.actor.ActorSystem
import akka.dispatch.{PriorityGenerator, UnboundedStablePriorityMailbox}
import com.typesafe.config.Config
import abacus.actors.DgimActor.QueryAll

/* DgimActor mailbox that prioritizes query messages. */
class QueryPriorityMailbox(settings: ActorSystem.Settings, config: Config)
  extends UnboundedStablePriorityMailbox(
    // Lower values are higher priority
    PriorityGenerator {
      case QueryAll(k, topN) => 0
      case _ => 1
    })