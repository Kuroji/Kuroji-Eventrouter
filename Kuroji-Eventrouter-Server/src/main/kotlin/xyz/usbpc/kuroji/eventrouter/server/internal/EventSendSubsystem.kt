package xyz.usbpc.kuroji.eventrouter.server.internal

import kotlinx.coroutines.experimental.Job
import kotlinx.coroutines.experimental.future.await
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.newFixedThreadPoolContext
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.x.async.AsyncCuratorFramework
import xyz.usbpc.kuroji.eventrouter.api.KurojiEventrouter
import xyz.usbpc.kuroji.eventrouter.server.SubsystemManager
import xyz.usbpc.kuroji.eventrouter.server.getLogger

interface EventSenderSubsystem : SubsystemManager {
    fun onEvent(event: KurojiEventrouter.Event)
}

/**
 * Is responsible for sending events to subscribed clients.
 * @param client CuratorFramwork connected to the zookeeper cluster where clients can subscribe.
 */
class EventSendSubsystem(val client: CuratorFramework) : EventSenderSubsystem {
    private val messageMultiplier: MessageMultiplier = MessageMultiplier()
    private val controlJob: Job
    private val aclient: AsyncCuratorFramework = AsyncCuratorFramework.wrap(client.usingNamespace("eventrouter"))
    private val knownSubGroups = mutableMapOf<String, SubGroupManager>()
    private val controlContext = newFixedThreadPoolContext(4, "EventSendSubsystem-Control")

    companion object {
        private val log = this.getLogger()
    }

    init {
        log.info("Starting up the sending subsystem.")
        controlJob = launch(controlContext) {
            while (isActive) {
                //Look and watch for children of the "/clients" znode
                val stage = aclient.watched().children.forPath("/clients")
                log.trace("Getting all children")

                //Get all child znodes of "/clients"
                val subGroups = stage.await()

                //Calculate all SubGroups not present on zookeeper anymore
                val delSubGroups = knownSubGroups.keys - subGroups

                //Calculate all SubGroups new in zookeeper now
                val newSubGroups = subGroups - knownSubGroups.keys
                //Bring the knownSubGroups up to speed
                log.trace("Shutting down all Managers for deleted SubGroups")
                knownSubGroups.filterKeys { it in delSubGroups }.forEach { (key, subGroupManager) ->
                    log.info("Removing SubGroupManager for SubGroup {}", key)
                    subGroupManager.shutdown()
                    knownSubGroups.remove(key)
                }
                log.trace("Starting up Managers for new SubGroups")

                //Create new SubGroupManagers
                newSubGroups.forEach { name ->
                    log.info("Creating SubGroupManager for SubGroup {}", name)
                    val subGroupManager = SubGroupManager(name, client, messageMultiplier)
                    subGroupManager.start(controlContext)
                    knownSubGroups[name] = subGroupManager
                }
                log.trace("Waiting for some change in the SubGroups")

                //Wait until there is some change then repeat
                stage.event().await()
            }

        }
    }

    override suspend fun stop() {
        log.info("Shutting down the sending subsystem.")
        controlJob.cancel()
        knownSubGroups.values.forEach { subGroupManager ->
            subGroupManager.shutdown()
        }
    }

    override fun onEvent(event: KurojiEventrouter.Event) = messageMultiplier.onEvent(event)

    override suspend fun awaitTermination() {
        controlJob.join()
    }

}