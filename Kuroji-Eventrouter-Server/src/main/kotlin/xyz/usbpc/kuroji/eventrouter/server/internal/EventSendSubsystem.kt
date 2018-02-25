package xyz.usbpc.kuroji.eventrouter.server.internal

import kotlinx.coroutines.experimental.Job
import kotlinx.coroutines.experimental.future.await
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.newFixedThreadPoolContext
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.x.async.AsyncCuratorFramework
import xyz.usbpc.kuroji.eventrouter.server.getLogger

class EventSendSubsystem(val client: CuratorFramework, val messageMultiplier: MessageMultiplier) {

    private val aclient: AsyncCuratorFramework = AsyncCuratorFramework.wrap(client.usingNamespace("eventrouter"))
    private val knownSubGroups = mutableMapOf<String, SubGroupManager>()
    private val controlContext = newFixedThreadPoolContext(4, "EventSendSubsystem-Control")
    private val controlJob: Job

    companion object {
        private val LOGGER = this.getLogger()
    }

    init {
        controlJob = launch(controlContext) {
            while (isActive) {
                //Look and watch for children of the "/clients" znode
                val stage = aclient.watched().children.forPath("/clients")
                LOGGER.trace("Getting all children")
                //Get all child znodes of "/clients"
                val subGroups = stage.await()
                //Calculate all SubGroups not present on zookeeper anymore
                val delSubGroups = knownSubGroups.keys - subGroups
                //Calculate all SubGroups new in zookeeper now
                val newSubGroups = subGroups - knownSubGroups.keys
                //Bring the knownSubGroups up to speed
                LOGGER.trace("Shutting down all Managers for deleted SubGroups")
                knownSubGroups.filterKeys { it in delSubGroups }.forEach { (key, subGroupManager) ->
                    subGroupManager.shutdown()
                    knownSubGroups.remove(key)
                }
                LOGGER.trace("Starting up Managers for new SubGroups")
                //Create new SubGroupManagers
                newSubGroups.forEach { name ->
                    val subGroupManager = SubGroupManager(name, client, messageMultiplier)
                    subGroupManager.start(controlContext)
                    knownSubGroups[name] = subGroupManager
                }
                LOGGER.trace("Waiting for some change in the SubGroups")
                //Wait until there is some change then repeat
                stage.event().await()
            }

        }
    }

    suspend fun awaitTermination() {
        controlJob.join()
    }

    suspend fun shutdown() {
        controlJob.cancel()
        knownSubGroups.values.forEach { subGroupManager ->
            subGroupManager.shutdown()
        }
    }
}