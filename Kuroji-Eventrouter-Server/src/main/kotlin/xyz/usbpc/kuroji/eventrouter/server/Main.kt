package xyz.usbpc.kuroji.eventrouter.server

import com.google.protobuf.util.JsonFormat
import io.grpc.Metadata
import io.grpc.stub.MetadataUtils
import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.future.await
import kotlinx.coroutines.experimental.guava.await
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.x.async.AsyncCuratorFramework
import org.apache.curator.x.async.api.CreateOption
import org.apache.zookeeper.CreateMode
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import xyz.usbpc.kuroji.eventrouter.api.KurojiEventrouter
import xyz.usbpc.kuroji.eventrouter.server.internal.MessageMultiplier
import xyz.usbpc.kuroji.eventrouter.server.internal.SubGroupManager
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread

fun Any.getLogger() : Logger =
    if (this.javaClass.name.endsWith("\$Companion"))
        LoggerFactory.getLogger(this.javaClass.declaringClass)
    else
        LoggerFactory.getLogger(this.javaClass)

fun main(args: Array<String>) = runBlocking {
    val LOGGER = ::main.getLogger()
    val rawClient = newCuratorFrameworkClient(args[0])
    rawClient.start()
    val client = rawClient.usingNamespace("eventrouter")
    try {
        client.blockUntilConnected(30, TimeUnit.SECONDS)
    } catch (ex: InterruptedException) {
        LOGGER.error("Could not connect to to zookeeper in 30 seconds.")
        return@runBlocking
    }
    val messageMultiplier = MessageMultiplier()
    //TODO replace this with a working configuration for receiving things
    launch(newSingleThreadContext("JustToTest")) {
        val jsonParse = JsonFormat.parser()
        var routing = 0L
        while (isActive) {
            val eventBuilder = KurojiEventrouter.Event.newBuilder()
            eventBuilder.botId = 10
            eventBuilder.shardId = 55
            eventBuilder.traceId = "nope"
            eventBuilder.routing = KurojiEventrouter.RoutingInfo.newBuilder().setId(routing++).build()
            routing %= 30
            eventBuilder.type = KurojiEventrouter.EventType.MESSAGE_CREATE
            //eventBuilder.event = com.google.protobuf.Any.pack(messageBuilder.build())
            messageMultiplier.onEvent(eventBuilder.build())
            //println("Send one Message!")
            delay(10)
        }
    }
    val aclient = AsyncCuratorFramework.wrap(client)
    aclient.createNode("/clients", options = setOf(CreateOption.createParentsIfNeeded, CreateOption.setDataIfExists))

    //Start thing that does the sending... so create channel, nameresolver etc.
    val controlContext = newFixedThreadPoolContext(4, "Control-Thread")
    val knownSubGroups = mutableMapOf<String, SubGroupManager>()
    LOGGER.info("Starting main control job")
    val mainControlJob = launch(controlContext) {
        val LOGGER = this.getLogger()
        try {
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
        } catch (ex: CancellationException) {
            LOGGER.info("We have been canceled!")
        } finally {
        }

    }

    LOGGER.debug("Registering shutdown hook")
    //Tell the JVM what we want to do on shutdown
    Runtime.getRuntime().addShutdownHook(
            thread (start = false, isDaemon = false, name = "Shutdown") {
                runBlocking {
                    LOGGER.info("Shutting down...")
                    mainControlJob.cancel()
                    knownSubGroups.values.forEach { subGroupManager ->
                        subGroupManager.shutdown()
                    }
                }
                rawClient.close()
                LOGGER.info("Bye <3")
            }
    )

    mainControlJob.join()

}

fun newCuratorFrameworkClient(connectString: String, baseSleepTime: Int = 1000, maxRetries: Int = 3) : CuratorFramework {
    val retryPolicy = ExponentialBackoffRetry(1000, 3)
    return CuratorFrameworkFactory.newClient(connectString, retryPolicy)
}

suspend fun AsyncCuratorFramework.createNode(path: String, options: Set<CreateOption> = setOf(CreateOption.createParentsIfNeeded), mode: CreateMode = CreateMode.PERSISTENT, data: ByteArray? = null) : String {
    val a = create().withOptions(options, mode)
    val b = if (data == null) {
        a.forPath(path)
    } else {
        a.forPath(path, data)
    }
    return b.await()
}