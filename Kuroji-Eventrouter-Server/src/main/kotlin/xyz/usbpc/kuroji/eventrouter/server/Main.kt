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
import xyz.usbpc.kuroji.eventrouter.api.KurojiEventrouter
import xyz.usbpc.kuroji.eventrouter.server.internal.MessageMultiplier
import xyz.usbpc.kuroji.eventrouter.server.internal.SubGroupManager
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread

fun main(args: Array<String>) = runBlocking {
    val rawClient = newCuratorFrameworkClient(args[0])
    rawClient.start()
    val client = rawClient.usingNamespace("eventrouter")
    try {
        client.blockUntilConnected(30, TimeUnit.SECONDS)
    } catch (ex: InterruptedException) {
        //TODO logging
        println("Did not connect with zookeeper in 30 seconds")
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
    //TODO logging
    val knownSubGroups = mutableMapOf<String, SubGroupManager>()
    println("Starting main control job...")
    val mainControlJob = launch(controlContext) {
        try {
            while (isActive) {
                //Look and watch for children of the "/clients" znode
                val stage = aclient.watched().children.forPath("/clients")

                println("Getting all children")
                //Get all child znodes of "/clients"
                val subGroups = stage.await()
                println("Looking at what SubGroups were deleted")
                //Calculate all SubGroups not present on zookeeper anymore
                val delSubGroups = knownSubGroups.keys - subGroups
                println("Looking at what SubGroups were created")
                //Calculate all SubGroups new in zookeeper now
                val newSubGroups = subGroups - knownSubGroups.keys
                //Bring the knownSubGroups up to speed
                println("Doing things with the deleted sub groups")
                knownSubGroups.filterKeys { it in delSubGroups }.forEach { (key, subGroupManager) ->
                    subGroupManager.shutdown()
                    knownSubGroups.remove(key)
                }
                println("Doing things with the new sub groups")
                //Create new SubGroupManagers
                newSubGroups.forEach { name ->
                    val subGroupManager = SubGroupManager(name, client, messageMultiplier)
                    subGroupManager.start(controlContext)
                    knownSubGroups[name] = subGroupManager
                }

                println("Waiting for something to change")
                //Wait until there is some change then repeat
                stage.event().await()
            }
        } catch (ex: CancellationException) {
            //TODO logging
            println("We have been canceled!")
        } finally {
            //Let's clean our mess!
            //TODO logging

        }

    }
    //TODO logging
    println("Main control job started...")

    println("Registering shutdown hook...")

    //Tell the JVM what we want to do on shutdown
    Runtime.getRuntime().addShutdownHook(
            thread (start = false, isDaemon = false) {
                runBlocking {
                    //TODO logging
                    println("Shutting down main Control job...")
                    mainControlJob.cancel()
                    println("Shutting down sending")
                    knownSubGroups.values.forEach { subGroupManager ->
                        subGroupManager.shutdown()
                    }
                    println("Shut down main control job")
                }
                println("closing connection to zookeeper")
                rawClient.close()
                println("Closed connection to zookeeper")
            }
    )
    //TODO logging
    println("Shutdown hook registered...")
    runBlocking {
        mainControlJob.join()
        println("Bye <3")
    }

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