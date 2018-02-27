package xyz.usbpc.kuroji.eventrouter.client

import kotlinx.coroutines.experimental.future.await
import kotlinx.coroutines.experimental.sync.Mutex
import kotlinx.coroutines.experimental.sync.withLock
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.x.async.AsyncCuratorFramework
import org.apache.curator.x.async.api.CreateOption
import org.apache.zookeeper.CreateMode
import xyz.usbpc.kuroji.eventrouter.api.KurojiEventrouter
import xyz.usbpc.kuroji.proto.discord.events.Event
import java.net.InetAddress
import java.util.concurrent.TimeUnit
import kotlin.coroutines.experimental.Continuation
import kotlin.coroutines.experimental.suspendCoroutine

class EventRouterSubscriber(val consumerGroupName: String, val port: Int, val topics: Set<Event.EventType>, zookeeper: String = "localhost:2181") {
    val aclient: AsyncCuratorFramework
    val client: CuratorFramework
    val subDataMutex = Mutex()
    var subData = KurojiEventrouter.Subscriber.newBuilder()
            .setHostname(InetAddress.getLocalHost().hostName)
            .setPort(port)
            .setStatus(KurojiEventrouter.Subscriber.Status.RUNNING)
    var ourUrl: String? = null
    init {
        val retryPolicy = ExponentialBackoffRetry(1000, 3)
        client = CuratorFrameworkFactory.newClient(zookeeper, retryPolicy)
        client.start()
        try {
            client.blockUntilConnected(30, TimeUnit.SECONDS)
        } catch (ex: InterruptedException) {
            //TODO: Make exception less generic
            throw Exception("Could not connect with zookeeper after 30 seconds", ex)
        }
        aclient = AsyncCuratorFramework.wrap(client)
    }
    suspend fun register() {
        //TODO: Make sure we only add to the subscribed topics... just to be sure
        val consumerGroup = KurojiEventrouter.SubscriberGroup.newBuilder().addAllTopics(topics).setName(consumerGroupName).build()
        aclient.create()
                .withOptions(setOf(CreateOption.setDataIfExists, CreateOption.createParentsIfNeeded), CreateMode.CONTAINER)
                .forPath("/eventrouter/clients/$consumerGroupName", consumerGroup.toByteArray())
                .await()
        ourUrl = aclient.create()
                .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                .forPath("/eventrouter/clients/$consumerGroupName/sub", subData.build().toByteArray())
                .await()
    }

    suspend fun claim(thing: KurojiEventrouter.RoutingInfo) {
        subDataMutex.withLock {
            subData.addClaimed(thing)
            val tmp = subData.build()
            updateZookeeper(tmp.toByteArray())
            subData = tmp.toBuilder()
        }
    }

    suspend fun release(thing: KurojiEventrouter.RoutingInfo) {
        subDataMutex.withLock {
            val newClaimed = subData.claimedList.filter { claim ->
                claim != thing
            }
            subData.clearClaimed()
            subData.addAllClaimed(newClaimed)
            val tmp = subData.build()
            updateZookeeper(tmp.toByteArray())
            subData = tmp.toBuilder()
        }
        if (this::waitingCont.isInitialized && subData.claimedList.isEmpty()) {
            waitingCont.resume(Unit)
        }
    }

    suspend fun updateZookeeper(data: ByteArray) =
            aclient.setData().forPath(ourUrl, data).await()


    suspend fun startShutdown() {
        subDataMutex.withLock {
            subData.status = KurojiEventrouter.Subscriber.Status.SHUTTING_DOWN
            val tmp = subData.build()
            updateZookeeper(tmp.toByteArray())
            subData = tmp.toBuilder()
        }
    }

    suspend fun unregister() {
        if (ourUrl != null) {
            aclient.delete().forPath(ourUrl).await()
            ourUrl = null
        }
    }
    lateinit var waitingCont : Continuation<Unit>
    suspend fun waitForAllUnclaimed() {
        subDataMutex.withLock {
            if (subData.claimedList.isEmpty())
                return
        }
        suspendCoroutine<Unit> { cont ->
            waitingCont = cont
        }
    }

    suspend fun shutdown() {
        startShutdown()
        waitForAllUnclaimed()
        unregister()
        aclient.unwrap().close()
    }
}