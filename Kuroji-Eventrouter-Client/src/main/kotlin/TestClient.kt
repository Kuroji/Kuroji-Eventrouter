import io.grpc.ServerBuilder
import io.grpc.stub.StreamObserver
import kotlinx.coroutines.experimental.future.await
import kotlinx.coroutines.experimental.runBlocking
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.x.async.AsyncCuratorFramework
import org.apache.curator.x.async.api.CreateOption
import org.apache.zookeeper.CreateMode
import xyz.usbpc.kuroji.eventrouter.api.KurojiEventrouter
import xyz.usbpc.kuroji.eventrouter.api.MessageRouterSubscriberGrpc
import java.net.InetAddress
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread

fun main(args: Array<String>) = runBlocking {
    val zookeeper = EventRouterSubscriber("totallyNew", args[0].toInt(), setOf(KurojiEventrouter.EventType.MESSAGE_CREATE))
    val server = ServerBuilder.forPort(args[0].toInt())
            .addService(MessageRouterSubscriber())
            .build()
            .start()
    zookeeper.aregister()

    Runtime.getRuntime().addShutdownHook(
            thread (start = false, isDaemon = false) {
                println("Shutting down...")
                zookeeper.shutdown()
                server.shutdown()
                println("Bye <3")
            }
    )
    server.awaitTermination()
}

class MessageRouterSubscriber : MessageRouterSubscriberGrpc.MessageRouterSubscriberImplBase() {
    override fun onEvent(request: KurojiEventrouter.Event, responseObserver: StreamObserver<KurojiEventrouter.SubResponse>) {
        println("Got a message! (${request.routing.id})")
        responseObserver.onNext(KurojiEventrouter.SubResponse.getDefaultInstance())
        responseObserver.onCompleted()
    }
}

class EventRouterSubscriber(val consumerGroupName: String, val port: Int, val topics: Set<KurojiEventrouter.EventType>, zookeeper: String = "localhost:2181") {
    val aclient: AsyncCuratorFramework
    val client: CuratorFramework
    val subData = KurojiEventrouter.Subscriber.newBuilder()
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
    suspend fun aregister() {
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
    suspend fun astartShutdown() {
        synchronized(subData) {
            subData.status = KurojiEventrouter.Subscriber.Status.SHUTTING_DOWN
            aclient.setData().forPath(ourUrl, subData.build().toByteArray()).await()
        }
    }
    suspend fun aunregister() {
        if (ourUrl != null) {
            aclient.delete().forPath(ourUrl).await()
            ourUrl = null
        }
    }
    fun unregister() {
        if (ourUrl != null) {
            client.delete().forPath(ourUrl)
            ourUrl = null
        }
    }
    suspend fun ashutdown() {
        aunregister()
        aclient.unwrap().close()
    }
    fun shutdown() {
        unregister()
        client.close()
    }
}