package xyz.usbpc.kuroji.eventrouter.server

import com.google.protobuf.Message
import com.google.protobuf.util.JsonFormat
import io.grpc.Server
import io.grpc.ServerBuilder
import io.grpc.stub.StreamObserver
import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.future.await
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.x.async.AsyncCuratorFramework
import org.apache.curator.x.async.api.CreateOption
import org.apache.zookeeper.CreateMode
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import xyz.astolfo.websocket.rpc.KurojiWebsocket
import xyz.astolfo.websocket.rpc.WebsocketSubscriberGrpc
import xyz.usbpc.kuroji.eventrouter.api.KurojiEventrouter
import xyz.usbpc.kuroji.eventrouter.server.internal.EventSendSubsystem
import xyz.usbpc.kuroji.eventrouter.server.internal.MessageMultiplier
import xyz.usbpc.kuroji.proto.discord.events.*
import xyz.usbpc.kuroji.proto.discord.objects.*
import xyz.usbpc.kuroji.websocket.client.WebsocketSubscriber
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread

fun Any.getLogger() : Logger =
    if (this.javaClass.name.endsWith("\$Companion"))
        LoggerFactory.getLogger(this.javaClass.declaringClass)
    else
        LoggerFactory.getLogger(this.javaClass)

fun main(args: Array<String>) = runBlocking {
    val eventrouter = Eventrouter()

    Runtime.getRuntime().addShutdownHook(
            thread(start = false, isDaemon = false) {
                runBlocking(newSingleThreadContext("Shutdown-Context")) {
                    eventrouter.stop()
                }
            }
    )

    eventrouter.awaitTermination()
}

class WSSubscriber(client: CuratorFramework, private val port: Int = 7120) : WebsocketSubscriberGrpc.WebsocketSubscriberImplBase() {
    val zoo = WebsocketSubscriber(port, client)
    lateinit var server : Server
    suspend fun start() {
        server = ServerBuilder.forPort(port)
                .addService(this)
                .build()
                .start()
        zoo.register()
    }
    suspend fun stop() {
        zoo.unregister()
        server.shutdown()
        server.awaitTermination()
    }
    override fun onEvent(request: KurojiWebsocket.RawEvent, responseObserver: StreamObserver<KurojiWebsocket.SubResponse>) {
        launch {

        }
    }
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