import io.grpc.ServerBuilder
import io.grpc.stub.StreamObserver
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.newSingleThreadContext
import kotlinx.coroutines.experimental.runBlocking
import xyz.usbpc.kuroji.eventrouter.api.KurojiEventrouter
import xyz.usbpc.kuroji.eventrouter.api.MessageRouterSubscriberGrpc
import xyz.usbpc.kuroji.eventrouter.client.EventRouterSubscriber
import xyz.usbpc.kuroji.proto.discord.events.Event
import xyz.usbpc.kuroji.proto.discord.objects.MessageOuterClass
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread

fun main(args: Array<String>) = runBlocking {
    val zookeeper = EventRouterSubscriber("totallyNew", args[0].toInt(), setOf(Event.EventType.MESSAGE_CREATE))
    val server = ServerBuilder.forPort(args[0].toInt())
            .addService(MessageRouterSubscriber())
            .build()
            .start()
    zookeeper.register()
    //zookeeper.claim(KurojiEventrouter.RoutingInfo.newBuilder().setId(10).build())

    Runtime.getRuntime().addShutdownHook(
            thread (start = false, isDaemon = false) {
                println("Shutting down...")
                runBlocking(newSingleThreadContext("Shutdown Coroutine")) {
                    launch(coroutineContext) {
                        delay(30, TimeUnit.SECONDS)
                        zookeeper.release(KurojiEventrouter.RoutingInfo.newBuilder().setId(10).build())
                    }
                    zookeeper.shutdown()
                }
                server.shutdown()
                println("Bye <3")
            }
    )
    server.awaitTermination()
}

class MessageRouterSubscriber : MessageRouterSubscriberGrpc.MessageRouterSubscriberImplBase() {
    override fun onEvent(request: KurojiEventrouter.Event, responseObserver: StreamObserver<KurojiEventrouter.SubResponse>) {
        val msg = request.event.unpack(MessageOuterClass.Message::class.java)

        println("Message took: ${System.currentTimeMillis() - request.botId}ms")
        println("${msg.author.username}: ${msg.content}")
        responseObserver.onNext(KurojiEventrouter.SubResponse.getDefaultInstance())
        responseObserver.onCompleted()
    }
}
