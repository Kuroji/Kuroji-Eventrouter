import com.google.protobuf.Int64Value
import com.google.protobuf.util.JsonFormat
import io.grpc.ServerBuilder
import io.grpc.stub.StreamObserver
import io.lettuce.core.RedisClient
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.codec.RedisCodec
import io.lettuce.core.codec.StringCodec
import kotlinx.coroutines.experimental.future.await
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.newSingleThreadContext
import kotlinx.coroutines.experimental.runBlocking
import okhttp3.MediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import xyz.usbpc.kuroji.eventrouter.api.KurojiEventrouter
import xyz.usbpc.kuroji.eventrouter.api.MessageRouterSubscriberGrpc
import xyz.usbpc.kuroji.eventrouter.client.EventRouterSubscriber
import xyz.usbpc.kuroji.proto.discord.events.Event
import xyz.usbpc.kuroji.proto.discord.objects.MessageOuterClass
import xyz.usbpc.kuroji.proto.discord.objects.UserOuterClass
import java.nio.ByteBuffer
import java.util.*
import kotlin.concurrent.thread

fun mai(args: Array<String>) {
    val set = EnumSet.of(Event.EventType.MESSAGE_CREATE, Event.EventType.PRESENCE_UPDATE, Event.EventType.TYPING_START)
    set.forEach {
        println(it)
    }
}

fun main(args: Array<String>) = runBlocking {
    val zookeeper = EventRouterSubscriber("totallyNew", args[0].toInt(), setOf(Event.EventType.MESSAGE_CREATE))
    val redisClient = RedisClient.create("redis://localhost:6379/0")
    val redisConnection = redisClient.connect(StringByteCodec())
    val redis = redisConnection.async()
    val okHttpClient = OkHttpClient.Builder().build()
    val server = ServerBuilder.forPort(args[0].toInt())
            .addService(MessageRouterSubscriber(redis, okHttpClient, args[1]))
            .build()
            .start()
    zookeeper.register()
    //zookeeper.claim(KurojiEventrouter.RoutingInfo.newBuilder().setId(10).build())
    Runtime.getRuntime().addShutdownHook(
            thread (start = false, isDaemon = false) {
                println("Shutting down...")
                runBlocking(newSingleThreadContext("Shutdown Coroutine")) {
                    zookeeper.shutdown()
                }
                server.shutdown()
                println("Bye <3")
            }
    )
    server.awaitTermination()
}

val JSON = MediaType.parse("application/json; charset=utf-8")

class MessageRouterSubscriber(val redis: RedisAsyncCommands<String, ByteArray>, val okHttp: OkHttpClient, val TOKEN: String) : MessageRouterSubscriberGrpc.MessageRouterSubscriberImplBase() {
    val printer = JsonFormat.printer().omittingInsignificantWhitespace()
    val parse = JsonFormat.parser()
    val pings = mutableSetOf<Long>()
    override fun onEvent(request: KurojiEventrouter.Event, responseObserver: StreamObserver<KurojiEventrouter.SubResponse>) {
        launch {
            val message = MessageOuterClass.Message.parseFrom(request.event.value)
            println("[${message.channelId}] ${message.author.username}: ${message.content}")
            if (message.nonce?.value in pings) {
                val time = System.currentTimeMillis() - message.nonce!!.value
                pings.remove(message.nonce.value)

                val response = MessageOuterClass.Message.newBuilder()
                        .setContent("That took me ${time}ms")
                        .build()

                val okRequest = Request.Builder()
                        .header("Authorization", "Bot $TOKEN")
                        .url("https://discordapp.com/api/v6/channels/${message.channelId}/messages/${message.id}")
                        .patch(RequestBody.create(JSON, printer.print(response)))
                        .build()

                okHttp.newCall(okRequest).execute()
            } else if (message.content.startsWith("2?ping")) {
                println("got a message with 2?ping!")
                val time = System.currentTimeMillis()
                pings.add(time)
                val response = MessageOuterClass.Message.newBuilder()
                        .setContent("Pong!")
                        .setNonce(Int64Value.of(time))
                        .build()

                val okRequest = Request.Builder()
                        .header("Authorization", "Bot $TOKEN")
                        .url("https://discordapp.com/api/v6/channels/${message.channelId}/messages")
                        .post(RequestBody.create(JSON, printer.print(response)))
                        .build()

                okHttp.newCall(okRequest).execute()

            }
            responseObserver.onNext(KurojiEventrouter.SubResponse.getDefaultInstance())
            responseObserver.onCompleted()
        }
    }
}

class StringByteCodec : RedisCodec<String, ByteArray> {
    val stringDelegate = StringCodec()
    val byteDelegate = ByteArrayCodec()

    override fun encodeKey(key: String): ByteBuffer =
            stringDelegate.encodeKey(key)

    override fun decodeKey(bytes: ByteBuffer): String =
            stringDelegate.decodeKey(bytes)

    override fun encodeValue(value: ByteArray): ByteBuffer =
            byteDelegate.encodeValue(value)

    override fun decodeValue(bytes: ByteBuffer): ByteArray =
            byteDelegate.decodeValue(bytes)

}