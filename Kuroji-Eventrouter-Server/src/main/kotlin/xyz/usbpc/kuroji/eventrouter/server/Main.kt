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
            eventBuilder.type = Event.EventType.MESSAGE_CREATE
            //eventBuilder.event = com.google.protobuf.Any.pack(messageBuilder.build())
            messageMultiplier.onEvent(eventBuilder.build())
            //println("Send one Message!")
            delay(100)
        }
    }
    val aclient = AsyncCuratorFramework.wrap(client)
    aclient.createNode("/clients", options = setOf(CreateOption.createParentsIfNeeded, CreateOption.setDataIfExists))

    //Start thing that does the sending... so create channel, nameresolver etc.
    val sendSubsystem = EventSendSubsystem(client, messageMultiplier)

    LOGGER.debug("Registering shutdown hook")
    //Tell the JVM what we want to do on shutdown
    Runtime.getRuntime().addShutdownHook(
            thread (start = false, isDaemon = false, name = "Shutdown") {
                runBlocking {
                    sendSubsystem.shutdown()
                }
                rawClient.close()
                LOGGER.info("Bye <3")
            }
    )

    sendSubsystem.awaitTermination()

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

fun newEventFromRawEvent(rawEvent: KurojiWebsocket.RawEvent) : KurojiEventrouter.Event {
    val builder = KurojiEventrouter.Event.newBuilder()
    when (rawEvent.name) {
        "READY" -> builder.apply {
            type = Event.EventType.READY
            val msg = ReadyEventOuterClass.ReadyEvent.getDefaultInstance().buildFromJson(rawEvent.data)
            event = com.google.protobuf.Any.pack(msg)
            routing = KurojiEventrouter.RoutingInfo.newBuilder().setId(0).build()
        }
        "CHANNEL_CREATE" -> builder.apply {
            type = Event.EventType.READY
            val msg = ChannelOuterClass.Channel.getDefaultInstance().buildFromJson(rawEvent.data)
            event = com.google.protobuf.Any.pack(msg)
            routing = KurojiEventrouter.RoutingInfo.newBuilder().setId(msg.id).build()
        }
        "CHANNEL_UPDATE" -> builder.apply {
            type = Event.EventType.READY
            val msg = ChannelOuterClass.Channel.getDefaultInstance().buildFromJson(rawEvent.data)
            event = com.google.protobuf.Any.pack(msg)
            routing = KurojiEventrouter.RoutingInfo.newBuilder().setId(msg.id).build()
        }
        "CHANNEL_DELETE" -> builder.apply {
            type = Event.EventType.READY
            val msg = ChannelOuterClass.Channel.getDefaultInstance().buildFromJson(rawEvent.data)
            event = com.google.protobuf.Any.pack(msg)
            routing = KurojiEventrouter.RoutingInfo.newBuilder().setId(msg.id).build()
        }
        "CHANNEL_PINS_UPDATE" -> builder.apply {
            type = Event.EventType.READY
            val msg = ChannelPinsUpdateEventOuterClass.ChannelPinsUpdateEvent.getDefaultInstance().buildFromJson(rawEvent.data)
            event = com.google.protobuf.Any.pack(msg)
            routing = KurojiEventrouter.RoutingInfo.newBuilder().setId(msg.channelId).build()
        }
        "GUILD_CREATE" -> builder.apply {
            type = Event.EventType.READY
            val msg = GuildOuterClass.Guild.getDefaultInstance().buildFromJson(rawEvent.data)
            event = com.google.protobuf.Any.pack(msg)
            routing = KurojiEventrouter.RoutingInfo.newBuilder().setId(msg.id).build()
        }
        "GUILD_UPDATE" -> builder.apply {
            type = Event.EventType.READY
            val msg = GuildOuterClass.Guild.getDefaultInstance().buildFromJson(rawEvent.data)
            event = com.google.protobuf.Any.pack(msg)
            routing = KurojiEventrouter.RoutingInfo.newBuilder().setId(msg.id).build()
        }
        "GUILD_DELETE" -> builder.apply {
            type = Event.EventType.READY
            val msg = GuildOuterClass.Guild.getDefaultInstance().buildFromJson(rawEvent.data)
            event = com.google.protobuf.Any.pack(msg)
            routing = KurojiEventrouter.RoutingInfo.newBuilder().setId(msg.id).build()
        }
        "GUILD_BAN_ADD" -> builder.apply {
            type = Event.EventType.READY
            val msg = GuildBanEventOuterClass.GuildBanEvent.getDefaultInstance().buildFromJson(rawEvent.data)
            event = com.google.protobuf.Any.pack(msg)
            routing = KurojiEventrouter.RoutingInfo.newBuilder().setId(msg.guildId).build()
        }
        "GUILD_BAN_REMOVE" -> builder.apply {
            type = Event.EventType.READY
            val msg = GuildBanEventOuterClass.GuildBanEvent.getDefaultInstance().buildFromJson(rawEvent.data)
            event = com.google.protobuf.Any.pack(msg)
            routing = KurojiEventrouter.RoutingInfo.newBuilder().setId(msg.guildId).build()
        }
        "GUILD_EMOJIS_UPDATE" -> builder.apply {
            type = Event.EventType.READY
            val msg = GuildEmojisUpdateEventOuterClass.GuildEmojisUpdateEvent.getDefaultInstance().buildFromJson(rawEvent.data)
            event = com.google.protobuf.Any.pack(msg)
            routing = KurojiEventrouter.RoutingInfo.newBuilder().setId(msg.guildId).build()
        }
        "GUILD_INTEGRATIONS_UPDATE" -> builder.apply {
            type = Event.EventType.READY
            val msg = GuildIntegrationsUpdateEventOuterClass.GuildIntegrationsUpdateEvent.getDefaultInstance().buildFromJson(rawEvent.data)
            event = com.google.protobuf.Any.pack(msg)
            routing = KurojiEventrouter.RoutingInfo.newBuilder().setId(msg.guildId).build()
        }
        "GUILD_MEMBER_ADD" -> builder.apply {
            type = Event.EventType.READY
            val msg = GuildOuterClass.Guild.Member.getDefaultInstance().buildFromJson(rawEvent.data)
            event = com.google.protobuf.Any.pack(msg)
            routing = KurojiEventrouter.RoutingInfo.newBuilder().setId(msg.guildId).build()
        }
        "GUILD_MEMBER_REMOVE" -> builder.apply {
            type = Event.EventType.READY
            val msg = GuildMemberRemoveEventOuterClass.GuildMemberRemoveEvent.getDefaultInstance().buildFromJson(rawEvent.data)
            event = com.google.protobuf.Any.pack(msg)
            routing = KurojiEventrouter.RoutingInfo.newBuilder().setId(msg.guildId).build()
        }
        "GUILD_MEMBER_UPDATE" -> builder.apply {
            type = Event.EventType.READY
            val msg = GuildMemberUpdateEventOuterClass.GuildMemberUpdateEvent.getDefaultInstance().buildFromJson(rawEvent.data)
            event = com.google.protobuf.Any.pack(msg)
            routing = KurojiEventrouter.RoutingInfo.newBuilder().setId(msg.guildId).build()
        }
        "GUILD_MEMBER_CHUNK" -> builder.apply {
            type = Event.EventType.READY
            val msg = GuildMembersChunkEventOuterClass.GuildMembersChunkEvent.getDefaultInstance().buildFromJson(rawEvent.data)
            event = com.google.protobuf.Any.pack(msg)
            routing = KurojiEventrouter.RoutingInfo.newBuilder().setId(msg.guildId).build()
        }
        "GUILD_ROLE_CREATE" -> builder.apply {
            type = Event.EventType.READY
            val msg = GuildRoleEventOuterClass.GuildRoleEvent.getDefaultInstance().buildFromJson(rawEvent.data)
            event = com.google.protobuf.Any.pack(msg)
            routing = KurojiEventrouter.RoutingInfo.newBuilder().setId(msg.guildId).build()
        }
        "GUILD_ROLE_UPDATE" -> builder.apply {
            type = Event.EventType.READY
            val msg = GuildRoleEventOuterClass.GuildRoleEvent.getDefaultInstance().buildFromJson(rawEvent.data)
            event = com.google.protobuf.Any.pack(msg)
            routing = KurojiEventrouter.RoutingInfo.newBuilder().setId(msg.guildId).build()
        }
        "GUILD_ROLE_DELETE" -> builder.apply {
            type = Event.EventType.READY
            val msg = GuildRoleDeleteEventOuterClass.GuildRoleDeleteEvent.getDefaultInstance().buildFromJson(rawEvent.data)
            event = com.google.protobuf.Any.pack(msg)
            routing = KurojiEventrouter.RoutingInfo.newBuilder().setId(msg.guildId).build()
        }
        "MESSAGE_CREATE" -> builder.apply {
            type = Event.EventType.READY
            val msg = MessageOuterClass.Message.getDefaultInstance().buildFromJson(rawEvent.data)
            event = com.google.protobuf.Any.pack(msg)
            routing = KurojiEventrouter.RoutingInfo.newBuilder().setId(msg.channelId).build()
        }
        "MESSAGE_UPDATE" -> builder.apply {
            type = Event.EventType.READY
            val msg = MessageOuterClass.Message.getDefaultInstance().buildFromJson(rawEvent.data)
            event = com.google.protobuf.Any.pack(msg)
            routing = KurojiEventrouter.RoutingInfo.newBuilder().setId(msg.channelId).build()
        }
        "MESSAGE_DELETE" -> builder.apply {
            type = Event.EventType.READY
            val msg = MessageDeleteEventOuterClass.MessageDeleteEvent.getDefaultInstance().buildFromJson(rawEvent.data)
            event = com.google.protobuf.Any.pack(msg)
            routing = KurojiEventrouter.RoutingInfo.newBuilder().setId(msg.channelId).build()
        }
        "MESSAGE_DELETE_BULK" -> builder.apply {
            type = Event.EventType.READY
            val msg = MessageDeleteBulkEventOuterClass.MessageDeleteBulkEvent.getDefaultInstance().buildFromJson(rawEvent.data)
            event = com.google.protobuf.Any.pack(msg)
            routing = KurojiEventrouter.RoutingInfo.newBuilder().setId(msg.channelId).build()
        }
        "MESSAGE_REACTION_ADD" -> builder.apply {
            type = Event.EventType.READY
            val msg = MessageReactionEventOuterClass.MessageReactionEvent.getDefaultInstance().buildFromJson(rawEvent.data)
            event = com.google.protobuf.Any.pack(msg)
            routing = KurojiEventrouter.RoutingInfo.newBuilder().setId(msg.channelId).build()
        }
        "MESSAGE_REACTION_REMOVE" -> builder.apply {
            type = Event.EventType.READY
            val msg = MessageReactionEventOuterClass.MessageReactionEvent.getDefaultInstance().buildFromJson(rawEvent.data)
            event = com.google.protobuf.Any.pack(msg)
            routing = KurojiEventrouter.RoutingInfo.newBuilder().setId(msg.channelId).build()
        }
        "MESSAGE_REACTION_REMOVE_ALL" -> builder.apply {
            type = Event.EventType.READY
            val msg = MessageReactionEventOuterClass.MessageReactionEvent.getDefaultInstance().buildFromJson(rawEvent.data)
            event = com.google.protobuf.Any.pack(msg)
            routing = KurojiEventrouter.RoutingInfo.newBuilder().setId(msg.channelId).build()
        }
        "PRESENCE_UPDATE" -> builder.apply {
            type = Event.EventType.READY
            val msg = PresenceUpdateOuterClass.PresenceUpdate.getDefaultInstance().buildFromJson(rawEvent.data)
            event = com.google.protobuf.Any.pack(msg)
            routing = KurojiEventrouter.RoutingInfo.newBuilder().setId(msg.guildId).build()
        }
        "TYPING_START" -> builder.apply {
            type = Event.EventType.READY
            val msg = TypingStartEventOuterClass.TypingStartEvent.getDefaultInstance().buildFromJson(rawEvent.data)
            event = com.google.protobuf.Any.pack(msg)
            routing = KurojiEventrouter.RoutingInfo.newBuilder().setId(msg.channelId).build()
        }
        "USER_UPDATE" -> builder.apply {
            type = Event.EventType.READY
            val msg = UserOuterClass.User.getDefaultInstance().buildFromJson(rawEvent.data)
            event = com.google.protobuf.Any.pack(msg)
            routing = KurojiEventrouter.RoutingInfo.newBuilder().setId(msg.id).build()
        }
        "VOICE_STATE_UPDATE" -> builder.apply {
            type = Event.EventType.READY
            val msg = VoiceStateOuterClass.VoiceState.getDefaultInstance().buildFromJson(rawEvent.data)
            event = com.google.protobuf.Any.pack(msg)
            routing = KurojiEventrouter.RoutingInfo.newBuilder().setId(msg.channelId).build()
        }
        "VOICE_SERVER_UPDATE" -> builder.apply {
            type = Event.EventType.READY
            val msg = VoiceServerUpdateEventOuterClass.VoiceServerUpdateEvent.getDefaultInstance().buildFromJson(rawEvent.data)
            event = com.google.protobuf.Any.pack(msg)
            routing = KurojiEventrouter.RoutingInfo.newBuilder().setId(msg.guildId).build()
        }
        "WEBHOOKS_UPDATE" -> builder.apply {
            type = Event.EventType.READY
            val msg = WebhooksUpdateEventOuterClass.WebhooksUpdateEvent.getDefaultInstance().buildFromJson(rawEvent.data)
            event = com.google.protobuf.Any.pack(msg)
            routing = KurojiEventrouter.RoutingInfo.newBuilder().setId(msg.channelId).build()
        }
        else -> throw NotImplementedError("The topic ${rawEvent.name} is not handeled yet!")
    }
    builder.apply {
        botId = rawEvent.botId
        shardId = rawEvent.shardId
        traceId = rawEvent.traceId
    }
    return builder.build()
}

fun <T : Message> T.buildFromJson(json: String): T {
    val builder = this.newBuilderForType()
    JsonFormat.parser().ignoringUnknownFields().merge(json, builder)
    @Suppress("UNCHECKED_CAST")
    return builder.build() as T
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