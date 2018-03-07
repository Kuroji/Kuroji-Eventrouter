package xyz.usbpc.kuroji.eventrouter.server.internal

import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.Message
import com.google.protobuf.util.JsonFormat
import xyz.astolfo.websocket.rpc.KurojiWebsocket
import xyz.usbpc.kuroji.eventrouter.api.KurojiEventrouter
import xyz.usbpc.kuroji.eventrouter.server.InternalEvent
import xyz.usbpc.kuroji.eventrouter.server.SubsystemManager
import xyz.usbpc.kuroji.eventrouter.server.getLogger
import xyz.usbpc.kuroji.proto.discord.events.*
import xyz.usbpc.kuroji.proto.discord.objects.*

interface MessageDecoder : SubsystemManager {
    /**
     * Parses the raw events, also decides what to use as the routing info.
     * @param rawEvent the event as received from that Websocket that should be parsed
     * @return The parsed event in an internal format or null if the event could not be parsed
     */
    fun parseRawEvent(rawEvent: KurojiWebsocket.RawEvent) : InternalEvent?
}

class MessageDecoderImpl : MessageDecoder {
    //Json parser, setup to ignore Unknown Fields cause discord likes to do small changes that would otherise result in exceptions
    private val parser = JsonFormat.parser().ignoringUnknownFields()

    companion object {
        val log = this.getLogger()
    }

    init {
        log.info("Starting up event decoding.")
    }

    override suspend fun stop() {
        log.info("Shutting down event decoding.")
        //NOP
    }

    override suspend fun awaitTermination() {
        //NOP
    }
    /**
     * Generic function that does the actual parsing from Json data
     * @receiver instance of protobuf message to with to parse the JSON data
     * @param json the json to parse
     * @return the parsed message
     */
    private fun <T : Message> T.buildFromJson(json: String): T {
        /* Creates a new builder for the message type we will parse the json as.
         * This seems to be the best way to get a new builder for any type without reflection and should be fast.
         */
        val builder = this.newBuilderForType()
        //Do the actual parsing work
        parser.merge(json, builder)
        @Suppress("UNCHECKED_CAST")
        return builder.build() as T
    }

    override fun parseRawEvent(rawEvent: KurojiWebsocket.RawEvent): InternalEvent? {
        val builder = KurojiEventrouter.Event.newBuilder()
        lateinit var msg: Message
        try {
            when (rawEvent.name) {
                "READY" -> builder.apply {
                    type = Event.EventType.READY
                    msg = ReadyEventOuterClass.ReadyEvent.getDefaultInstance().buildFromJson(rawEvent.data)
                    event = com.google.protobuf.Any.pack(msg)
                    routing = KurojiEventrouter.RoutingInfo.newBuilder().setId(0).build()
                }
                "CHANNEL_CREATE" -> builder.apply {
                    type = Event.EventType.CHANNEL_CREATE
                    msg = ChannelOuterClass.Channel.getDefaultInstance().buildFromJson(rawEvent.data)
                    event = com.google.protobuf.Any.pack(msg)
                    routing = KurojiEventrouter.RoutingInfo.newBuilder().setId((msg as ChannelOuterClass.Channel).id).build()
                }
                "CHANNEL_UPDATE" -> builder.apply {
                    type = Event.EventType.CHANNEL_UPDATE
                    msg = ChannelOuterClass.Channel.getDefaultInstance().buildFromJson(rawEvent.data)
                    event = com.google.protobuf.Any.pack(msg)
                    routing = KurojiEventrouter.RoutingInfo.newBuilder().setId((msg as ChannelOuterClass.Channel).id).build()
                }
                "CHANNEL_DELETE" -> builder.apply {
                    type = Event.EventType.CHANNEL_DELETE
                    msg = ChannelOuterClass.Channel.getDefaultInstance().buildFromJson(rawEvent.data)
                    event = com.google.protobuf.Any.pack(msg)
                    routing = KurojiEventrouter.RoutingInfo.newBuilder().setId((msg as ChannelOuterClass.Channel).id).build()
                }
                "CHANNEL_PINS_UPDATE" -> builder.apply {
                    type = Event.EventType.CHANNEL_PINS_UPDATE
                    msg = ChannelPinsUpdateEventOuterClass.ChannelPinsUpdateEvent.getDefaultInstance().buildFromJson(rawEvent.data)
                    event = com.google.protobuf.Any.pack(msg)
                    routing = KurojiEventrouter.RoutingInfo.newBuilder().setId((msg as ChannelPinsUpdateEventOuterClass.ChannelPinsUpdateEvent).channelId).build()
                }
                "GUILD_CREATE" -> builder.apply {
                    type = Event.EventType.GUILD_CREATE
                    msg = GuildOuterClass.Guild.getDefaultInstance().buildFromJson(rawEvent.data)
                    event = com.google.protobuf.Any.pack(msg)
                    routing = KurojiEventrouter.RoutingInfo.newBuilder().setId((msg as GuildOuterClass.Guild).id).build()
                }
                "GUILD_UPDATE" -> builder.apply {
                    type = Event.EventType.GUILD_UPDATE
                    msg = GuildOuterClass.Guild.getDefaultInstance().buildFromJson(rawEvent.data)
                    event = com.google.protobuf.Any.pack(msg)
                    routing = KurojiEventrouter.RoutingInfo.newBuilder().setId((msg as GuildOuterClass.Guild).id).build()
                }
                "GUILD_DELETE" -> builder.apply {
                    type = Event.EventType.GUILD_DELETE
                    msg = GuildOuterClass.Guild.getDefaultInstance().buildFromJson(rawEvent.data)
                    event = com.google.protobuf.Any.pack(msg)
                    routing = KurojiEventrouter.RoutingInfo.newBuilder().setId((msg as GuildOuterClass.Guild).id).build()
                }
                "GUILD_BAN_ADD" -> builder.apply {
                    type = Event.EventType.GUILD_BAN_ADD
                    msg = GuildBanEventOuterClass.GuildBanEvent.getDefaultInstance().buildFromJson(rawEvent.data)
                    event = com.google.protobuf.Any.pack(msg)
                    routing = KurojiEventrouter.RoutingInfo.newBuilder().setId((msg as GuildBanEventOuterClass.GuildBanEvent).guildId).build()
                }
                "GUILD_BAN_REMOVE" -> builder.apply {
                    type = Event.EventType.GUILD_BAN_REMOVE
                    msg = GuildBanEventOuterClass.GuildBanEvent.getDefaultInstance().buildFromJson(rawEvent.data)
                    event = com.google.protobuf.Any.pack(msg)
                    routing = KurojiEventrouter.RoutingInfo.newBuilder().setId((msg as GuildBanEventOuterClass.GuildBanEvent).guildId).build()
                }
                "GUILD_EMOJIS_UPDATE" -> builder.apply {
                    type = Event.EventType.GUILD_EMOJIS_UPDATE
                    msg = GuildEmojisUpdateEventOuterClass.GuildEmojisUpdateEvent.getDefaultInstance().buildFromJson(rawEvent.data)
                    event = com.google.protobuf.Any.pack(msg)
                    routing = KurojiEventrouter.RoutingInfo.newBuilder().setId((msg as GuildEmojisUpdateEventOuterClass.GuildEmojisUpdateEvent).guildId).build()
                }
                "GUILD_INTEGRATIONS_UPDATE" -> builder.apply {
                    type = Event.EventType.GUILD_INTEGRATIONS_UPDATE
                    msg = GuildIntegrationsUpdateEventOuterClass.GuildIntegrationsUpdateEvent.getDefaultInstance().buildFromJson(rawEvent.data)
                    event = com.google.protobuf.Any.pack(msg)
                    routing = KurojiEventrouter.RoutingInfo.newBuilder().setId((msg as GuildIntegrationsUpdateEventOuterClass.GuildIntegrationsUpdateEvent).guildId).build()
                }
                "GUILD_MEMBER_ADD" -> builder.apply {
                    type = Event.EventType.GUILD_MEMBER_ADD
                    msg = GuildOuterClass.Guild.Member.getDefaultInstance().buildFromJson(rawEvent.data)
                    event = com.google.protobuf.Any.pack(msg)
                    routing = KurojiEventrouter.RoutingInfo.newBuilder().setId((msg as GuildOuterClass.Guild.Member).guildId!!.value).build()
                }
                "GUILD_MEMBER_REMOVE" -> builder.apply {
                    type = Event.EventType.GUILD_MEMBER_REMOVE
                    msg = GuildMemberRemoveEventOuterClass.GuildMemberRemoveEvent.getDefaultInstance().buildFromJson(rawEvent.data)
                    event = com.google.protobuf.Any.pack(msg)
                    routing = KurojiEventrouter.RoutingInfo.newBuilder().setId((msg as GuildMemberRemoveEventOuterClass.GuildMemberRemoveEvent).guildId).build()
                }
                "GUILD_MEMBER_UPDATE" -> builder.apply {
                    type = Event.EventType.GUILD_MEMBER_UPDATE
                    msg = GuildMemberUpdateEventOuterClass.GuildMemberUpdateEvent.getDefaultInstance().buildFromJson(rawEvent.data)
                    event = com.google.protobuf.Any.pack(msg)
                    routing = KurojiEventrouter.RoutingInfo.newBuilder().setId((msg as GuildMemberUpdateEventOuterClass.GuildMemberUpdateEvent).guildId).build()
                }
                "GUILD_MEMBER_CHUNK" -> builder.apply {
                    type = Event.EventType.GUILD_MEMBERS_CHUNCK
                    msg = GuildMembersChunkEventOuterClass.GuildMembersChunkEvent.getDefaultInstance().buildFromJson(rawEvent.data)
                    event = com.google.protobuf.Any.pack(msg)
                    routing = KurojiEventrouter.RoutingInfo.newBuilder().setId((msg as GuildMembersChunkEventOuterClass.GuildMembersChunkEvent).guildId).build()
                }
                "GUILD_ROLE_CREATE" -> builder.apply {
                    type = Event.EventType.GUILD_ROLE_CREATE
                    msg = GuildRoleEventOuterClass.GuildRoleEvent.getDefaultInstance().buildFromJson(rawEvent.data)
                    event = com.google.protobuf.Any.pack(msg)
                    routing = KurojiEventrouter.RoutingInfo.newBuilder().setId((msg as GuildRoleEventOuterClass.GuildRoleEvent).guildId).build()
                }
                "GUILD_ROLE_UPDATE" -> builder.apply {
                    type = Event.EventType.GUILD_ROLE_UPDATE
                    msg = GuildRoleEventOuterClass.GuildRoleEvent.getDefaultInstance().buildFromJson(rawEvent.data)
                    event = com.google.protobuf.Any.pack(msg)
                    routing = KurojiEventrouter.RoutingInfo.newBuilder().setId((msg as GuildRoleEventOuterClass.GuildRoleEvent).guildId).build()
                }
                "GUILD_ROLE_DELETE" -> builder.apply {
                    type = Event.EventType.GUILD_ROLE_DELETE
                    msg = GuildRoleDeleteEventOuterClass.GuildRoleDeleteEvent.getDefaultInstance().buildFromJson(rawEvent.data)
                    event = com.google.protobuf.Any.pack(msg)
                    routing = KurojiEventrouter.RoutingInfo.newBuilder().setId((msg as GuildRoleDeleteEventOuterClass.GuildRoleDeleteEvent).guildId).build()
                }
                "MESSAGE_CREATE" -> builder.apply {
                    type = Event.EventType.MESSAGE_CREATE
                    msg = MessageOuterClass.Message.getDefaultInstance().buildFromJson(rawEvent.data)
                    event = com.google.protobuf.Any.pack(msg)
                    routing = KurojiEventrouter.RoutingInfo.newBuilder().setId((msg as MessageOuterClass.Message).channelId).build()
                }
                "MESSAGE_UPDATE" -> builder.apply {
                    type = Event.EventType.MESSAGE_UPDATE
                    msg = MessageOuterClass.Message.getDefaultInstance().buildFromJson(rawEvent.data)
                    event = com.google.protobuf.Any.pack(msg)
                    routing = KurojiEventrouter.RoutingInfo.newBuilder().setId((msg as MessageOuterClass.Message).channelId).build()
                }
                "MESSAGE_DELETE" -> builder.apply {
                    type = Event.EventType.MESSAGE_DELETE
                    msg = MessageDeleteEventOuterClass.MessageDeleteEvent.getDefaultInstance().buildFromJson(rawEvent.data)
                    event = com.google.protobuf.Any.pack(msg)
                    routing = KurojiEventrouter.RoutingInfo.newBuilder().setId((msg as MessageDeleteEventOuterClass.MessageDeleteEvent).channelId).build()
                }
                "MESSAGE_DELETE_BULK" -> builder.apply {
                    type = Event.EventType.MESSAGE_DELETE_BULK
                    msg = MessageDeleteBulkEventOuterClass.MessageDeleteBulkEvent.getDefaultInstance().buildFromJson(rawEvent.data)
                    event = com.google.protobuf.Any.pack(msg)
                    routing = KurojiEventrouter.RoutingInfo.newBuilder().setId((msg as MessageDeleteBulkEventOuterClass.MessageDeleteBulkEvent).channelId).build()
                }
                "MESSAGE_REACTION_ADD" -> builder.apply {
                    type = Event.EventType.MESSAGE_REACTION_ADD
                    msg = MessageReactionEventOuterClass.MessageReactionEvent.getDefaultInstance().buildFromJson(rawEvent.data)
                    event = com.google.protobuf.Any.pack(msg)
                    routing = KurojiEventrouter.RoutingInfo.newBuilder().setId((msg as MessageReactionEventOuterClass.MessageReactionEvent).channelId).build()
                }
                "MESSAGE_REACTION_REMOVE" -> builder.apply {
                    type = Event.EventType.MESSAGE_REACTION_REMOVE
                    msg = MessageReactionEventOuterClass.MessageReactionEvent.getDefaultInstance().buildFromJson(rawEvent.data)
                    event = com.google.protobuf.Any.pack(msg)
                    routing = KurojiEventrouter.RoutingInfo.newBuilder().setId((msg as MessageReactionEventOuterClass.MessageReactionEvent).channelId).build()
                }
                "MESSAGE_REACTION_REMOVE_ALL" -> builder.apply {
                    type = Event.EventType.MESSAGE_REACTION_REMOVE_ALL
                    msg = MessageReactionEventOuterClass.MessageReactionEvent.getDefaultInstance().buildFromJson(rawEvent.data)
                    event = com.google.protobuf.Any.pack(msg)
                    routing = KurojiEventrouter.RoutingInfo.newBuilder().setId((msg as MessageReactionEventOuterClass.MessageReactionEvent).channelId).build()
                }
                "PRESENCE_UPDATE" -> builder.apply {
                    type = Event.EventType.PRESENCE_UPDATE
                    msg = PresenceUpdateOuterClass.PresenceUpdate.getDefaultInstance().buildFromJson(rawEvent.data)
                    event = com.google.protobuf.Any.pack(msg)
                    routing = KurojiEventrouter.RoutingInfo.newBuilder().setId((msg as PresenceUpdateOuterClass.PresenceUpdate).guildId).build()
                }
                "TYPING_START" -> builder.apply {
                    type = Event.EventType.TYPING_START
                    msg = TypingStartEventOuterClass.TypingStartEvent.getDefaultInstance().buildFromJson(rawEvent.data)
                    event = com.google.protobuf.Any.pack(msg)
                    routing = KurojiEventrouter.RoutingInfo.newBuilder().setId((msg as TypingStartEventOuterClass.TypingStartEvent).channelId).build()
                }
                "USER_UPDATE" -> builder.apply {
                    type = Event.EventType.USER_UPDATE
                    msg = UserOuterClass.User.getDefaultInstance().buildFromJson(rawEvent.data)
                    event = com.google.protobuf.Any.pack(msg)
                    routing = KurojiEventrouter.RoutingInfo.newBuilder().setId((msg as UserOuterClass.User).id).build()
                }
                "VOICE_STATE_UPDATE" -> builder.apply {
                    type = Event.EventType.VOICE_STATE_UPDATE
                    msg = VoiceStateOuterClass.VoiceState.getDefaultInstance().buildFromJson(rawEvent.data)
                    event = com.google.protobuf.Any.pack(msg)
                    //TODO this will make problems if we recive voice state for DMs... but this is for bots only and they currently can't have that
                    routing = KurojiEventrouter.RoutingInfo.newBuilder().setId((msg as VoiceStateOuterClass.VoiceState).channelId!!.value).build()
                }
                "VOICE_SERVER_UPDATE" -> builder.apply {
                    type = Event.EventType.VOICE_SERVER_UPDATE
                    msg = VoiceServerUpdateEventOuterClass.VoiceServerUpdateEvent.getDefaultInstance().buildFromJson(rawEvent.data)
                    event = com.google.protobuf.Any.pack(msg)
                    routing = KurojiEventrouter.RoutingInfo.newBuilder().setId((msg as VoiceServerUpdateEventOuterClass.VoiceServerUpdateEvent).guildId).build()
                }
                "WEBHOOKS_UPDATE" -> builder.apply {
                    type = Event.EventType.WEBHOOKS_UPDATE
                    msg = WebhooksUpdateEventOuterClass.WebhooksUpdateEvent.getDefaultInstance().buildFromJson(rawEvent.data)
                    event = com.google.protobuf.Any.pack(msg)
                    routing = KurojiEventrouter.RoutingInfo.newBuilder().setId((msg as WebhooksUpdateEventOuterClass.WebhooksUpdateEvent).channelId).build()
                }
                else -> {
                    log.error("Unknown event type {} with data {}!", rawEvent.name, rawEvent.data)
                    return null
                }
            }
        } catch (ex: InvalidProtocolBufferException) {
            log.error("Could not parse {} with data {}!", rawEvent.name, rawEvent.data)
            return null
        }
        builder.apply {
            botId = rawEvent.botId
            shardId = rawEvent.shardId
            traceId = rawEvent.traceId
        }
        return InternalEvent(msg, builder.build())
    }

}