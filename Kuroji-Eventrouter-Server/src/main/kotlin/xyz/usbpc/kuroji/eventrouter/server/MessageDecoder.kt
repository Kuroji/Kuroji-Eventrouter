package xyz.usbpc.kuroji.eventrouter.server

import com.google.protobuf.Message
import com.google.protobuf.util.JsonFormat
import xyz.astolfo.websocket.rpc.KurojiWebsocket
import xyz.usbpc.kuroji.eventrouter.api.KurojiEventrouter
import xyz.usbpc.kuroji.proto.discord.events.*
import xyz.usbpc.kuroji.proto.discord.objects.*

interface MessageDecoder : SubsystemManager {
    /**
     * Decodes a raw event from the Websocket into an Internal Format so data is accessible without parsing then
     * proto Event
     */
    fun parseRawEvent(rawEvent: KurojiWebsocket.RawEvent) : InternalEvent
}

class MessageDecoderImpl : MessageDecoder {
    private val parser = JsonFormat.parser().ignoringUnknownFields()

    override suspend fun stop() {
        //NOP
    }

    override suspend fun awaitTermination() {
        //NOP
    }
    /**
     * Generic function that does the actual parsing from Json data
     */
    private fun <T : Message> T.buildFromJson(json: String): T {
        val builder = this.newBuilderForType()
        parser.merge(json, builder)
        @Suppress("UNCHECKED_CAST")
        return builder.build() as T
    }


    override fun parseRawEvent(rawEvent: KurojiWebsocket.RawEvent): InternalEvent {
        val builder = KurojiEventrouter.Event.newBuilder()
        lateinit var msg: Message
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
                routing = KurojiEventrouter.RoutingInfo.newBuilder().setId((msg as GuildOuterClass.Guild.Member).guildId).build()
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
                routing = KurojiEventrouter.RoutingInfo.newBuilder().setId((msg as VoiceStateOuterClass.VoiceState).channelId).build()
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
            else -> throw NotImplementedError("The topic ${rawEvent.name} is not handeled yet!")
        }
        builder.apply {
            botId = rawEvent.botId
            shardId = rawEvent.shardId
            traceId = rawEvent.traceId
        }
        return InternalEvent(msg, builder.build())
    }

}