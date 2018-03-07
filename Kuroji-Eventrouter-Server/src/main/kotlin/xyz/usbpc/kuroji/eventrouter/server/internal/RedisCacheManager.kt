package xyz.usbpc.kuroji.eventrouter.server.internal

import io.lettuce.core.RedisClient
import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.codec.RedisCodec
import io.lettuce.core.codec.StringCodec
import xyz.usbpc.kuroji.eventrouter.server.CacheManager
import xyz.usbpc.kuroji.eventrouter.server.InternalEvent
import xyz.usbpc.kuroji.proto.discord.events.*
import xyz.usbpc.kuroji.proto.discord.objects.*
import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

class RedisCacheManager : CacheManager {
    val redisClient = RedisClient.create("redis://localhost:6397/0")
    val redisConnection = redisClient.connect(StringByteCodec())
    val redis = redisConnection.async()

    override suspend fun stop() {
        redisConnection.close()
    }

    override suspend fun awaitTermination() {

    }

    //TODO remove dublicate code, move caching of events into specific functions per event.
    override fun onEvent(event: InternalEvent) {
        when (event.event.type) {
            Event.EventType.READY -> {
                val readyEvent = (event.msg as ReadyEventOuterClass.ReadyEvent)
                readyEvent.privateChannelsList.forEach { channel ->
                    redis.setex("${event.event.botId}:channel:${channel.id}", TimeUnit.HOURS.toSeconds(12), channel.toByteArray())
                }

            }
            Event.EventType.CHANNEL_CREATE -> {
                val channel = (event.msg as ChannelOuterClass.Channel)
                redis.setex("${event.event.botId}:channel:${channel.id}", TimeUnit.DAYS.toSeconds(1), channel.toByteArray())
            }
            Event.EventType.CHANNEL_UPDATE -> {
                val channel = (event.msg as ChannelOuterClass.Channel)
                redis.setex("${event.event.botId}:channel:${channel.id}", TimeUnit.DAYS.toSeconds(1), channel.toByteArray())
            }
            Event.EventType.CHANNEL_DELETE -> {
                val channel = (event.msg as ChannelOuterClass.Channel)
                redis.del("${event.event.botId}:channel:${channel.id}")
            }
            Event.EventType.CHANNEL_PINS_UPDATE -> {}
            Event.EventType.GUILD_CREATE -> {
                val guild = (event.msg as GuildOuterClass.Guild)
                redis.setex("${event.event.botId}:guild:${guild.id}", TimeUnit.DAYS.toSeconds(1), guild.toByteArray())
            }
            Event.EventType.GUILD_UPDATE -> {
                val guild = (event.msg as GuildOuterClass.Guild)
                redis.setex("${event.event.botId}:guild:${guild.id}", TimeUnit.DAYS.toSeconds(1), guild.toByteArray())
            }
            Event.EventType.GUILD_DELETE -> {
                val guild = (event.msg as GuildOuterClass.Guild)
                redis.del("${event.event.botId}:guild:${guild.id}")
            }
            Event.EventType.GUILD_BAN_ADD -> {}
            Event.EventType.GUILD_BAN_REMOVE -> {}
            Event.EventType.GUILD_EMOJIS_UPDATE -> {}
            Event.EventType.GUILD_INTEGRATIONS_UPDATE -> {}
            Event.EventType.GUILD_MEMBER_ADD -> {
                val member = event.msg as GuildOuterClass.Guild.Member
                redis.setex("${event.event.botId}:user:${member.user.id}", TimeUnit.HOURS.toSeconds(2), member.user.toByteArray())
                //TODO figure out how to cache what roles a user has for a guild
            }
            Event.EventType.GUILD_MEMBER_REMOVE -> {}
            Event.EventType.GUILD_MEMBER_UPDATE -> {}
            Event.EventType.GUILD_MEMBERS_CHUNCK -> {}
            Event.EventType.GUILD_ROLE_CREATE -> {
                val roleEvent = event.msg as GuildRoleEventOuterClass.GuildRoleEvent
                //TODO probably put this in a set... would make more sense
                redis.setex("${event.event.botId}:role:${roleEvent.guildId}:${roleEvent.role.id}", TimeUnit.HOURS.toSeconds(2), roleEvent.role.toByteArray())
            }
            Event.EventType.GUILD_ROLE_UPDATE ->  {
                val roleEvent = event.msg as GuildRoleEventOuterClass.GuildRoleEvent
                redis.setex("${event.event.botId}:role:${roleEvent.guildId}:${roleEvent.role.id}", TimeUnit.HOURS.toSeconds(2), roleEvent.role.toByteArray())
            }
            Event.EventType.GUILD_ROLE_DELETE -> {
                val roleDeleteEvent = event.msg as GuildRoleDeleteEventOuterClass.GuildRoleDeleteEvent
                redis.del("${event.event.botId}:role:${roleDeleteEvent.guildId}:${roleDeleteEvent.roleId}")
            }
            Event.EventType.MESSAGE_CREATE -> {
                val message = event.msg as MessageOuterClass.Message
                redis.setex("${event.event.botId}:message:${message.id}", TimeUnit.HOURS.toSeconds(1), message.toByteArray())
                redis.expire("${event.event.botId}:user:${message.author.id}", TimeUnit.HOURS.toSeconds(2))
            }
            Event.EventType.MESSAGE_UPDATE -> {
                val message = event.msg as MessageOuterClass.Message
                //TODO this might actually delete some data of a message
                redis.setex("${event.event.botId}:message:${message.id}", TimeUnit.HOURS.toSeconds(1), message.toByteArray())
                redis.expire("${event.event.botId}:user:${message.author.id}", TimeUnit.HOURS.toSeconds(2))
            }
            Event.EventType.MESSAGE_DELETE -> {
                val messageDelete = event.msg as MessageDeleteEventOuterClass.MessageDeleteEvent
                redis.del("${event.event.botId}:message${messageDelete.id}")
            }
            Event.EventType.MESSAGE_DELETE_BULK -> {}
            Event.EventType.MESSAGE_REACTION_ADD -> {}
            Event.EventType.MESSAGE_REACTION_REMOVE -> {}
            Event.EventType.MESSAGE_REACTION_REMOVE_ALL -> {}
            Event.EventType.PRESENCE_UPDATE -> {
                val presenceUpdate = event.msg as PresenceUpdateOuterClass.PresenceUpdate
                redis.setex("${event.event.botId}:presence:${presenceUpdate.guildId}:${presenceUpdate.user.id}", TimeUnit.MINUTES.toSeconds(30), presenceUpdate.toByteArray())
                redis.expire("${event.event.botId}:user:${presenceUpdate.user.id}", TimeUnit.HOURS.toSeconds(2))
            }
            Event.EventType.TYPING_START -> {
                val typingEvent = event.msg as TypingStartEventOuterClass.TypingStartEvent
                redis.expire("${event.event.botId}:user:${typingEvent.userId}", TimeUnit.HOURS.toSeconds(2))
            }
            Event.EventType.USER_UPDATE -> {
                val user = event.msg as UserOuterClass.User
                redis.setex("${event.event.botId}:user:${user.id}", TimeUnit.HOURS.toSeconds(2), user.toByteArray())
            }
            Event.EventType.VOICE_STATE_UPDATE -> {}
            Event.EventType.VOICE_SERVER_UPDATE -> {}
            Event.EventType.WEBHOOKS_UPDATE -> {}
            Event.EventType.UNRECOGNIZED -> TODO()
            Event.EventType.UNKNOWN -> TODO()
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