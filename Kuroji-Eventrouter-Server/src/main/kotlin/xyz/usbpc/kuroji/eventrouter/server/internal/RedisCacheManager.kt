package xyz.usbpc.kuroji.eventrouter.server.internal

import com.google.common.primitives.Longs
import com.google.protobuf.Int64Value
import com.google.protobuf.StringValue
import com.google.protobuf.Timestamp
import com.google.protobuf.util.JsonFormat
import io.lettuce.core.RedisClient
import io.lettuce.core.ScanArgs
import io.lettuce.core.ValueScanCursor
import io.lettuce.core.codec.ByteArrayCodec
import io.lettuce.core.codec.CompressionCodec
import io.lettuce.core.codec.RedisCodec
import io.lettuce.core.codec.StringCodec
import kotlinx.coroutines.experimental.future.await
import xyz.usbpc.kuroji.caching.CacheFormats
import xyz.usbpc.kuroji.eventrouter.server.CacheManager
import xyz.usbpc.kuroji.eventrouter.server.InternalEvent
import xyz.usbpc.kuroji.eventrouter.server.getLogger
import xyz.usbpc.kuroji.proto.discord.events.*
import xyz.usbpc.kuroji.proto.discord.objects.*
import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

fun Long.toByteArray() =
        Longs.toByteArray(this)
fun ByteArray.toLong() =
        Longs.fromByteArray(this)

class RedisCacheManager : CacheManager {
    val redisClient = RedisClient.create("redis://localhost:6379/0")
    val redisConnection = redisClient.connect(StringByteCodec())
    val redis = redisConnection.async()

    companion object {
        val log = this.getLogger()
    }

    override suspend fun stop() {
        //TODO figure out how to wait for nothing to be in the onEvent code.
        redisConnection.close()
    }

    override suspend fun awaitTermination() {

    }

    //TODO remove dublicate code, move caching of events into specific functions per event.
    override suspend fun onEvent(event: InternalEvent) {
        log.debug("Got event of type {}", event.event.type)
        when (event.event.type) {
            //Event.EventType.READY -> {}
            Event.EventType.CHANNEL_CREATE -> {
                val channel = (event.msg as ChannelOuterClass.Channel)
                redis.set("${event.event.botId}:channel:${channel.id}", channel.toByteArray())
                if (channel.hasGuildId()) {
                    redis.sadd("${event.event.botId}:guild:${channel.guildId!!.value}:channels", channel.id.toByteArray())
                }
            }
            Event.EventType.CHANNEL_UPDATE -> {
                val channel = (event.msg as ChannelOuterClass.Channel)
                redis.set("${event.event.botId}:channel:${channel.id}", channel.toByteArray())
            }
            Event.EventType.CHANNEL_DELETE -> {
                val channel = (event.msg as ChannelOuterClass.Channel)
                if (channel.hasGuildId()) {
                    redis.srem("${event.event.botId}:guild:${channel.guildId!!.value}:channels", channel.id.toByteArray())
                }
                redis.del("${event.event.botId}:channel:${channel.id}")

            }
            //Event.EventType.CHANNEL_PINS_UPDATE -> {}
            Event.EventType.GUILD_CREATE -> {
                val guild = (event.msg as GuildOuterClass.Guild)
                val builder = GuildOuterClass.Guild.newBuilder(guild)

                builder.clearVoiceStates()
                builder.clearRoles()
                builder.clearMembers()
                builder.clearChannels()
                builder.clearPresences()

                //Save the actual guild object
                redis.set("${event.event.botId}:guild:${guild.id}", builder.build().toByteArray())

                //Save what channels are part of this guild
                guild.channelsList.forEach { channel ->
                    redis.set("${event.event.botId}:channel:${channel.id}", channel.toByteArray())
                }

                val guildIds = Array(guild.channelsList.size) {guild.getChannels(it).toByteArray()}
                redis.sadd("${event.event.botId}:guild:${guild.id}:channels", *guildIds)

                //Save what users are part of this guild
                guild.membersList.forEach { member ->
                    val memberBuilder = GuildOuterClass.Guild.Member.newBuilder(member)

                    memberBuilder.clearUser()
                    memberBuilder.clearRoles()

                    //Let's add that user to the known users!
                    redis.set("${event.event.botId}:user:${member.user.id}", member.user.toByteArray())

                    //And populate the member field for this user in this guild
                    redis.hset(
                            "${event.event.botId}:guild:${guild.id}:user:${member.user.id}",
                            "member",
                            memberBuilder.build().toByteArray())

                    //And save what roles the user is part of
                    val roleIds = CacheFormats.Fixed64List.newBuilder().addAllValues(member.rolesList).build()
                    redis.hset(
                            "${event.event.botId}:guild:${guild.id}:user:${member.user.id}",
                            "roles",
                            roleIds.toByteArray()
                    )

                }

                //Now let's deal with the voice states
                guild.voiceStatesList.forEach { partVoiceState ->

                    val voiceState = VoiceStateOuterClass.VoiceState
                            .newBuilder(partVoiceState)
                            .setGuildId(Int64Value.of(guild.id))
                            .build()

                    redis.hset(
                            "${event.event.botId}:guild:${guild.id}:user:${partVoiceState.userId}",
                            "voiceState",
                            voiceState.toByteArray())
                }

                //And with presences
                guild.presencesList.forEach { presence ->
                    val presenceBuilder = PresenceUpdateOuterClass.PresenceUpdate.newBuilder(presence)
                    presenceBuilder.clearUser()
                    presenceBuilder.clearRoles()

                    redis.hset(
                            "${event.event.botId}:guild:${guild.id}:user:${presence.user.id}",
                            "presence",
                            presenceBuilder.build().toByteArray())
                }

                //Save the roles of this guild
                guild.rolesList.forEach { role ->
                    redis.hset(
                            "${event.event.botId}:guild:${guild.id}:roles",
                            role.id.toString(),
                            role.toByteArray()
                    )
                }
            }

            Event.EventType.GUILD_UPDATE -> {
                val guildBuilder = GuildOuterClass.Guild.newBuilder((event.msg as GuildOuterClass.Guild))
                //This is a tiny bit more elaborate since I wanna preserve the joined_at value
                val oldGuild = GuildOuterClass.Guild.parseFrom(redis.get("${event.event.botId}:guild:${guildBuilder.id}").await())
                guildBuilder.joinedAt = oldGuild.joinedAt
                val guild = guildBuilder.build()
                redis.set("${event.event.botId}:guild:${guild.id}", guild.toByteArray())
            }
            Event.EventType.GUILD_DELETE -> {
                val guild = (event.msg as GuildOuterClass.Guild)
                //TODO set the guild to unavailable somewhere... maybe?
                //Don't wanna delete everything if this is just an outage!
                if (guild.unavailable?.value != true) {
                    //Delete the guild itself... but we also need to delete all things associated with this...
                    redis.del("${event.event.botId}:guild:${guild.id}")
                    //We'll get channel delete events for all the channels... So we just need to delete the set with all the channel ids
                    redis.del("${event.event.botId}:guild:${guild.id}:channels")
                    //And also let's get rid of the roles
                    redis.del("${event.event.botId}:guild:${guild.id}:roles")
                    //And the most complicated bit, the guild users
                    val args = ScanArgs.Builder.matches("${event.event.botId}:guild:${guild.id}:user:*")
                    var cursor = redis.scan(args).await()
                    val guildUsers = mutableSetOf<String>()
                    guildUsers.addAll(cursor.keys)
                    while (!cursor.isFinished) {
                        cursor = redis.scan(cursor).await()
                        guildUsers.addAll(cursor.keys)
                    }
                    //This deletes all the keys associated with users in that guild.
                    //Done in chuncks of 20 keys at a time to not block redis if we leave a large guild
                    guildUsers.chunked(20) {it.toTypedArray()}.forEach { userIds ->
                        redis.del(*userIds)
                    }
                }
            }
            //Event.EventType.GUILD_BAN_ADD -> {}
            //Event.EventType.GUILD_BAN_REMOVE -> {}
            //Event.EventType.GUILD_EMOJIS_UPDATE -> {}
            //Event.EventType.GUILD_INTEGRATIONS_UPDATE -> {}
            Event.EventType.GUILD_MEMBER_ADD -> {
                val member = event.msg as GuildOuterClass.Guild.Member
                //Let's cache this user!
                redis.set("${event.event.botId}:user:${member.user.id}", member.user.toByteArray())

                val memberBuilder = GuildOuterClass.Guild.Member.newBuilder(member)
                        .clearGuildId()
                        .clearUser()
                        .clearRoles()
                        .build()
                //Firstly the member object itself
                redis.hset(
                        "${event.event.botId}:guild:${member.guildId!!.value}:user:${member.user.id}",
                        "member",
                        memberBuilder.toByteArray()
                )
                //And the roles the user is part of
                val roleIds = CacheFormats.Fixed64List.newBuilder().addAllValues(member.rolesList).build()
                redis.hset(
                        "${event.event.botId}:guild:${member.guildId!!.value}:user:${member.user.id}",
                        "roles",
                        roleIds.toByteArray()
                )
            }

            Event.EventType.GUILD_MEMBER_REMOVE -> {
                //TODO also check if that user is in any other guild maybe... or maybe just let the TTL take care of this idk
                val member = event.msg as GuildMemberRemoveEventOuterClass.GuildMemberRemoveEvent
                redis.del("${event.event.botId}:guild:${member.guildId}:user:${member.user.id}")
            }

            Event.EventType.GUILD_MEMBER_UPDATE -> {
                val member = event.msg as GuildMemberUpdateEventOuterClass.GuildMemberUpdateEvent
                //Let's cache this user!
                redis.set("${event.event.botId}:user:${member.user.id}", member.user.toByteArray())

                //TODO cache this properyl

            }
            Event.EventType.GUILD_MEMBERS_CHUNCK -> {
                val membersChunck = event.msg as GuildMembersChunkEventOuterClass.GuildMembersChunkEvent
                membersChunck.membersList.forEach { member ->
                    //TODO this is awful I just copy pasted from GUILD_MEMBER_ADD... mostly
                    val member = event.msg as GuildOuterClass.Guild.Member
                    //Let's cache this user!
                    redis.set("${event.event.botId}:user:${member.user.id}", member.user.toByteArray())

                    val memberBuilder = GuildOuterClass.Guild.Member.newBuilder(member)
                            .clearGuildId()
                            .clearUser()
                            .clearRoles()
                            .build()
                    //Firstly the member object itself
                    redis.hset(
                            "${event.event.botId}:guild:${membersChunck.guildId}:user:${member.user.id}",
                            "member",
                            memberBuilder.toByteArray()
                    )
                    //And the roles the user is part of
                    val roleIds = CacheFormats.Fixed64List.newBuilder().addAllValues(member.rolesList).build()
                    redis.hset(
                            "${event.event.botId}:guild:${membersChunck.guildId}:user:${member.user.id}",
                            "roles",
                            roleIds.toByteArray()
                    )
                }
            }
            Event.EventType.GUILD_ROLE_CREATE -> {
                val roleEvent = event.msg as GuildRoleEventOuterClass.GuildRoleEvent
                redis.hset("${event.event.botId}:guild:${roleEvent.guildId}:roles", "${roleEvent.role.id}", roleEvent.role.toByteArray())
            }
            Event.EventType.GUILD_ROLE_UPDATE ->  {
                val roleEvent = event.msg as GuildRoleEventOuterClass.GuildRoleEvent
                redis.hset("${event.event.botId}:guild:${roleEvent.guildId}:roles", "${roleEvent.role.id}", roleEvent.role.toByteArray())
            }
            Event.EventType.GUILD_ROLE_DELETE -> {
                val roleDeleteEvent = event.msg as GuildRoleDeleteEventOuterClass.GuildRoleDeleteEvent
                redis.hdel("${event.event.botId}:guild:${roleDeleteEvent.guildId}:roles", "${roleDeleteEvent.roleId}")
            }
            Event.EventType.MESSAGE_CREATE -> {
                val message = event.msg as MessageOuterClass.Message
                redis.setex("${event.event.botId}:message:${message.id}", TimeUnit.DAYS.toSeconds(1), message.toByteArray())
            }
            Event.EventType.MESSAGE_UPDATE -> {
                val message = event.msg as MessageOuterClass.Message
                //TODO this is a bit complicated, I need to find out something better, but for now let's just invalidate the cache for that message!
                redis.del("${event.event.botId}:message:${message.id}")
            }
            Event.EventType.MESSAGE_DELETE -> {
                val messageDelete = event.msg as MessageDeleteEventOuterClass.MessageDeleteEvent
                redis.del("${event.event.botId}:message:${messageDelete.id}")
            }
            Event.EventType.MESSAGE_DELETE_BULK -> {
                val bulkDeleteEvent = event.msg as MessageDeleteBulkEventOuterClass.MessageDeleteBulkEvent
                bulkDeleteEvent.idsList.forEach { messageId ->
                    redis.del("${event.event.botId}:message:$messageId")
                }
            }
            //Event.EventType.MESSAGE_REACTION_ADD -> {}
            //Event.EventType.MESSAGE_REACTION_REMOVE -> {}
            //Event.EventType.MESSAGE_REACTION_REMOVE_ALL -> {}
            Event.EventType.PRESENCE_UPDATE -> {
                //TODO this may be the first time we see a specific user on big guilds... so I should probably kinda cache the user object?
                val presenceUpdate = event.msg as PresenceUpdateOuterClass.PresenceUpdate
                val presence = PresenceUpdateOuterClass.PresenceUpdate.newBuilder(presenceUpdate)
                        .clearUser()
                        .clearRoles()
                        .build()
                redis.hset(
                        "${event.event.botId}:guild:${presence.guildId}:user:${presenceUpdate.user.id}",
                        "presence",
                        presence.toByteArray()
                )

                val roleIds = CacheFormats.Fixed64List.newBuilder()
                        .addAllValues(presenceUpdate.rolesList)
                        .build()

                redis.hset(
                        "${event.event.botId}:guild:${presence.guildId}:user:${presenceUpdate.user.id}",
                        "roles",
                        roleIds.toByteArray()
                )

                if (presence.user.username != "") {
                    redis.set("${event.event.botId}:user:${presenceUpdate.user.id}", presenceUpdate.user.toByteArray())
                }
            }

            //Event.EventType.TYPING_START -> { }

            Event.EventType.USER_UPDATE -> {
                val user = event.msg as UserOuterClass.User
                redis.set("${event.event.botId}:user:${user.id}",  user.toByteArray())
            }
            Event.EventType.VOICE_STATE_UPDATE -> {
                val voiceState = event.msg as VoiceStateOuterClass.VoiceState
                if (voiceState.hasGuildId()) {
                    if (voiceState.hasChannelId()) {
                        //A user joined or changed the channel
                        redis.hset(
                                "${event.event.botId}:guild:${voiceState.guildId!!.value}:user:${voiceState.userId}",
                                "voiceState",
                                voiceState.toByteArray()
                                )
                    } else {
                        //A user left the channel
                        redis.hdel(
                                "${event.event.botId}:guild:${voiceState.guildId!!.value}:user:${voiceState.userId}",
                                "voiceState"
                        )
                    }
                }

            }
            //Event.EventType.VOICE_SERVER_UPDATE -> {}
            //Event.EventType.WEBHOOKS_UPDATE -> {}
            Event.EventType.UNRECOGNIZED -> log.warn("Unrecognized event!")
            Event.EventType.UNKNOWN -> log.warn("Unknown event!")
            else -> {/*Nothing to cache*/}
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