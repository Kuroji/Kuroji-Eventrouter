package xyz.usbpc.kuroji.eventrouter.server.internal

import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.ClosedSendChannelException
import kotlinx.coroutines.experimental.sync.Mutex
import kotlinx.coroutines.experimental.sync.withLock
import xyz.usbpc.kuroji.eventrouter.api.KurojiEventrouter

class MessageMultiplier {
    private val channels: MutableMap<KurojiEventrouter.EventType, MutableList<NamedChannel<KurojiEventrouter.Event>>> = mutableMapOf()
    fun onEvent(event: KurojiEventrouter.Event) {
        channels[event.type]?.forEach {
            try {
                if (!it.offer(event)) {
                    //TODO logging
                    println("Dropped ${event.type} for channel ${it.name}!")
                }
            } catch (ex: ClosedSendChannelException) {
                //TODO logging info
                println("Tried to send ${event.type} to channel ${it.name} but channel was closed.")
            }
        }
    }

    suspend fun registerChannel(type: KurojiEventrouter.EventType, namedChannel: NamedChannel<KurojiEventrouter.Event>) {
        mutex.withLock {
            channels.getOrPut(type) {
                mutableListOf()
            }.add(namedChannel)
        }
        namedChannel.receive()
    }

    private val mutex = Mutex()
    suspend fun unregisterChannel(type: KurojiEventrouter.EventType, name: String) {
        mutex.withLock<Unit> {
            val list = channels[type] ?: return
            if (list.size == 1)
                channels.remove(type)
            list.remove(list.single { it.name == name })
        }
    }
}

class NamedChannel<T>(val name: String, capacity: Int = 0, channel: Channel<T> = Channel(capacity)) : Channel<T> by channel