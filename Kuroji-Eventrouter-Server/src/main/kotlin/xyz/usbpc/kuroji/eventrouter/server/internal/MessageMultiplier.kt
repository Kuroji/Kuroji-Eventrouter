package xyz.usbpc.kuroji.eventrouter.server.internal

import kotlinx.coroutines.experimental.sync.Mutex
import kotlinx.coroutines.experimental.sync.withLock
import xyz.usbpc.kuroji.eventrouter.api.KurojiEventrouter
import xyz.usbpc.kuroji.eventrouter.server.getLogger
import xyz.usbpc.kuroji.proto.discord.events.Event

class MessageMultiplier {
    companion object {
        val log = this.getLogger()
    }
    private val functions: MutableMap<Event.EventType, MutableList<EventSender>> = mutableMapOf()
    private val functionsMutex = Mutex()

    fun onEvent(event: KurojiEventrouter.Event) {
        functions[event.type]?.forEach { sender ->
            sender.sendEvent(event)
        }
    }

    suspend fun registerFunction(type: Event.EventType, eventSender: EventSender) {
        functionsMutex.withLock {
            functions.getOrPut(type) {
                mutableListOf()
            }.add(eventSender)
        }
    }

    suspend fun unregisterFunction(type: Event.EventType, name: String) {
        functionsMutex.withLock {
            val list = functions[type] ?: return
            list.remove(list.single{it.name == name})
            if (list.size == 1)
                functions.remove(type)
        }
    }
}

interface EventSender {
    val name: String
    fun sendEvent (event: KurojiEventrouter.Event)
}