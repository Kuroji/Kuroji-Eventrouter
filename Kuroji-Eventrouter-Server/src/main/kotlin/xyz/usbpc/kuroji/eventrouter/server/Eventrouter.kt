package xyz.usbpc.kuroji.eventrouter.server

import com.google.protobuf.Message
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import xyz.astolfo.websocket.rpc.KurojiWebsocket
import xyz.usbpc.kuroji.eventrouter.api.KurojiEventrouter
import xyz.usbpc.kuroji.eventrouter.server.internal.EventReceiver
import xyz.usbpc.kuroji.eventrouter.server.internal.EventReciverImpl
import xyz.usbpc.kuroji.eventrouter.server.internal.EventSendSubsystem
import xyz.usbpc.kuroji.eventrouter.server.internal.EventSender

/**
 * This is the glue that holds everything together
 */
class Eventrouter : SubsystemManager {

    private val sender: EventSender
    private val cacher: CacheManager
    private val decoder: MessageDecoderImpl
    lateinit var receiver: EventReceiver
    private var curatorClient: CuratorFramework = newCuratorFrameworkClient("localhost:2181").apply { start() }

    companion object {
        val log = this.getLogger()
    }

    init {
        //TODO remove hardcoded connect String
        sender = EventSendSubsystem(curatorClient)
        cacher = NoOpCacheManager()
        decoder = MessageDecoderImpl()
        receiver = EventReciverImpl(curatorClient, eventrouter = this)
    }

    override suspend fun stop() {
        receiver.stop()
        decoder.stop()
        cacher.stop()
        sender.stop()
    }

    fun onRawMessage(event: KurojiWebsocket.RawEvent) {
        val event = decoder.parseRawEvent(event)
        log.info("Decoded event of type {}", event.event.type)
        cacher.onEvent(event)
        sender.onEvent(event.event)
    }

    override suspend fun awaitTermination() {
        receiver.awaitTermination()
        decoder.awaitTermination()
        cacher.awaitTermination()
        sender.awaitTermination()
    }

}

class InternalEvent(val msg: Message, val event: KurojiEventrouter.Event)

fun newCuratorFrameworkClient(connectString: String, baseSleepTime: Int = 1000, maxRetries: Int = 3) : CuratorFramework {
    val retryPolicy = ExponentialBackoffRetry(1000, 3)
    return CuratorFrameworkFactory.newClient(connectString, retryPolicy)
}

interface SubsystemManager {
    suspend fun stop()
    suspend fun awaitTermination()
}

interface CacheManager : SubsystemManager {
    fun onEvent(event: InternalEvent)
}

class NoOpCacheManager : CacheManager {
    override suspend fun stop() {}
    override suspend fun awaitTermination() {}
    override fun onEvent(event: InternalEvent) {}
}