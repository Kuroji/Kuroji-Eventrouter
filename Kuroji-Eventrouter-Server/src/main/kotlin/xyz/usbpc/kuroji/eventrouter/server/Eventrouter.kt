package xyz.usbpc.kuroji.eventrouter.server

import com.google.protobuf.Message
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import xyz.astolfo.websocket.rpc.KurojiWebsocket
import xyz.usbpc.kuroji.eventrouter.api.KurojiEventrouter
import xyz.usbpc.kuroji.eventrouter.server.internal.*

/**
 * The Eventrouter starts all subsystems. The subsystems are:
 * EventReceiving - connects to a WS and recives all events for futher processing
 * EventDecoding  - Takes the JSON and decodes the messages to protobuf
 * Caching        - Takes the events and puts things that will most likely be used into a cache (currently does nothing)
 * EventSending   - Sends events to subscribers.
 */
class Eventrouter : SubsystemManager {

    private val sender: EventSenderSubsystem
    private val cacher: CacheManager
    private val decoder: MessageDecoderImpl
    private val receiver: EventReceiver
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

    /**
     * This stops the eventrouter and all subsystems.
     * It stops them in an order and a way so that no received messages get lost.
     */
    override suspend fun stop() {
        receiver.stop()
        decoder.stop()
        cacher.stop()
        sender.stop()
    }

    fun onRawMessage(event: KurojiWebsocket.RawEvent) {
        val event = decoder.parseRawEvent(event) ?: return
        cacher.onEvent(event)
        sender.onEvent(event.event)
    }

    /**
     * Awaits for the termination of the eventrouter by waiting for all subsystems to terminate.
     */
    override suspend fun awaitTermination() {
        receiver.awaitTermination()
        decoder.awaitTermination()
        cacher.awaitTermination()
        sender.awaitTermination()
    }

}

/**
 * This holds both the decoded protobuf message and the Event for futher sending.
 * It's needed so we don't have to decode the message encoded in the event for caching.
 * @param msg holds the decoded protobuf event
 * @param event holds the encoded protobuf event fo sending
 */
class InternalEvent(val msg: Message, val event: KurojiEventrouter.Event)

/**
 * Starts up a new Curator Framework client
 * @param connectString a zookeeper connection string
 * @param baseSleepTime the base time for exponential backoff, default 1000
 * @param maxRetries max number of retrys for connecting, default 3
 */
fun newCuratorFrameworkClient(connectString: String, baseSleepTime: Int = 1000, maxRetries: Int = 3) : CuratorFramework {
    val retryPolicy = ExponentialBackoffRetry(1000, 3)
    return CuratorFrameworkFactory.newClient(connectString, retryPolicy)
}

/**
 * Common interface for all Subsystems to implement
 */
interface SubsystemManager {
    /**
     * Stops the subsystem.
     * @return when the subsystem is stopped
     */
    suspend fun stop()

    /**
     * Waits for this subsystem to stop
     * @return when the subsystem is stopped
     */
    suspend fun awaitTermination()
}

/**
 * Interface for the Cache manager.
 * The cache manager is responsible to put events wich data is likely needed by calls to the discord api into a cache
 * accessible by both this and the discord API wrapper.
 */
interface CacheManager : SubsystemManager {
    /**
     * @param event Event that might be cached.
     */
    fun onEvent(event: InternalEvent)
}

/**
 * Implementation of Cache Manager that does nothing.
 */
class NoOpCacheManager : CacheManager {
    override suspend fun stop() {}
    override suspend fun awaitTermination() {}
    override fun onEvent(event: InternalEvent) {}
}