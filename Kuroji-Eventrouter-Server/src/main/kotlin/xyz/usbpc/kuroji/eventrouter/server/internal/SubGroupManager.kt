package xyz.usbpc.kuroji.eventrouter.server.internal

import io.grpc.ConnectivityState
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.Metadata
import io.grpc.stub.MetadataUtils
import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.future.await
import kotlinx.coroutines.experimental.guava.await
import kotlinx.coroutines.experimental.sync.Mutex
import kotlinx.coroutines.experimental.sync.withLock
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.x.async.AsyncCuratorFramework
import org.apache.zookeeper.Watcher
import xyz.usbpc.kuroji.eventrouter.api.KurojiEventrouter
import xyz.usbpc.kuroji.eventrouter.api.MessageRouterSubscriberGrpc
import xyz.usbpc.kuroji.eventrouter.server.getLogger
import xyz.usbpc.kuroji.eventrouter.server.grpc.EventRouterLoadBalancerFactory
import xyz.usbpc.kuroji.eventrouter.server.grpc.ZookeeperNameResolverProvider
import xyz.usbpc.kuroji.proto.discord.events.Event
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import kotlin.coroutines.experimental.CoroutineContext
import kotlin.coroutines.experimental.suspendCoroutine

class SubGroupManager(val name: String, client: CuratorFramework, val messageMultiplier: MessageMultiplier, val capacity: Int = 50) {
    companion object {
        private val log = this.getLogger()
    }
    private val managedChannel: ManagedChannel = createChannelForSubGroup(client, name)
    //Creating a stub here, cause they are thread save and we don't wanna create objects if not needed
    private val stub = MessageRouterSubscriberGrpc.newFutureStub(managedChannel)
    //The root is now our SubGroup we are responsible for
    private val aclient = AsyncCuratorFramework.wrap(client.usingNamespace("eventrouter/clients/$name"))
    private val topics : MutableSet<Event.EventType> = EnumSet.noneOf(Event.EventType::class.java)
    private var controlJob: Job? = null
    //TODO name the threads
    private val threadPoolExecutor = ThreadPoolExecutor(1, 50, 30, TimeUnit.SECONDS, LinkedBlockingQueue())
    private val context = threadPoolExecutor.asCoroutineDispatcher()


    private val startMutex = Mutex()
    suspend fun start(context: CoroutineContext) = startMutex.withLock {
        if (controlJob != null)
            throw IllegalStateException("This manager is already started!")
        //Look what we are subscribed and create a NamedChannel for each event
        //  create NamedChannels for each Event type
        //  create worker threads for each Event
        //  register named channels for each event type
        //start listening to changes in the subscribed topics and adjust accordingly
        var state : ConnectivityState = managedChannel.getState(true)
        do {
            suspendCoroutine<Unit> { cont ->
                managedChannel.notifyWhenStateChanged(state) {
                    cont.resume(Unit)
                }
            }
            state = managedChannel.getState(true)
        } while (state != ConnectivityState.READY)

        controlJob = launch(context) {
            while (isActive) {
                val stage = aclient.watched().data.forPath("/")
                val data = KurojiEventrouter.SubscriberGroup.parseFrom(stage.await())
                val newTopics = data.topicsList - topics
                val oldTopics = topics - data.topicsList

                oldTopics.forEach { topic ->
                    messageMultiplier.unregisterFunction(topic, name)
                    log.info("SubGroup {} is no longer listening for {}", name, topic)
                }
                topics.removeAll(oldTopics)
                newTopics.forEach { topic ->
                    messageMultiplier.registerFunction(topic, SendWorker(name, stub, context))
                    log.info("SubGroup {} is now listening for {}", name, topic)
                }
                topics.addAll(newTopics)

                //Wait for something to change from what we've gotten before
                if (Watcher.Event.EventType.NodeDeleted == stage.event().await().type)
                    break
            }
        }
    }

    private val shutdownMutex = Mutex()

    suspend fun shutdown() = shutdownMutex.withLock<Unit> {
        if (controlJob == null)
            throw IllegalStateException("Trying to shutdown before starting dosen't work")
        //End the control job so it dosen't spin up new things
        controlJob!!.cancel()
        controlJob!!
        controlJob = null
        topics.forEach {
            messageMultiplier.unregisterFunction(it, name)
        }
        threadPoolExecutor.shutdown()
        while (!threadPoolExecutor.isTerminated)
            yield()
        //Shut down all the things we created
    }

    private class SendWorker(override val name: String, val stub: MessageRouterSubscriberGrpc.MessageRouterSubscriberFutureStub, val context: CoroutineContext) : EventSender {
        companion object {
            private val key = Metadata.Key.of("routing-bin", Metadata.BINARY_BYTE_MARSHALLER)
        }
        override fun sendEvent(event: KurojiEventrouter.Event) {
            launch(context) {
                val metadata = Metadata()
                metadata.put(key, event.routing.toByteArray())
                MetadataUtils.attachHeaders(stub, metadata).onEvent(event).await()
            }
        }

    }

    private fun createChannelForSubGroup(client: CuratorFramework, groupName: String) : ManagedChannel {
        val nameResolver = ZookeeperNameResolverProvider(
                AsyncCuratorFramework.wrap(client.usingNamespace("eventrouter/clients"))
        )
        return ManagedChannelBuilder
                .forTarget("/$groupName")
                .nameResolverFactory(nameResolver)
                .loadBalancerFactory(EventRouterLoadBalancerFactory(client))
                //TODO: figure out how to TLS https://github.com/grpc/grpc-java/blob/master/SECURITY.md
                .usePlaintext(true)
                .build()
    }
}