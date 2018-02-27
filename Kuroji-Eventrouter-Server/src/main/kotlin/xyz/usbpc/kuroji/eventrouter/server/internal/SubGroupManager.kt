package xyz.usbpc.kuroji.eventrouter.server.internal

import io.grpc.ConnectivityState
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.Metadata
import io.grpc.stub.MetadataUtils
import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.Channel
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
import kotlin.coroutines.experimental.CoroutineContext
import kotlin.coroutines.experimental.suspendCoroutine

//TODO maybe monitor if there are no clients and stop things from spamming the logs?
class SubGroupManager(val name: String, client: CuratorFramework, val messageMultiplier: MessageMultiplier, val capacity: Int = 50) {
    companion object {
        private val LOGGER = this.getLogger()
    }
    private val managedChannel: ManagedChannel = createChannelForSubGroup(client, name)
    //Creating a stub here, cause they are thread save and we don't wanna create objects if not needed
    private val stub = MessageRouterSubscriberGrpc.newFutureStub(managedChannel)
    //The root is now our SubGroup we are responsible for
    private val aclient = AsyncCuratorFramework.wrap(client.usingNamespace("eventrouter/clients/$name"))
    private val topics : MutableMap<Event.EventType, TopicData> =
            EnumMap(Event.EventType::class.java)
    private var controlJob: Job? = null

    private val startMutex = Mutex()
    suspend fun start(context: CoroutineContext) = startMutex.withLock {
        LOGGER.info("Starting manager for {}", name)
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
                LOGGER.trace("Starting new loop of control loop for SubGroup {}", name)
                val stage = aclient.watched().data.forPath("/")
                //Get all the data!
                LOGGER.trace("Getting data from zookeeper for SubGroup {}", name)
                val data = KurojiEventrouter.SubscriberGroup.parseFrom(stage.await())
                LOGGER.trace("Creating topic list for the received data for SubGroup {}", name)
                val newTopics = data.topicsList - topics.keys
                LOGGER.trace("New topics for SubGroup {} are {}", name, newTopics)
                val oldTopics = topics.keys - data.topicsList
                LOGGER.trace("Old topics for SubGroup {} are {}", name, oldTopics)

                //Get rid of topics the SubGroup dose no longer subscribe to.
                oldTopics.forEach { topic ->
                    messageMultiplier.unregisterChannel(topic, name)
                    val (channel, theadPool, jobs) = topics[topic]!!
                    //Close the channel causing the send workers to terminate
                    channel.close()
                    //Wait for all send workers to terminate
                    jobs.forEach { it.join() }
                    //Close the Threadpool releasing the hold Threads (system resources)
                    theadPool.close()
                    //Remove the topic from the map.
                    topics.remove(topic)
                }
                //Create workers for topics the SubGroup just subscribed to.
                newTopics.forEach { topic ->
                    //Create the NamedChannel for this topic and this ConsumerGroup
                    val topicChannel = NamedChannel<KurojiEventrouter.Event>(name, capacity * 100)
                    //Create a new threadpool context to make things fast... maybe.
                    val workerContext = newFixedThreadPoolContext(4, "$name-${topic.name}")
                    //Create 50 send workers
                    //TODO: make these autoscale?
                    val jobs = List(50) {
                        newSendWorker(topicChannel, stub, workerContext)
                    }
                    //Strategic delay to let the workerContext do it's thing so the coroutines are ready when messages start to flow in
                    //delay(500)
                    //Register the channel we created
                    messageMultiplier.registerChannel(topic, topicChannel)
                    //Save all the things we created to the Map
                    topics[topic] = TopicData(topicChannel, workerContext, jobs)
                }
                //Wait for something to change from what we've gotten before
                if (Watcher.Event.EventType.NodeDeleted == stage.event().await().type)
                    break
            }
        }
    }
    private data class TopicData(val namedChannel: NamedChannel<KurojiEventrouter.Event>,
                                 val threadPoolDispatcher: ThreadPoolDispatcher,
                                 val jobs: List<Job>)
    private val shutdownMutex = Mutex()

    suspend fun shutdown() = shutdownMutex.withLock<Unit> {
        if (controlJob == null)
            throw IllegalStateException("Trying to shutdown before starting dosen't work")
        //End the control job so it dosen't spin up new things
        controlJob!!.cancel()
        controlJob!!
        controlJob = null
        //Shut down all the things we created
        topics.forEach {(key, data) ->
            messageMultiplier.unregisterChannel(key, name)
            data.namedChannel.close()
            data.jobs.forEach { it.join() }
            data.threadPoolDispatcher.close()
        }
        //Just in case this will be started again... it shouldn't and I control the whole app.. but why not
        topics.clear()
    }
    private fun newSendWorker(channel: Channel<KurojiEventrouter.Event>,
                      stub: MessageRouterSubscriberGrpc.MessageRouterSubscriberFutureStub,
                      context: CoroutineContext = DefaultDispatcher) =
            launch(context) {
                for (item in channel) {
                    try {
                        val key = Metadata.Key.of("routing-bin", Metadata.BINARY_BYTE_MARSHALLER)
                        val metadata = Metadata()
                        metadata.put(key, item.routing.toByteArray())
                        MetadataUtils.attachHeaders(stub, metadata).onEvent(item).await()
                    } catch (ex: Exception) {
                        LOGGER.error("Something went wrong sending a request!", ex)
                    }
                }
            }
}



internal fun createChannelForSubGroup(client: CuratorFramework, groupName: String) : ManagedChannel {
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