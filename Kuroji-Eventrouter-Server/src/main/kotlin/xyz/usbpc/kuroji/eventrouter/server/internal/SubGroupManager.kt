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
import xyz.usbpc.kuroji.eventrouter.server.grpc.EventRouterLoadBalancerFactory
import xyz.usbpc.kuroji.eventrouter.server.zoo.ZookeeperNameResolverProvider
import java.util.*
import kotlin.coroutines.experimental.CoroutineContext
import kotlin.coroutines.experimental.suspendCoroutine

//TODO maybe monitor if there are no clients and stop things from spamming the logs?
class SubGroupManager(val name: String, client: CuratorFramework, val messageMultiplier: MessageMultiplier, val capacity: Int = 50) {
    private val managedChannel: ManagedChannel = createChannelForSubGroup(client, name)
    //Creating a stub here, cause they are thread save and we don't wanna create objects if not needed
    private val stub = MessageRouterSubscriberGrpc.newFutureStub(managedChannel)
    //The root is now our SubGroup we are responsible for
    private val aclient = AsyncCuratorFramework.wrap(client.usingNamespace("eventrouter/clients/$name"))
    private val topics : MutableMap<KurojiEventrouter.EventType, TopicData> =
            EnumMap(KurojiEventrouter.EventType::class.java)
    private var controlJob: Job? = null

    private val startMutex = Mutex()
    suspend fun start(context: CoroutineContext) = startMutex.withLock {
        //TODO logging
        println("Starting manager for $name")
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
                println("Starting new loop of Control Loop for SubGroup $name")
                val stage = aclient.watched().data.forPath("/")
                //Get all the data!
                println("Getting data from zookeeper for SubGroup $name")
                val data = KurojiEventrouter.SubscriberGroup.parseFrom(stage.await())
                println("Creating topic lists for the retrived data for SubGroup $name")
                val newTopics = data.topicsList - topics.keys
                println("New topics for SubGroup $name are $newTopics")
                val oldTopics = topics.keys - data.topicsList
                println("Old topics for SubGroup $name are $oldTopics")

                println("Maybe getting rid of old channels and things")
                //Get rid of topics the SubGroup dose no longer subscribe to.
                oldTopics.forEach { topic ->
                    //TODO logging
                    println("Getting rid of $topic for SubGroup $name")
                    messageMultiplier.unregisterChannel(topic, name)
                    val (channel, theadPool, jobs) = topics[topic]!!
                    //Close the channel causing the send workers to terminate
                    println("Closing channel for $topic for SubGroup $name")
                    channel.close()
                    //Wait for all send workers to terminate
                    println("Waiting for all coroutines to stop for $topic for SubGroup $name")
                    jobs.forEach { it.join() }
                    //Close the Threadpool releasing the hold Threads (system resources)
                    println("Closing thread pool for $topic for SubGroup $name")
                    theadPool.close()
                    //Remove the topic from the map.
                    println("Removing $topic from Map for SubGroup $name")
                    topics.remove(topic)
                }
                println("Maybe creating new channels and things")
                //Create workers for topics the SubGroup just subscribed to.
                newTopics.forEach { topic ->
                    //Create the NamedChannel for this topic and this ConsumerGroup
                    println("Creating channel for $topic for SubGroup $name")
                    val topicChannel = NamedChannel<KurojiEventrouter.Event>(name, capacity * 100)
                    //Create a new threadpool context to make things fast... maybe.
                    println("Creating context for workers of $topic for SubGroup $name")
                    val workerContext = newFixedThreadPoolContext(4, "$name-${topic.name}")
                    //Create 50 send workers
                    //TODO: make these autoscale?
                    println("Creating workers for $topic for SubGroup $name")
                    val jobs = List(50) {
                        newSendWorker(topicChannel, stub, workerContext)
                    }
                    //Strategic delay to let the workerContext do it's thing so the coroutines are ready when messages start to flow in
                    //delay(500)
                    //Register the channel we created
                    println("Registering channel for $topic for SubGroup $name")
                    messageMultiplier.registerChannel(topic, topicChannel)
                    //Save all the things we created to the Map
                    println("Putting all information of $topic for SubGroup $name into the map")
                    topics[topic] = TopicData(topicChannel, workerContext, jobs)
                }
                //Wait for something to change from what we've gotten before
                println("Waiting for something to change in Zookeeper for $name")
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
        println("Starting shutdown of SubGroup $name")
        println("Waiting for controljob of SubGroup $name to cancel")
        controlJob!!.cancel()
        controlJob!!
        controlJob = null
        //Shut down all the things we created
        println("Releasing all resources of SubGroup $name")
        topics.forEach {(key, data) ->
            println("Unregitering channel $key for SubGroup $name")
            messageMultiplier.unregisterChannel(key, name)
            println("Closing channel $key for SubGroup $name")
            data.namedChannel.close()
            println("Waiting for jobs of channel $key for SubGroup $name to end")
            data.jobs.forEach { it.join() }
            println("Closing thread pool for $key for SubGroup $name")
            data.threadPoolDispatcher.close()
            println("Done releasing resourses of $key for SubGroup $name")
        }
        //Just in case this will be started again... it shouldn't and I control the whole app.. but why not
        topics.clear()
        println("Released all resources of SubGroup $name")
    }
}

fun newSendWorker(channel: Channel<KurojiEventrouter.Event>,
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
                    //TODO logging
                    println("Something went wrong sending a request!")
                    println(ex)
                    //ex.printStackTrace()
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
            .loadBalancerFactory(EventRouterLoadBalancerFactory)
            //TODO: figure out how to TLS https://github.com/grpc/grpc-java/blob/master/SECURITY.md
            .usePlaintext(true)
            .build()
}