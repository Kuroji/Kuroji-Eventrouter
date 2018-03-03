package xyz.usbpc.kuroji.eventrouter.server.internal

import io.grpc.Server
import io.grpc.ServerBuilder
import io.grpc.stub.StreamObserver
import kotlinx.coroutines.experimental.asCoroutineDispatcher
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.runBlocking
import org.apache.curator.framework.CuratorFramework
import xyz.astolfo.websocket.rpc.KurojiWebsocket
import xyz.astolfo.websocket.rpc.WebsocketSubscriberGrpc
import xyz.usbpc.kuroji.eventrouter.server.Eventrouter
import xyz.usbpc.kuroji.eventrouter.server.SubsystemManager
import xyz.usbpc.kuroji.eventrouter.server.getLogger
import xyz.usbpc.kuroji.websocket.client.WebsocketSubscriber
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

interface EventReceiver : SubsystemManager

/**
 * This subsystem connects with a Websocket and receives messages
 * @param client an Instance of CuratorFramework used for registering in zookeeper
 * @param port the port where to start listening for connections from the Websocket, default: 7120
 * @param eventrouter an Instance of Eventrouter where to send the recived events
 */
class EventReciverImpl(client: CuratorFramework, port: Int = 7120, val eventrouter: Eventrouter) : EventReceiver, WebsocketSubscriberGrpc.WebsocketSubscriberImplBase() {
    private val zoo = WebsocketSubscriber(port, client)
    private val server : Server
    //TODO name threads
    //This is the executor that is used for most of the work in the eventrouter except for sending.
    private val threadPoolExecutor = ThreadPoolExecutor(1, 200, 30, TimeUnit.SECONDS, LinkedBlockingQueue())
    private val context = threadPoolExecutor.asCoroutineDispatcher()

    companion object {
        val log = this.getLogger()
    }

    init {
        log.info("Starting up the Receiving subsystem")
        server = ServerBuilder.forPort(port)
                .addService(this)
                .build()
                .start()
        runBlocking {
            zoo.register()
        }
    }

    /**
     * RPC call, gets called by the Websocket. This is where the events enter the eventrouter
     */
    override fun onEvent(request: KurojiWebsocket.RawEvent, responseObserver: StreamObserver<KurojiWebsocket.SubResponse>) {
        log.debug("Got an event of type {}", request.name)
        launch(context) {
            eventrouter.onRawMessage(request)
            responseObserver.onNext(KurojiWebsocket.SubResponse.getDefaultInstance())
            responseObserver.onCompleted()
        }
    }

    override suspend fun stop() {
        log.info("Shutting doth the Receiving subsystem")
        zoo.unregister()
        server.shutdown()
        server.awaitTermination()
        threadPoolExecutor.shutdown()
    }

    override suspend fun awaitTermination() {
        server.awaitTermination()
    }

}