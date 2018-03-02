package xyz.usbpc.kuroji.eventrouter.server.internal

import io.grpc.Server
import io.grpc.ServerBuilder
import io.grpc.stub.StreamObserver
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.runBlocking
import org.apache.curator.framework.CuratorFramework
import xyz.astolfo.websocket.rpc.KurojiWebsocket
import xyz.astolfo.websocket.rpc.WebsocketSubscriberGrpc
import xyz.usbpc.kuroji.eventrouter.server.Eventrouter
import xyz.usbpc.kuroji.eventrouter.server.SubsystemManager
import xyz.usbpc.kuroji.eventrouter.server.getLogger
import xyz.usbpc.kuroji.websocket.client.WebsocketSubscriber

interface EventReceiver : SubsystemManager

class EventReciverImpl(client: CuratorFramework, private val port: Int = 7120, val eventrouter: Eventrouter) : EventReceiver, WebsocketSubscriberGrpc.WebsocketSubscriberImplBase() {
    private val zoo = WebsocketSubscriber(port, client)
    private val server : Server

    companion object {
        val log = this.getLogger()
    }

    init {
        server = ServerBuilder.forPort(port)
                .addService(this)
                .build()
                .start()
        runBlocking {
            zoo.register()
        }
    }

    override fun onEvent(request: KurojiWebsocket.RawEvent, responseObserver: StreamObserver<KurojiWebsocket.SubResponse>) {
        //TODO put this in good context
        log.info("Got an event of type {}", request.name)
        launch {
            eventrouter.onRawMessage(request)
            responseObserver.onNext(KurojiWebsocket.SubResponse.getDefaultInstance())
            responseObserver.onCompleted()
        }
    }

    override suspend fun stop() {
        zoo.unregister()
        server.shutdown()
        server.awaitTermination()
    }

    override suspend fun awaitTermination() {
        server.awaitTermination()
    }

}