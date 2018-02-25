package xyz.usbpc.kuroji.eventrouter.server.grpc

import io.grpc.Attributes
import io.grpc.EquivalentAddressGroup
import io.grpc.NameResolver
import io.grpc.NameResolverProvider
import kotlinx.coroutines.experimental.Job
import kotlinx.coroutines.experimental.future.await
import kotlinx.coroutines.experimental.launch
import org.apache.curator.x.async.AsyncCuratorFramework
import org.apache.zookeeper.Watcher
import xyz.usbpc.kuroji.eventrouter.api.KurojiEventrouter
import java.net.InetSocketAddress
import java.net.URI

class ZookeeperNameResolverProvider(val aclient: AsyncCuratorFramework) : NameResolverProvider() {
    override fun newNameResolver(targetUri: URI, params: Attributes): NameResolver {
        return ZookeeperNameResolver(targetUri.toString(), aclient)
    }

    override fun getDefaultScheme(): String {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun priority(): Int {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun isAvailable(): Boolean {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

}

class ZookeeperNameResolver(val dir: String, val aclient: AsyncCuratorFramework) : NameResolver() {
    private var job: Job? = null
    override fun shutdown() {
        job?.cancel()
        job = null
    }
    companion object {
        val SERVER_NAME_ATTRIBUTE_KEY = Attributes.Key.of<String>("name")
    }
    override fun start(listener: Listener) {
        job = launch {
            while (isActive) {
                val thing = aclient.watched().children.forPath(dir)
                val subs = thing.await()
                        .map {server ->
                            aclient.data.forPath("$dir/$server").await() to server
                        }.map {(data, server) ->
                            KurojiEventrouter.Subscriber.parseFrom(data) to server
                        }.map {(sub, server) ->
                            val attributes = Attributes.newBuilder().set(SERVER_NAME_ATTRIBUTE_KEY, server).build()
                            EquivalentAddressGroup(InetSocketAddress(sub.hostname, sub.port), attributes)
                        }
                listener.onAddresses(subs, Attributes.EMPTY)
                if (Watcher.Event.EventType.NodeDeleted == thing.event().await().type)
                    break
            }
        }
    }

    override fun getServiceAuthority(): String {
        //TODO Figure out what this does
        return "none"
    }
}