package xyz.usbpc.kuroji.eventrouter.server.grpc

import io.grpc.*
import kotlinx.coroutines.experimental.future.await
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.newSingleThreadContext
import kotlinx.coroutines.experimental.runBlocking
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.x.async.AsyncCuratorFramework
import org.apache.zookeeper.Watcher
import xyz.usbpc.kuroji.eventrouter.api.KurojiEventrouter
import xyz.usbpc.kuroji.eventrouter.server.getLogger
import java.util.*

class EventRouterLoadBalancerFactory(private val client: CuratorFramework) : LoadBalancer.Factory() {
    override fun newLoadBalancer(helper: LoadBalancer.Helper): LoadBalancer {
        return EventRouterLoadBalancer(helper, client)
    }
    class EventRouterLoadBalancer(private val helper: LoadBalancer.Helper, private val client: CuratorFramework) : LoadBalancer() {
        private val loadBalancerContext = newSingleThreadContext("Load Balancer Context!")
        private val channelsByAddress = mutableMapOf<EquivalentAddressGroup, Subchannel>()
        private val knownChannelsHosts = mutableListOf<Subchannel>()
        private val aggregatedState: ConnectivityState
            get() {
                val states = EnumSet.noneOf(ConnectivityState::class.java)
                for (subchannel in channelsByAddress.values) {
                    states.add(subchannel.stateInfo.state)
                }
                if (states.contains(ConnectivityState.READY)) {
                    return ConnectivityState.READY
                }
                if (states.contains(ConnectivityState.CONNECTING)) {
                    return ConnectivityState.CONNECTING
                }
                return if (states.contains(ConnectivityState.IDLE)) {
                    ConnectivityState.CONNECTING
                } else ConnectivityState.TRANSIENT_FAILURE
            }

        private class Ref<T>(var value: T)

        companion object {
            private val STATE_INFO_KEY = Attributes.Key.of<Ref<ConnectivityStateInfo>>("grpc-stateInfo-info")
            private val STATE_KEY = Attributes.Key.of<Ref<KurojiEventrouter.Subscriber.Status>>("state-info")
            private val NAME_KEY = Attributes.Key.of<String>("channel-name-info")
            private val CLAIMED_KEY = Attributes.Key.of<Ref<List<KurojiEventrouter.RoutingInfo>>>("claimed-info")
            private val LOGGER = this.getLogger()
        }

        override fun handleResolvedAddressGroups(clients: MutableList<EquivalentAddressGroup>, attributes: Attributes) = runBlocking(loadBalancerContext) {
            val newClients = clients.filter { addrGroup ->
                addrGroup.name !in knownChannelsHosts
            }
            val clientsNames = clients.map { it.name }
            val oldClients = knownChannelsHosts.filter { channel ->
                channel.name !in clientsNames
            }
            val newChannels = newClients.map { client ->
                helper.createSubchannel(client, newAttributes(client.name))
            }.onEach { channel ->
                        channel.requestConnection()
                        watchZookeeperFor(channel)
                    }.sortedBy { it.name }

            knownChannelsHosts.removeAll { it.name in oldClients }
            oldClients.forEach { channel ->
                channelsByAddress.remove(channel.addresses)
            }
            knownChannelsHosts.insert(newChannels)
            newChannels.forEach { channel ->
                channelsByAddress[channel.addresses] = channel
            }
            updateBalancingState()
        }

        private fun newAttributes(name: String) =
                Attributes.newBuilder()
                        .set(STATE_INFO_KEY, Ref(ConnectivityStateInfo.forNonError(ConnectivityState.IDLE)))
                        .set(STATE_KEY, Ref(KurojiEventrouter.Subscriber.Status.UNKNOWN))
                        .set(NAME_KEY, name)
                        .set(CLAIMED_KEY, Ref(emptyList()))
                        .build()

        private fun watchZookeeperFor(channel: Subchannel) = launch(loadBalancerContext) {
            val aclient = AsyncCuratorFramework.wrap(client.usingNamespace("eventrouter/clients/totallyNew"))
            while (isActive) {
                val stage = aclient.watched().data.forPath("/${channel.name}")
                val data = KurojiEventrouter.Subscriber.parseFrom(stage.await())
                if (channel.myState != data.status) {
                    channel.myState = data.status
                }
                if (channel.claimed != data.claimedList) {
                    channel.claimed = data.claimedList
                }
                updateBalancingState()
                if (stage.event().await().type != Watcher.Event.EventType.NodeDataChanged)
                    break
            }
        }
        override fun handleSubchannelState(subchannel: Subchannel, stateInfo: ConnectivityStateInfo) = runBlocking(loadBalancerContext) {
            if (channelsByAddress[subchannel.addresses] !== subchannel)
                return@runBlocking

            if (stateInfo.state == ConnectivityState.IDLE) {
                subchannel.requestConnection()
            }
            subchannel.stateInfo = stateInfo
            updateBalancingState()
        }

        private fun updateBalancingState() {
            val activeList = knownChannelsHosts.filter { channel ->
                channel.stateInfo.state == ConnectivityState.READY && channel.myState == KurojiEventrouter.Subscriber.Status.RUNNING
            }
            val map = mutableMapOf<KurojiEventrouter.RoutingInfo, Subchannel>()
            knownChannelsHosts.filter { channel ->
                channel.stateInfo.state == ConnectivityState.READY
            }.forEach { channel ->
                        channel.claimed.forEach { claim ->
                            map[claim] = channel
                        }
                    }
            helper.updateBalancingState(aggregatedState, Picker(activeList, map))
        }

        /**
         * Inserts new channels in alphabetical order, needed to make sure all instances use the routing key the same
         */
        private fun MutableList<Subchannel>.insert(channels: List<Subchannel>) {
            var curIndex = 0
            for (channel in channels) {
                while (true) {
                    if (this.size == curIndex || this[curIndex].name > channel.name) {
                        this.add(curIndex, channel)
                        break
                    }
                    curIndex++
                }
                curIndex++
            }
        }

        private class Picker(val subchannels: List<LoadBalancer.Subchannel>, val specialRouting : Map<KurojiEventrouter.RoutingInfo, LoadBalancer.Subchannel>) : SubchannelPicker() {
            companion object {
                val HEADER_ROUTING_KEY = Metadata.Key.of("routing-bin", Metadata.BINARY_BYTE_MARSHALLER)
            }
            override fun pickSubchannel(args: LoadBalancer.PickSubchannelArgs): LoadBalancer.PickResult {
                //println("Trying to pick from these subchannels: $subchannels")
                val routingInfo = KurojiEventrouter.RoutingInfo.parseFrom(args.headers[HEADER_ROUTING_KEY]!!)
                specialRouting[routingInfo]?.let { subchannel ->
                    return LoadBalancer.PickResult.withSubchannel(subchannel)
                }
                if (subchannels.isEmpty())
                    return LoadBalancer.PickResult.withNoResult()
                return LoadBalancer.PickResult.withSubchannel(subchannels[(routingInfo.id % subchannels.size).toInt()])
            }

        }

        override fun handleNameResolutionError(error: Status) {
            //NOP
        }

        override fun shutdown() {
            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
        }

        //Extensions for Subchannel to make my life easier
        operator fun List<Subchannel>.contains(other: String) : Boolean = this.any { it.name == other }

        private val Subchannel.name : String
            get() = this.attributes[NAME_KEY]!!
        private var Subchannel.stateInfo: ConnectivityStateInfo
            inline set(value) {
                this.attributes[STATE_INFO_KEY]!!.value = value
            }
            inline get() = this.attributes[STATE_INFO_KEY]!!.value
        private var Subchannel.claimed : List<KurojiEventrouter.RoutingInfo>
            inline set(value) {
                this.attributes[CLAIMED_KEY]!!.value = value
            }
            inline get() = this.attributes[CLAIMED_KEY]!!.value

        private var Subchannel.myState : KurojiEventrouter.Subscriber.Status
            inline set(value) {
                this.attributes[STATE_KEY]!!.value = value
            }
            inline get() = this.attributes[STATE_KEY]!!.value
        private val EquivalentAddressGroup.name : String
            inline get() = this.attributes[ZookeeperNameResolver.SERVER_NAME_ATTRIBUTE_KEY]!!

    }


}
