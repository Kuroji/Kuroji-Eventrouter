package xyz.usbpc.kuroji.eventrouter.server.grpc

import io.grpc.*
import xyz.usbpc.kuroji.eventrouter.api.KurojiEventrouter
import xyz.usbpc.kuroji.eventrouter.server.zoo.ZookeeperNameResolver
import java.util.*

object EventRouterLoadBalancerFactory : LoadBalancer.Factory() {
    override fun newLoadBalancer(helper: LoadBalancer.Helper): LoadBalancer {
        return EventRouterLoadBalancer(helper)
    }
    class EventRouterLoadBalancer(val helper: LoadBalancer.Helper) : LoadBalancer() {
        val aggregatedState: ConnectivityState
            get() {
                val states = EnumSet.noneOf(ConnectivityState::class.java)
                for (subchannel in channelsByAdress.values) {
                    states.add(subchannel.attributes[STATE_KEY]!!.value.state)
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

        internal class Ref<T>(var value: T)
        companion object {
            private val STATE_KEY = Attributes.Key.of<Ref<ConnectivityStateInfo>>("state-info")
        }
        override fun handleNameResolutionError(error: Status?) {
            //NOP
        }

        override fun shutdown() {
            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
        }

        override fun handleSubchannelState(subchannel: Subchannel, stateInfo: ConnectivityStateInfo) {
            if (channelsByAdress[subchannel.addresses] !== subchannel)
                return

            if (stateInfo.state == ConnectivityState.IDLE) {
                subchannel.requestConnection()
            }
            subchannel.attributes[STATE_KEY]?.value = stateInfo
            updateBalancingState(aggregatedState)
        }

        private fun updateBalancingState(state: ConnectivityState) {
            val activeList = knownChannelsHosts.filter { (ch, _) ->
                ch.attributes[STATE_KEY]!!.value.state == ConnectivityState.READY
            }.map { (ch, _) -> ch }
            helper.updateBalancingState(state, Picker(activeList, emptyMap()))
        }

        val channelsByAdress = mutableMapOf<EquivalentAddressGroup, Subchannel>()
        val knownChannelsHosts = mutableListOf<Pair<Subchannel, String>>()
        override fun handleResolvedAddressGroups(servers: MutableList<EquivalentAddressGroup>, attributes: Attributes) {
            println(servers)
            val currentList = servers
                    .map { it to it.attributes[ZookeeperNameResolver.SERVER_NAME_ATTRIBUTE_KEY]!! }
            val newServers = currentList
                    .filter { new ->
                        knownChannelsHosts.none { old -> new.second == old.second }
                    }.sortedBy { it.second }
            val oldServers = knownChannelsHosts.filter { old ->
                currentList.none { new -> old.second == new.second }
            }
            knownChannelsHosts.removeAll { (_, name) ->  oldServers.any { (_, toRemove) -> name == toRemove }}
            val newChannels = newServers.map { (adress, name) ->
                val attr = Attributes.newBuilder()
                        .set(STATE_KEY, Ref(ConnectivityStateInfo.forNonError(ConnectivityState.IDLE)))
                        .build()
                val ch = helper.createSubchannel(adress, attr)
                channelsByAdress[ch.addresses] = ch
                ch to name
            }
            newChannels.forEach { (ch, _) -> ch.requestConnection() }
            knownChannelsHosts.insert(newChannels)
            updateBalancingState(aggregatedState)
            oldServers.forEach { (ch, _ ) ->
                ch.shutdown()
                channelsByAdress.remove(ch.addresses)
            }
        }

        fun MutableList<Pair<Subchannel, String>>.insert(channels: List<Pair<Subchannel, String>>) {
            var curIndex = 0
            for (channel in channels) {
                while (true) {
                    if (this.size == curIndex || this[curIndex].second > channel.second) {
                        this.add(curIndex, channel)
                        break
                    }
                    curIndex++
                }
                curIndex++
            }
        }

    }

    internal class Picker(val subchannels: List<LoadBalancer.Subchannel>, val specialRouting : Map<Long, LoadBalancer.Subchannel>) : LoadBalancer.SubchannelPicker() {
        companion object {
            val HEADER_ROUTING_KEY = Metadata.Key.of("routing-bin", Metadata.BINARY_BYTE_MARSHALLER)
        }
        override fun pickSubchannel(args: LoadBalancer.PickSubchannelArgs): LoadBalancer.PickResult {
            //println("Trying to pick from these subchannels: $subchannels")
            val routingInfo = KurojiEventrouter.RoutingInfo.parseFrom(args.headers[HEADER_ROUTING_KEY]!!)
            specialRouting[routingInfo.id]?.let { subchannel ->
                return LoadBalancer.PickResult.withSubchannel(subchannel)
            }
            if (subchannels.isEmpty())
                return LoadBalancer.PickResult.withNoResult()
            return LoadBalancer.PickResult.withSubchannel(subchannels[(routingInfo.id % subchannels.size).toInt()])
        }

    }

}