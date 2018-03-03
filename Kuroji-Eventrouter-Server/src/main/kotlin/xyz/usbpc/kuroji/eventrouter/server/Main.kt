package xyz.usbpc.kuroji.eventrouter.server

import kotlinx.coroutines.experimental.newSingleThreadContext
import kotlinx.coroutines.experimental.runBlocking
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import kotlin.concurrent.thread

fun Any.getLogger() : Logger =
    if (this.javaClass.name.endsWith("\$Companion"))
        LoggerFactory.getLogger(this.javaClass.declaringClass)
    else
        LoggerFactory.getLogger(this.javaClass)

fun main(args: Array<String>) = runBlocking {
    //TODO get arguments fromm command line maybe config or enviroment variables?
    //(zookeeper connect string, port for gRPC getting message form WS)
    val eventrouter = Eventrouter()

    Runtime.getRuntime().addShutdownHook(
            thread(start = false, isDaemon = false) {
                runBlocking(newSingleThreadContext("Shutdown-Context")) {
                    eventrouter.stop()
                }
            }
    )

    eventrouter.awaitTermination()
}
