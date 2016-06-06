/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.dstream

import java.rmi.registry.LocateRegistry

import org.apache.spark.{Logging, SparkEnv}
import org.apache.spark.storage.{StorageLevel, StreamBlockId}
import org.apache.spark.streaming.StreamingContext
import pt.inescid.gsd.art.{ArtManager, RemoteArtManager}

import scala.io.BufferedSource
import scala.reflect.ClassTag
import java.net.{ConnectException, InetAddress, InetSocketAddress, Socket}
import java.nio.ByteBuffer
import java.nio.channels.{ReadableByteChannel, SocketChannel}
import java.io.{EOFException, PrintStream}
import java.util.concurrent.ArrayBlockingQueue

import org.apache.spark.streaming.receiver.Receiver

import scala.util.Random


/**
 * An input stream that reads blocks of serialized objects from a given network address.
 * The blocks will be inserted directly into the block store. This is the fastest way to get
 * data into Spark Streaming, though it requires the sender to batch data and serialize it
 * in the format that the system is configured with.
 */
private[streaming]
class RawInputDStream[T: ClassTag](
    @transient ssc_ : StreamingContext,
    host: String,
    port: Int,
    storageLevel: StorageLevel
  ) extends ReceiverInputDStream[T](ssc_ ) with Logging {

  def getReceiver(): Receiver[T] = {
    // new RawNetworkReceiver(host, port, storageLevel, ssc.artManager).asInstanceOf[Receiver[T]]
    new RawNetworkReceiver(host, port, storageLevel).asInstanceOf[Receiver[T]]
  }
}

private[streaming]
class RawNetworkReceiver(host: String, port: Int, storageLevel: StorageLevel)
  extends Receiver[Any](storageLevel) with Logging {

  // var artManager: ArtManager = null

//  def this(host: String, port: Int, storageLevel: StorageLevel, artManager: ArtManager) {
//    this(host, port, storageLevel)
//    this.artManager = artManager
//  }

  var blockPushingThread: Thread = null

  def onStart() {
    // Open a socket to the target address and keep reading from it
    logInfo("Connecting to " + host + ":" + port)
    val channel = SocketChannel.open()
    channel.configureBlocking(true)
    channel.connect(new InetSocketAddress(host, port))
    logInfo("Connected to " + host + ":" + port)

    val queue = new ArrayBlockingQueue[ByteBuffer](2)

    blockPushingThread = new Thread {
      setDaemon(true)
      override def run() {
        var nextBlockNumber = 0
        while (true) {
          val buffer = queue.take()
          nextBlockNumber += 1
          store(buffer)
        }
      }
    }
    blockPushingThread.start()

    val lengthBuffer = ByteBuffer.allocate(4)

    // find StreamingContext/Driver host
    val driverHostname =
      (1 to 6).map("ginja-a" + _).find(hostname =>
        try {
          val socket = new Socket(InetAddress.getByName(hostname), 9999)
          socket.close()
          true
        } catch {
          case ce: ConnectException => false
        }
      ).get

    while (true) {

      val startTick = System.currentTimeMillis()

      lengthBuffer.clear()
      readFully(channel, lengthBuffer)
      lengthBuffer.flip()
      val length = lengthBuffer.getInt()
      val dataBuffer = ByteBuffer.allocate(length)
      readFully(channel, dataBuffer)
      dataBuffer.flip()
      // logInfo("Read a block with " + length + " bytes")

      val duration = System.currentTimeMillis() - startTick
      val rate = length / duration
      logInfo(s"Read a block with $length bytes in $duration millis. " +
        "Ingestion Rate: $rate bytes/millis")

      // SROE
      // discard lines in blocks or blocks
//      println("##### dataBuffer contents: " + new String(dataBuffer.array()))
//      println("##### ArtManager location: " + artManager)
//      println("##### ArtManager currentAccuracy: " + artManager.accuracy)


//      val registry = LocateRegistry.getRegistry("ginja-a1")
//      val stub = registry.lookup(artManager.ArtServiceName).asInstanceOf[RemoteArtManager]
//      println("##### ArtManager currentAccuracy RMI: " + stub.getAccuracy())

      // FIXME maintain same socket always open
      val s = new Socket(InetAddress.getByName(driverHostname), 9998)
      val in = new BufferedSource(s.getInputStream()).getLines()
      val out = new PrintStream(s.getOutputStream())
      out.println(rate)
      out.flush()
      val accuracy = in.next().toDouble
      s.close()
      println("##### ArtManager currentAccuracy Socket: " + accuracy)

      if(Random.nextInt(100) < accuracy) {
        queue.put(dataBuffer)
      }
    }
  }

  def onStop() {
    if (blockPushingThread != null) blockPushingThread.interrupt()
  }

  /** Read a buffer fully from a given Channel */
  private def readFully(channel: ReadableByteChannel, dest: ByteBuffer) {
    while (dest.position < dest.limit) {
      if (channel.read(dest) == -1) {
        throw new EOFException("End of channel")
      }
    }
  }
}
