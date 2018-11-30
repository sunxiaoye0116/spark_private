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

package org.apache.spark.broadcast

import java.io._
import java.net.{URI, URL, URLConnection}
import java.util.concurrent.TimeUnit

import org.apache.spark._
import org.apache.spark.bold.network.broadcast._
import org.apache.spark.io.CompressionCodec
import org.apache.spark.storage.{BlockManager, BroadcastBlockId, StorageLevel}
import org.apache.spark.util._

import scala.reflect.ClassTag

/**
  * A [[org.apache.spark.broadcast.Broadcast]] implementation that uses HTTP server
  * as a broadcast mechanism. The first time a HTTP broadcast variable (sent as part of a
  * task) is deserialized in the executor, the broadcasted data is fetched from the driver
  * (through a HTTP server running at the driver) and stored in the BlockManager of the
  * executor to speed up future accesses.
  */

private[spark] class BoldBroadcast[T: ClassTag](
  @transient var value_ : T, isLocal: Boolean, id: Long)
  extends Broadcast[T](id) with Logging with Serializable {
  logDebug("[BOLD] HttpBroadcast is created" + id)

  //  override protected def getValue() = value_
  override protected def getValue(): T = {
    //    logDebug("[BOLD] getValue is called")
    value_
  }

  private val blockId = BroadcastBlockId(id)

  /*
   * Broadcasted data is also stored in the BlockManager of the driver. The BlockManagerMaster
   * does not need to be told about this block as not only need to know about this data block.
   */
  BoldBroadcast.synchronized {
    logDebug("[BOLD] putSingle")
    SparkEnv.get.blockManager.putSingle(
      blockId, value_, StorageLevel.MEMORY_AND_DISK, tellMaster = false)
  }

  if (!isLocal) {
    logDebug("[BOLD] !isLocal")
    BoldBroadcast.write(id, value_)
  }

  /**
    * Remove all persisted state associated with this HTTP broadcast on the executors.
    */
  override protected def doUnpersist(blocking: Boolean) {
    logInfo("doUnpersist is called" + id)
    BoldBroadcast.unpersist(id, removeFromDriver = false, blocking)
  }

  /**
    * Remove all persisted state associated with this HTTP broadcast on the executors and driver.
    */
  override protected def doDestroy(blocking: Boolean) {
    logInfo("doDestroy is called" + id)
    BoldBroadcast.unpersist(id, removeFromDriver = true, blocking)
  }

  /** Used by the JVM when serializing this object. */
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    logDebug("[BOLD] writeObject() is called")
    assertValid()
    out.defaultWriteObject()
    logDebug("[BOLD] writeObject() is returned")
  }

  /** Used by the JVM when deserializing this object. */
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    logDebug("[BOLD] readObject() is called")
    in.defaultReadObject()
    BoldBroadcast.synchronized {
      SparkEnv.get.blockManager.getSingle(blockId) match {
        case Some(x) => value_ = x.asInstanceOf[T]
        case None => {
          logInfo("Started reading broadcast variable " + id)
          val start = System.nanoTime
          value_ = BoldBroadcast.read[T](id)
          val timeRead = (System.nanoTime - start) / 1e6
          logInfo(this.getClass.getSimpleName + ".read() variable " + id + " took " + timeRead + " ms")
          /*
           * We cache broadcast data in the BlockManager so that subsequent tasks using it
           * do not need to re-fetch. This data is only used locally and no other node
           * needs to fetch this block, so we don't notify the master.
           */
          SparkEnv.get.blockManager.putSingle(
            blockId, value_, StorageLevel.MEMORY_AND_DISK, tellMaster = false)
          val time = (System.nanoTime - start) / 1e9
          logInfo("Reading broadcast variable " + id + " took " + time + " s")
        }
      }
    }
  }
}


/*
  Major difference between Http and Bold is at the Object
 */
private[broadcast] object BoldBroadcast extends Logging {
  private var initialized = false
  private var broadcastDir: File = null
  private var compress: Boolean = false
  private var bufferSize: Int = 65536
  private var serverUri: String = null
  private var broadcastDirPath: String = null
  private var socketFile: String = null
  private var server: HttpServer = null
  private var securityManager: SecurityManager = null
  private var blockManager: BlockManager = null
  private var boldManager: BoldManager = null

  // TODO: This shouldn't be a global variable so that multiple SparkContexts can coexist
  private val files = new TimeStampedHashSet[File]
  private val httpReadTimeout = TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES).toInt
  private var compressionCodec: CompressionCodec = null
  private var cleaner: MetadataCleaner = null

  def getServerUri: String = {
    serverUri
  }

  def getBroadcastDir: String = {
    broadcastDirPath
  }

  def getUDSFile: String = {
    socketFile
  }

  def initialize(isDriver: Boolean, conf: SparkConf, securityMgr: SecurityManager, blockMgr: BlockManager) {
    synchronized {
      if (!initialized) {
        bufferSize = conf.getInt("spark.buffer.size", 65536)
        compress = conf.getBoolean("spark.broadcast.compress", true)
        securityManager = securityMgr
        blockManager = blockMgr
        if (isDriver) {
          logDebug("[BOLD] initialized() called at driver")
          createServer(conf)
          conf.set("spark.boldBroadcast.uri", serverUri)
          conf.set("spark.boldBroadcast.broadcastDir", broadcastDirPath)
          conf.set("spark.boldBroadcast.socketFile", "/home/bold/domain_socket_file")
        } else {
          // register the broadcast folder at worker
          // so that the fold can be removed after app finishes
          ShutdownHookManager.registerShutdownDeleteDir(
            new File(conf.get("spark.boldBroadcast.broadcastDir")))
        }
        serverUri = conf.get("spark.boldBroadcast.uri")
        broadcastDirPath = conf.get("spark.boldBroadcast.broadcastDir")
        socketFile = conf.get("spark.boldBroadcast.socketFile")

        boldManager = new BoldManager(isDriver, conf) //, blockManager)
        boldManager.initialize()
        blockManager.setBoldManager(boldManager)
        //        boldManager.init()

        // TODO: should have a cleaner for BoldBroadcast as well...
        cleaner = new MetadataCleaner(MetadataCleanerType.HTTP_BROADCAST, cleanup, conf)
        compressionCodec = CompressionCodec.createCodec(conf)
        initialized = true
      }
    }
  }

  def stop() {
    synchronized {
      if (server != null) {
        server.stop()
        server = null
      }
      if (cleaner != null) {
        cleaner.cancel()
        cleaner = null
      }
      if (boldManager != null) {
        boldManager.stop()
        boldManager = null
      }
      compressionCodec = null
      initialized = false
    }
  }

  private def createServer(conf: SparkConf) {
    //    broadcastDir = Utils.createTempDir(Utils.getLocalDir(conf), "broadcast")
    broadcastDir = Utils.createTempDir("/mnt/ramdisk/", "broadcast")
    logInfo("broadcastDir: " + broadcastDir)
    broadcastDirPath = broadcastDir.getAbsolutePath + "/"
    val broadcastPort = conf.getInt("spark.broadcast.port", 0)
    server =
      new HttpServer(conf, broadcastDir, securityManager, broadcastPort, "HTTP broadcast server")
    server.start()
    //    serverUri = server.uri
    serverUri = server.uri
    //    s"${Utils.localHostNameForURI()}"
    logInfo("Broadcast server started at " + serverUri)
    logInfo("[BOLD] Broadcast server started at " + serverUri + "/" + broadcastDirPath)
  }

//  private def createServer_wo_Http(conf: SparkConf) {
//    // TODO: register to the broadcast master to multicast agent
//    // TODO: directory should comes from a conf
//    serverUri = s"${Utils.localHostNameForURI()}"
//    broadcastDir = Utils.createTempDir("/mnt/ramdisk/", "broadcast")
//    broadcastDirPath = broadcastDir.getAbsolutePath + "/"
//    //    val broadcastPort = conf.getInt("spark.broadcast.port", 0)
//    //    server =
//    //      new HttpServer(conf, broadcastDir, securityManager,
//    // broadcastPort, "HTTP broadcast server")
//    //    server.start()
//    //    serverUri = server.uri
//    logInfo("[BOLD] Broadcast server started at " + serverUri + ":" + broadcastDirPath)
//  }

  def getFile(id: Long): File = new File(broadcastDir, BroadcastBlockId(id).name)

  private def write(id: Long, value: Any) {
    val file = getFile(id)
    val fileOutputStream = new FileOutputStream(file)
    Utils.tryWithSafeFinally {
      logDebug("[BOLD] file.getName(): " + file.getName + " broadcastDirPath: " + broadcastDirPath)
      val out: OutputStream = {
        if (compress) {
          logDebug("[BOLD] compress")
          compressionCodec.compressedOutputStream(fileOutputStream)
        } else {
          logDebug("[BOLD] no compress")
          new BufferedOutputStream(fileOutputStream, bufferSize)
        }
      }
      logDebug("[BOLD] create serializer")
      val ser = SparkEnv.get.serializer.newInstance()
      val serOut = ser.serializeStream(out)
      Utils.tryWithSafeFinally {
        val writeObjectCalled = System.nanoTime()
        serOut.writeObject(value)
        val time = (System.nanoTime - writeObjectCalled) / 1e6
        logInfo("[BOLD] writeObject() took " + time + " ms")
      } {
        serOut.close()
      }
      logDebug("[BOLD] add file to files")
      files += file
    } {
      fileOutputStream.close()
    }
    logDebug("[BOLD] file size: " + file.length())
    // Republic apecific: start
    logDebug("[BOLD] call agent.add()")
    boldManager.add(id, file.length())
    logDebug("[BOLD] return from agent.add()")

    if (file.length() > 1 * 1024 * 1024) {
      logDebug("[BOLD] large data, call agent.push()")
      boldManager.push(id, 0)
      logDebug("[BOLD] return from agent.push()")
    }
    // Republic apecific: end
    logDebug("[BOLD] return from write()")
  }

  private def read[T: ClassTag](id: Long): T = {
    logInfo("[BOLD] read() is called. serverUri: " + serverUri + ", id: " + id)
    logInfo("create InputStream for broadcast-" + id)
    val in = {
      logInfo("[BOLD] start reading data from master " + "(ip: " + serverUri + ":" +
        broadcastDirPath + BroadcastBlockId(id).name + ")")
      var inputStream: InputStream = boldManager.read(id, 5)
      logInfo("[BOLD] finish reading data")
      if (inputStream == null) {

        val url = serverUri + "/" + BroadcastBlockId(id).name
        logInfo("[Bold] url: " + url)
        var uc: URLConnection = null

        if (securityManager.isAuthenticationEnabled()) {
          logDebug("broadcast security enabled")
          val newuri = Utils.constructURIForAuthentication(new URI(url), securityManager)
          uc = newuri.toURL.openConnection()
          uc.setConnectTimeout(httpReadTimeout)
          uc.setAllowUserInteraction(false)
        } else {
          logDebug("broadcast security disabled")
          uc = new URL(url).openConnection()
          uc.setConnectTimeout(httpReadTimeout)
        }
        Utils.setupSecureURLConnection(uc, securityManager)

        uc.setReadTimeout(httpReadTimeout)
        inputStream = uc.getInputStream
      }
      if (compress) {
        compressionCodec.compressedInputStream(inputStream)
      } else {
        new BufferedInputStream(inputStream, bufferSize)
      }
    }
    val ser = SparkEnv.get.serializer.newInstance()
    val serIn = ser.deserializeStream(in)
    logDebug("create readObject for broadcast-" + id)
    Utils.tryWithSafeFinally {
      serIn.readObject[T]()
    } {
      serIn.close()
    }
  }

  /**
    * Remove all persisted blocks associated with this HTTP broadcast on the executors.
    * If removeFromDriver is true, also remove these persisted blocks on the driver
    * and delete the associated broadcast file.
    */
  def unpersist(id: Long, removeFromDriver: Boolean, blocking: Boolean): Unit = synchronized {
    logInfo("[BOLD] unpersist for broadcast_" + id + " is called @ master only")
    SparkEnv.get.blockManager.master.removeBroadcast(id, removeFromDriver, blocking)
    if (removeFromDriver) {
      logInfo("[BOLD] also remove from the driver")
      val file = getFile(id)
      files.remove(file)
      deleteBroadcastFile(file)
    }
  }

  /**
    * Periodically clean up old broadcasts by removing the associated map entries and
    * deleting the associated files.
    */
  private def cleanup(cleanupTime: Long) {
    logInfo("[BOLD] cleanup is called")
    val iterator = files.internalMap.entrySet().iterator()
    while (iterator.hasNext) {
      val entry = iterator.next()
      val (file, time) = (entry.getKey, entry.getValue)
      if (time < cleanupTime) {
        iterator.remove()
        deleteBroadcastFile(file)
      }
    }
  }

  private def deleteBroadcastFile(file: File) {
    logInfo("[BOLD] deleteBroadcastFile for " + file.getPath())
    try {
      if (file.exists) {
        if (file.delete()) {
          logInfo("Deleted broadcast file: %s".format(file))
        } else {
          logWarning("Could not delete broadcast file: %s".format(file))
        }
      }
    } catch {
      case e: Exception =>
        logError("Exception while deleting broadcast file: %s".format(file), e)
    }
  }
}
