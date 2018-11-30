package org.apache.spark.bold.network.broadcast;

/**
 * Created by xs6 on 5/11/16.
 */


import com.etsy.net.JUDS;
import com.etsy.net.UnixDomainSocket;
import com.etsy.net.UnixDomainSocketClient;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.apache.spark.SparkConf;
import org.apache.spark.bold.io.FileChunkInputStream;
import org.apache.spark.scheduler.SchedulerBackend;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BroadcastBlockId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

//import org.apache.spark.storage.BlockManager;


public class BoldManager {
    private final Logger logger = LoggerFactory.getLogger(BoldManager.class);
    private final int CODE_REQ_INT = 90;
    private final int CODE_REP_INT = 91;
    private final int CODE_REQ_WRT = 92;
    private final int CODE_REP_WRT = 93;
    private final int CODE_REQ_PSH = 94;
    private final int CODE_REP_PSH = 95;
    private final int CODE_REQ_READ = 96;
    private final int CODE_REP_READ = 97;
    private final int CODE_REQ_DLT = 98;
    private final int CODE_REP_DLT = 99;

    private final int REPLY_SUCCESS = 50;
    private final int REPLY_FAIL = 51;
    private final int REPLY_ALTER = 52;

    private final int MSG_HDRLEN = 32 + 16;
    private final int IP_ADDR_LEN = 16;
    private final int APP_ID_LEN = 64;
    private final int DRIVER_EXECUTOR_ID = -1;
    private final ConcurrentHashMap<MsgTuple, Object> pendingRequest = new ConcurrentHashMap<>();
    private int readResults;
    private long readStart;
    private long readJump;
    private long readEnd;
    private boolean isDriver;
    private SparkConf conf;
    //    private BlockManager blockManager;
    private SchedulerBackend schedulerBackend;
    private boolean initialized = false;
    private String serverIP; // pure IP address string of the master
    private String broadcastDir; // absolute path of the broadcast data dir for the application
    private UUID broadcastID;
    private long broadcastIDmsb; // TODO: msb and lsb in Msg seems to be redundant
    private long broadcastIDlsb;
    private String executorId;
    private String appId;
    private String boldId;
    private SocketConnection connection;


    public BoldManager() {
        this.isDriver = false;
        ConsoleAppender console = new ConsoleAppender(); //create appender
        //configure the appender
//        String PATTERN = "%d [%p|%c|%C{1}] %m%n";
        String PATTERN = "%m%n";
        console.setLayout(new PatternLayout(PATTERN));
        console.setThreshold(Level.DEBUG);
        console.activateOptions();
        //add appender to any Logger (here is root)
        org.apache.log4j.Logger.getRootLogger().addAppender(console);
    }

    public BoldManager(boolean isDriver, SparkConf conf) { //}, BlockManager blm) {
        this.isDriver = isDriver;
        this.conf = conf;
//        this.blockManager = blm;
    }

    public void setSchedulerBackend(SchedulerBackend sb) {
        this.schedulerBackend = sb;
    }

    private void waitReply(MsgTuple mt, Object obj) {
        logger.debug("MsgTuple:\t" + mt.toString());
        logger.debug("Object:\t" + obj);
        synchronized (obj) {
            logger.debug("Synchronized:\t" + mt.toString());
            try {
                while (pendingRequest.containsKey(mt)) {
                    obj.wait(5);
                }

                assert !(pendingRequest.containsKey(mt));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public InputStream read(long id, long interval) {
        logger.info("[bold] sending read, id[" + id + "]");

        BlockId bid = new BroadcastBlockId(id, "");
//        String uri = serverIP + ":" + broadcastDir + bid.name();
        String filename = broadcastDir + bid.name();
//        logger.debug("uri:" + uri + ", filename: " + filename);

        MsgTuple mt = new MsgTuple(id, CODE_REP_READ);
        Object obj = new Object(); // TODO: this could the reply object

        /* send MsgHdr */
        synchronized (connection) {
            assert !pendingRequest.containsKey(mt);
            pendingRequest.put(mt, obj);
            sendMsgHdr(broadcastIDmsb, broadcastIDlsb, id, CODE_REQ_READ, MSG_HDRLEN);
        }
//        logger.info("[BOLD] data read sent, uri: " + uri + ", filename: " + filename);

        /* reply */
//        boolean ret = recvReply(broadcastIDmsb, broadcastIDlsb, id, CODE_REPLY_READ, MSG_HDRLEN + 4 + 4 * 3);
        waitReply(mt, obj);
        switch (readResults) {
            case REPLY_SUCCESS:
                logger.info("[BOLD] read successed");
                // TODO: if there are multiple reads for different data, readStart, readJump, readEnd could be messed up
                logger.debug("start [" + readStart + "] jump [" + readJump + "] fileSize [" + readEnd + "]");
                Vector<InputStream> inputStreams = new Vector<>();
                FileChunkInputStream fcis;
                try {
                    if (readStart < readJump) {
                        fcis = new FileChunkInputStream(new File(filename), readStart, readJump, interval);
                        inputStreams.add(fcis);
                    }
                    if (0 < readStart) {
                        fcis = new FileChunkInputStream(new File(filename), 0L, readStart, interval);
                        inputStreams.add(fcis);
                    }
                    if (readJump < readEnd) {
                        fcis = new FileChunkInputStream(new File(filename), readJump, readEnd, interval);
                        inputStreams.add(fcis);
                    }
                } catch (FileNotFoundException e) {
                    logger.error("file not found: ", filename);
                    e.printStackTrace();
                }

                Enumeration<InputStream> enu = inputStreams.elements();
                return new SequenceInputStream(enu);

            case REPLY_FAIL:
                logger.info("[BOLD] read() failed");
                return null;
            case REPLY_ALTER:
                logger.warn("[BOLD] read() altered");
                return null;
            default:
                logger.info("[BOLD] read() error");
                return null;
        }
    }

    public boolean remove(long id) {
        logger.info("[bold] sending remove, id[" + id + "]");

        MsgTuple mt = new MsgTuple(id, CODE_REP_DLT);
        Object obj = new Object();

        /* send MsgHdr */
        synchronized (connection) {
            assert !pendingRequest.containsKey(mt);
            pendingRequest.put(mt, obj);
            sendMsgHdr(broadcastIDmsb, broadcastIDlsb, id, CODE_REQ_DLT, MSG_HDRLEN);
        }

        /* reply */
//        boolean ret = recvReply(broadcastIDmsb, broadcastIDlsb, id, CODE_REPLY_REMOVE, MSG_HDRLEN + 4);
        waitReply(mt, obj);
        logger.info("[BOLD] remove successfully");
        return true;
    }

    public boolean add(long id, long size) {
        logger.info("[bold] sending add, id[" + id + "]" + " size[" + size + "]");

        MsgTuple mt = new MsgTuple(id, CODE_REP_WRT);
        Object obj = new Object();

        /* send MsgHdr */
        synchronized (connection) {
            assert !pendingRequest.containsKey(mt);
            pendingRequest.put(mt, obj);
            sendMsgHdr(broadcastIDmsb, broadcastIDlsb, id, CODE_REQ_WRT, MSG_HDRLEN + 8);
            connection.sendLong(size);
        }

        /* reply */
//        boolean ret = recvReply(broadcastIDmsb, broadcastIDlsb, id, CODE_REPLY_WRITE, MSG_HDRLEN + 4);
        waitReply(mt, obj);
        logger.info("[BOLD] add successfully");
        return true;
    }

    public boolean push(long id, int fanout_max) {
        logger.info("[bold] sending push, id[" + id + "]" + ", fanout_max[" + fanout_max + "]");

        MsgTuple mt = new MsgTuple(id, CODE_REP_PSH);
        Object obj = new Object();

        /* send MsgHdr */
        synchronized (connection) {
            assert !pendingRequest.containsKey(mt);
            pendingRequest.put(mt, obj);
//            logger.info("[BOLD] schedulerBackend: " + schedulerBackend);//            YarnClusterSchedulerBackend
            logger.info("[BOLD] schedulerBackend.getExecutorIPs(): " + schedulerBackend.getExecutorIPs());

            /* send number of receivers */
            int actual_fanout = schedulerBackend.getExecutorIPs().size();
            logger.info("executor list size: " + actual_fanout);
            if (fanout_max > 0) {
                actual_fanout = Math.min(actual_fanout, fanout_max);
            }
            logger.info("push executor list size: " + actual_fanout);

            sendMsgHdr(broadcastIDmsb, broadcastIDlsb, id, CODE_REQ_PSH, MSG_HDRLEN + 8 + IP_ADDR_LEN * actual_fanout);
            connection.sendLong(actual_fanout);

            /* send slave addresses */
            int fanout_counter = 0;
            for (String s : schedulerBackend.getExecutorIPs()) {
                connection.sendStr(s);
                connection.sendBytes(IP_ADDR_LEN - s.length());
                logger.info("executor: " + s);
                fanout_counter++;
                if (fanout_counter == actual_fanout && fanout_max > 0) {
                    break;
                }
            }
        }

        /* reply */
//        boolean ret = recvReply(broadcastIDmsb, broadcastIDlsb, id, CODE_REPLY_PUSH, MSG_HDRLEN + 4);
        waitReply(mt, obj);
        logger.info("[BOLD] push successfully");
        return true;
    }

    private void sendMsgHdr(long msb, long lsb, long id, int type, int len) {
        connection.sendLong(msb);
        connection.sendLong(lsb);
        connection.sendLong(id);
        connection.sendInt(type);
        connection.sendInt(len);
        long time_us = System.nanoTime() / 1000;
        connection.sendLong(time_us / (1000 * 1000));
        connection.sendLong(time_us % (1000 * 1000));
    }

//    private boolean recvReply(long msb, long lsb, long id, int type, int len) {
//        long recvLong;
//        int recvInt;
//
//        recvLong = connection.recvLong();
//        if (recvLong != msb) {
//            logger.error("recv wrong msb: " + recvLong);
//            return false;
//        }
//        recvLong = connection.recvLong();
//        if (recvLong != lsb) {
//            logger.error("recv wrong lsb: " + recvLong);
//            return false;
//        }
//        recvLong = connection.recvLong();
//        if (recvLong != id) {
//            logger.error("recv wrong did: " + recvLong);
//            return false;
//        }
//        recvInt = connection.recvInt();
//        if (recvInt != type) {
//            logger.error("recv wrong type: " + recvInt);
//            return false;
//        }
//        recvInt = connection.recvInt();
//        if (recvInt != len) {
//            logger.error("recv wrong len: " + recvInt);
//            return false;
//        }
//
//        recvInt = connection.recvInt();
//        if (recvInt != REPLY_SUCCESS) {
//            logger.error("recv unsuccessful: " + recvInt);
//            return false;
//        }
//
//        return true;
//    }

    public boolean init(String appId, String executorId) {
        this.appId = appId;
        this.executorId = executorId;
        this.boldId = this.appId + "/" + this.executorId;
        logger.info("[bold] [init] sending init, boldId[" + this.boldId + "]");

        MsgTuple mt = new MsgTuple(CODE_REP_INT);
        Object obj = new Object();

        /* send MsgHdr */
        synchronized (connection) {
            assert !pendingRequest.containsKey(mt);
            pendingRequest.put(mt, obj);
            sendMsgHdr(broadcastIDmsb, broadcastIDlsb, -1, CODE_REQ_INT, MSG_HDRLEN + IP_ADDR_LEN + 4 + 4 + APP_ID_LEN);

            /* send master ip */
            connection.sendStr(this.serverIP);
            connection.sendBytes(IP_ADDR_LEN - this.serverIP.length());

            /* send isDriver */
            if (this.isDriver) {
                connection.sendInt(DRIVER_EXECUTOR_ID); // executorId should be 'driver'
            } else {
                connection.sendInt(Integer.parseInt(executorId)); // executorId should be a decimal Integer
            }

            /* send padding */
            connection.sendInt(0);

            /* send application id */
            connection.sendStr(this.appId);
            connection.sendBytes(APP_ID_LEN - this.appId.length());
        }

        /* reply */
//        boolean ret = recvReply(broadcastIDmsb, broadcastIDlsb, -1, CODE_REPLY_INIT, MSG_HDRLEN + 4);
        waitReply(mt, obj);
        logger.info("[BOLD] init successfully");
        return true;
    }

    public void initialize() {
        logger.info("[BOLD] initialize() is called");
        if (!initialized) {
            logger.info("[BOLD] initialize");
            String socketFile = conf.get("spark.boldBroadcast.socketFile");

            /* get the master IP address */
            String serverUri = conf.get("spark.boldBroadcast.uri");
            this.serverIP = serverUri.substring(serverUri.indexOf("://") + 3, serverUri.lastIndexOf(':'));

            /* get the uuid */
            this.broadcastDir = conf.get("spark.boldBroadcast.broadcastDir");
            int uuidIndex = this.broadcastDir.lastIndexOf("broadcast") + "broadcast".length() + 1;
            this.broadcastID = UUID.fromString(this.broadcastDir.substring(uuidIndex, broadcastDir.length() - 1));
            this.broadcastIDmsb = this.broadcastID.getMostSignificantBits();
            this.broadcastIDlsb = this.broadcastID.getLeastSignificantBits();

            logger.info("[BOLD] initialize is called: sf: " + socketFile + ", su: " + this.serverIP + ", eid: " + this.executorId +
                    ", bd:" + this.broadcastDir +
                    ", uuid: " + this.broadcastID + " (" + String.format("0x%016x", this.broadcastIDmsb) + ", " + String.format("0x%016x", this.broadcastIDlsb) + ")");

            /* create juds */
            UnixDomainSocketClient clientSocket = null;
            try {
                clientSocket = new UnixDomainSocketClient(socketFile, JUDS.SOCK_STREAM);
            } catch (IOException e) {
                e.printStackTrace();
            }
            logger.debug("calling SocketConnection");
            connection = new SocketConnection(clientSocket);
            initialized = true;
        } else {
            logger.warn("boldManager has been initialized");
        }
    }

    public void stop() throws IOException {
        logger.debug("boldManager.stop() is called");
        if (initialized) {
            logger.debug("boldManager close uds connection");
            initialized = false;
            connection.replyServer.interrupt();
            connection.is.close();
            connection.os.close();
            connection.uds.close();
            connection.replyServer.stop();
        } else {
            logger.warn("boldManager not initialized/stopped");
        }
    }

    public boolean hasInitialized() {
        return initialized;
    }

    private void processReply(InputStream is) throws IOException {
        byte[] readLong = new byte[8];
        byte[] readInt = new byte[4];
        int readByte;
        while (!initialized) {
            logger.debug("in processReply. initialized" + initialized);
        }
        while (initialized) {
            logger.info("[BOLD] [loop] start");

            // get msb
            readByte = is.read(readLong, 0, readLong.length);
            logger.info("[BOLD] [loop] got msb");
            assert readByte == 8;
            long msb = ByteBuffer.wrap(readLong).getLong();
            assert msb == broadcastIDmsb;

            // get lsb
            readByte = is.read(readLong, 0, readLong.length);
            logger.info("[BOLD] [loop] got lsb");
            assert readByte == 8;
            long lsb = ByteBuffer.wrap(readLong).getLong();
            assert lsb == broadcastIDlsb;

            // get data id
            readByte = is.read(readLong, 0, readLong.length);
            logger.info("[BOLD] [loop] got did");
            assert readByte == 8;
            long did = ByteBuffer.wrap(readLong).getLong();

            // get type
            readByte = is.read(readInt, 0, readInt.length);
            logger.info("[BOLD] [loop] got type");
            assert readByte == 4;
            int type = ByteBuffer.wrap(readInt).getInt();

            // check if the request is pending
            MsgTuple mt = new MsgTuple(did, type);
            assert pendingRequest.containsKey(mt);
            logger.info("[BOLD] got message " + mt.toString());

            // get message len
            readByte = is.read(readInt, 0, readInt.length);
            logger.info("[BOLD] [loop] got len");
            assert readByte == 4;
            int len = ByteBuffer.wrap(readInt).getInt();

            // get tv_sec
            readByte = is.read(readLong, 0, readLong.length);
            logger.info("[BOLD] [loop] got sec");
            assert readByte == 8;

            // get tv_usec
            readByte = is.read(readLong, 0, readLong.length);
            logger.info("[BOLD] [loop] got usec");
            assert readByte == 8;

            int code;
            switch (type) {
                case CODE_REP_WRT:
                    assert len == MSG_HDRLEN + 4;
                    logger.debug("[BOLD] got CODE_REPLY_WRITE");
                    readByte = is.read(readInt, 0, readInt.length);
                    assert readByte == 4;
                    code = ByteBuffer.wrap(readInt).getInt();
                    assert code == REPLY_SUCCESS;
                    logger.debug("[BOLD] CODE_REPLY_WRITE success");
                    break;
                case CODE_REP_READ:
                    assert len == MSG_HDRLEN + 4 + 4 + 8 * 3;
                    logger.debug("[BOLD] got CODE_REPLY_READ");
                    readByte = is.read(readInt, 0, readInt.length);
                    assert readByte == 4;
                    code = ByteBuffer.wrap(readInt).getInt();
                    readResults = code;

                    readByte = is.read(readInt, 0, readInt.length);
                    assert readByte == 4;

                    readByte = is.read(readLong, 0, readLong.length);
                    assert readByte == 8;
                    readStart = ByteBuffer.wrap(readLong).getLong();

                    readByte = is.read(readLong, 0, readLong.length);
                    assert readByte == 8;
                    readJump = ByteBuffer.wrap(readLong).getLong();

                    readByte = is.read(readLong, 0, readLong.length);
                    assert readByte == 8;
                    readEnd = ByteBuffer.wrap(readLong).getLong();
                    logger.debug("[BOLD] CODE_REPLY_READ success");
                    break;
                case CODE_REP_DLT:
                    assert len == MSG_HDRLEN + 4;
                    logger.debug("[BOLD] got CODE_REPLY_REMOVE");
                    readByte = is.read(readInt, 0, readInt.length);
                    assert readByte == 4;
                    code = ByteBuffer.wrap(readInt).getInt();
                    assert code == REPLY_SUCCESS;
                    logger.debug("[BOLD] CODE_REPLY_REMOVE success");
                    break;
                case CODE_REP_INT:
                    assert len == MSG_HDRLEN + 4;
                    logger.debug("[BOLD] got CODE_REPLY_INIT");
                    readByte = is.read(readInt, 0, readInt.length);
                    assert readByte == 4;
                    code = ByteBuffer.wrap(readInt).getInt();
                    assert code == REPLY_SUCCESS;
                    logger.debug("[BOLD] CODE_REPLY_INIT success");
                    break;
                case CODE_REP_PSH:
                    assert len == MSG_HDRLEN + 4;
                    logger.debug("[BOLD] got CODE_REPLY_PUSH");
                    readByte = is.read(readInt, 0, readInt.length);
                    assert readByte == 4;
                    code = ByteBuffer.wrap(readInt).getInt();
                    assert code == REPLY_SUCCESS;
                    logger.debug("[BOLD] CODE_REPLY_PUSH success");
                    break;
                default:
                    logger.error("[BOLD] got wrong type");
                    break;
            }
            Object obj = pendingRequest.remove(mt);
            logger.debug("lock " + obj.toString());
            synchronized (connection) {
                synchronized (obj) {
                    logger.debug("notify the waiter");
                    obj.notify();
                }
            }
        }
        logger.info("[BOLD] processReply terminated");
    }

    private class SocketConnection {
        private InputStream is;
        private OutputStream os;
        private UnixDomainSocket uds;
        private Thread replyServer;

        SocketConnection(UnixDomainSocket pUnixDomainSocket) {
            try {
                uds = pUnixDomainSocket;
                is = pUnixDomainSocket.getInputStream();
                os = pUnixDomainSocket.getOutputStream();
            } catch (Exception e) {
                e.printStackTrace();
            }
            replyServer = new Thread() {
                public void run() {
                    try {
                        processReply(is);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            };
            logger.debug("calling replyServer.start()");
            replyServer.start();
        }

        void sendBytes(int num) {
            try {
                byte[] bytes = ByteBuffer.allocate(num).array(); //.putChar('\0').array();
                os.write(bytes);
                logger.debug("sent: " + num + "(" + num + " bytes)");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }


        void sendInt(int i) {
            try {
                byte[] bytes = ByteBuffer.allocate(4).putInt(i).array();
                os.write(bytes);
                logger.debug("sent: " + i + "(4 bytes)");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        void sendLong(long l) {
            try {
                byte[] bytes = ByteBuffer.allocate(8).putLong(l).array();
                os.write(bytes);
                logger.debug("sent: " + l + "(8 bytes)");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        void sendStr(String str) {
            try {
                logger.debug("[BOLD] str: " + str + ", length: " + str.getBytes("UTF8").length);
                os.write(str.getBytes("UTF8"));
                logger.debug("sent: " + str + "(" + str.getBytes("UTF8").length + " bytes)");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    private class MsgTuple {
        private long did;
        private int type;

        MsgTuple(long did, int type) {
            this.did = did;
            this.type = type;
        }

        MsgTuple(int type) {
            this(-1, type);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            MsgTuple other = (MsgTuple) obj;
            return (did == other.did) && (type == other.type);
        }

        @Override
        public int hashCode() {
            return (int) did * 100 + type;
        }

        @Override
        public String toString() {
            return "did: " + did + "; type: " + type;
        }
    }

}
