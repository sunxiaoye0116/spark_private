package org.apache.spark.bold.network.broadcast.standalone;

import com.etsy.net.JUDS;
import com.etsy.net.UnixDomainSocket;
import com.etsy.net.UnixDomainSocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by xs6 on 1/10/17.
 */
public abstract class ABoldManager {
    final Logger logger = LoggerFactory.getLogger(ABoldManager.class);
    final int CODE_REQUEST_WRITE = 89;
    final int CODE_REPLY_WRITE = 90;
    final int CODE_REQUEST_READ = 91;
    final int CODE_REPLY_READ = 92;
    final int CODE_REQUEST_PUSH = 97;
    final int CODE_REPLY_PUSH = 98;
    final int MSG_HDRLEN = 32 + 16;
    final int IP_ADDR_LEN = 16;
    final ConcurrentHashMap<ABoldManager.MsgTuple, Object> pendingRequest = new ConcurrentHashMap<>();
    private final int CODE_REQUEST_REMOVE = 93;
    private final int CODE_REPLY_REMOVE = 94;
    private final int CODE_REQUEST_INIT = 95;
    private final int CODE_REPLY_INIT = 96;
    private final int APP_ID_LEN = 24;
    private final int DRIVER_EXECUTOR_ID = -1;
    private final int REPLY_SUCCESS = 50;
    private final int REPLY_FAIL = 51;
    long broadcastIDmsb; // TODO: msb and lsb in Msg seems to be redundant
    long broadcastIDlsb;
    ABoldManager.SocketConnection connection;
    int readStart;
    int readJump;
    int readEnd;
    String broadcastDir; // absolute path of the broadcast data dir for the application
    private boolean isDriver;
    private boolean initialized = false;
    private String serverUri; // pure IP address string of the master
    private UUID broadcastID;
    private String executorId;
    private String appId;
    private String boldId;

    ABoldManager(boolean isDriver) {
        this.isDriver = isDriver;
    }

    public boolean remove(long id) {
        logger.info("[bold] sending remove, id[" + id + "]");

        MsgTuple mt = new MsgTuple(id, CODE_REPLY_REMOVE);
        Object obj = new Object();

        /* send MsgHdr */
        synchronized (connection) {
            assert !pendingRequest.containsKey(mt);
            pendingRequest.put(mt, obj);
            sendMsgHdr(broadcastIDmsb, broadcastIDlsb, id, CODE_REQUEST_REMOVE, MSG_HDRLEN);
        }

        /* reply */
//        boolean ret = recvReply(broadcastIDmsb, broadcastIDlsb, id, CODE_REPLY_REMOVE, MSG_HDRLEN + 4);
        waitReply(mt, obj);
        logger.info("[BOLD] remove successfully");
        return true;
    }

    public boolean init(String appId, String executorId) {
        this.appId = appId;
        this.executorId = executorId;
        this.boldId = this.appId + "/" + this.executorId;
        logger.info("[bold] sending init, boldId[" + this.boldId + "]");

        MsgTuple mt = new MsgTuple(CODE_REPLY_INIT);
        Object obj = new Object();

        /* send MsgHdr */
        synchronized (connection) {
            assert !pendingRequest.containsKey(mt);
            pendingRequest.put(mt, obj);
            sendMsgHdr(broadcastIDmsb, broadcastIDlsb, -1, CODE_REQUEST_INIT, MSG_HDRLEN + IP_ADDR_LEN + 4 + 4 + APP_ID_LEN);

            /* send master ip */
            connection.sendStr(this.serverUri);
            connection.sendBytes(IP_ADDR_LEN - this.serverUri.length());

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

    public void initialize(String sf, String m, String bd) {
        logger.info("ABoldManager.initialize() is called");
        if (!initialized) {
            logger.info("initialize ABoldManager");
            String socketFile = sf;

            /* get the master IP address */
            this.serverUri = m;

            /* get the uuid */
            this.broadcastDir = bd;
            int uuidIndex = this.broadcastDir.lastIndexOf("broadcast") + "broadcast".length() + 1;
            this.broadcastID = UUID.fromString(this.broadcastDir.substring(uuidIndex, broadcastDir.length() - 1));
            this.broadcastIDmsb = this.broadcastID.getMostSignificantBits();
            this.broadcastIDlsb = this.broadcastID.getLeastSignificantBits();

            logger.info("initialize is called: sf: " + socketFile + ", su: " + this.serverUri + ", eid: " + this.executorId +
                    ", bd:" + this.broadcastDir +
                    ", uuid: " + this.broadcastID + " (" + String.format("0x%016x", this.broadcastIDmsb) + ", " + String.format("0x%016x", this.broadcastIDlsb) + ")");

            /* create juds */
            UnixDomainSocketClient clientSocket = null;
            try {
                clientSocket = new UnixDomainSocketClient(socketFile, JUDS.SOCK_STREAM);
            } catch (IOException e) {
                e.printStackTrace();
            }
            connection = new SocketConnection(clientSocket);
            initialized = true;
        } else {
            logger.warn("ABoldManager has been initialized");
        }
    }

    public boolean hasInitialized() {
        return initialized;
    }

    void processReply(InputStream is) throws IOException {
        byte[] readLong = new byte[8];
        byte[] readInt = new byte[4];
        int readByte;

        while (initialized) {
            // get msb
            readByte = is.read(readLong, 0, readLong.length);
            assert readByte == 8;
            long msb = ByteBuffer.wrap(readLong).getLong();
            assert msb == broadcastIDmsb;

            // get lsb
            readByte = is.read(readLong, 0, readLong.length);
            assert readByte == 8;
            long lsb = ByteBuffer.wrap(readLong).getLong();
            assert lsb == broadcastIDlsb;

            // get data id
            readByte = is.read(readLong, 0, readLong.length);
            assert readByte == 8;
            long did = ByteBuffer.wrap(readLong).getLong();

            // get type
            readByte = is.read(readInt, 0, readInt.length);
            assert readByte == 4;
            int type = ByteBuffer.wrap(readInt).getInt();

            // check if the request is pending
            MsgTuple mt = new MsgTuple(did, type);
            assert pendingRequest.containsKey(mt);
            logger.info("[BOLD] got message " + mt.toString());

            // get message len
            readByte = is.read(readInt, 0, readInt.length);
            assert readByte == 4;
            int len = ByteBuffer.wrap(readInt).getInt();

            // get tv_sec
            readByte = is.read(readLong, 0, readLong.length);
            assert readByte == 8;

            // get tv_usec
            readByte = is.read(readLong, 0, readLong.length);
            assert readByte == 8;

            int code;
            switch (type) {
                case CODE_REPLY_WRITE:
                    assert len == MSG_HDRLEN + 4;
                    logger.debug("[BOLD] got CODE_REPLY_WRITE");
                    readByte = is.read(readInt, 0, readInt.length);
                    assert readByte == 4;
                    code = ByteBuffer.wrap(readInt).getInt();
                    assert code == REPLY_SUCCESS;
                    logger.debug("[BOLD] CODE_REPLY_WRITE success");
                    break;
                case CODE_REPLY_READ:
                    assert len == MSG_HDRLEN + 4 + 4 * 3;
                    logger.debug("[BOLD] got CODE_REPLY_READ");
                    readByte = is.read(readInt, 0, readInt.length);
                    assert readByte == 4;
                    code = ByteBuffer.wrap(readInt).getInt();
                    assert code == REPLY_SUCCESS;

                    readByte = is.read(readInt, 0, readInt.length);
                    assert readByte == 4;
                    readStart = ByteBuffer.wrap(readInt).getInt();

                    readByte = is.read(readInt, 0, readInt.length);
                    assert readByte == 4;
                    readJump = ByteBuffer.wrap(readInt).getInt();

                    readByte = is.read(readInt, 0, readInt.length);
                    assert readByte == 4;
                    readEnd = ByteBuffer.wrap(readInt).getInt();
                    logger.debug("[BOLD] CODE_REPLY_READ success");
                    break;
                case CODE_REPLY_REMOVE:
                    assert len == MSG_HDRLEN + 4;
                    logger.debug("[BOLD] got CODE_REPLY_REMOVE");
                    readByte = is.read(readInt, 0, readInt.length);
                    assert readByte == 4;
                    code = ByteBuffer.wrap(readInt).getInt();
                    assert code == REPLY_SUCCESS;
                    logger.debug("[BOLD] CODE_REPLY_REMOVE success");
                    break;
                case CODE_REPLY_INIT:
                    assert len == MSG_HDRLEN + 4;
                    logger.debug("[BOLD] got CODE_REPLY_INIT");
                    readByte = is.read(readInt, 0, readInt.length);
                    assert readByte == 4;
                    code = ByteBuffer.wrap(readInt).getInt();
                    assert code == REPLY_SUCCESS;
                    logger.debug("[BOLD] CODE_REPLY_INIT success");
                    break;
                case CODE_REPLY_PUSH:
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

    void waitReply(MsgTuple mt, Object obj) {
        synchronized (obj) {
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

    void sendMsgHdr(long msb, long lsb, long id, int type, int len) {
        connection.sendLong(msb);
        connection.sendLong(lsb);
        connection.sendLong(id);
        connection.sendInt(type);
        connection.sendInt(len);
        long time_us = System.nanoTime() / 1000;
        connection.sendLong(time_us / (1000 * 1000));
        connection.sendLong(time_us % (1000 * 1000));
    }

    protected class SocketConnection {
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

    protected class MsgTuple {
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
            ABoldManager.MsgTuple other = (ABoldManager.MsgTuple) obj;
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
