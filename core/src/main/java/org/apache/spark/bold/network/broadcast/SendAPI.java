package org.apache.spark.bold.network.broadcast;

/**
 * Created by xs6 on 5/11/16.
 */

import com.etsy.net.JUDS;
import com.etsy.net.UnixDomainSocket;
import com.etsy.net.UnixDomainSocketClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;

public class SendAPI {
    private final Logger logger = LoggerFactory.getLogger(SendAPI.class);

    private class SocketConnection {
        private String description;
        private InputStream is;
        private OutputStream os;

        public SocketConnection(String pDescription, UnixDomainSocket pUnixDomainSocket) {
            try {
                description = pDescription;
                is = pUnixDomainSocket.getInputStream();
                os = pUnixDomainSocket.getOutputStream();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void expectStr(String pSentence) {
            try {
                byte[] expected = new byte[pSentence.getBytes("UTF8").length];
                int read = is.read(expected);
                if (read == expected.length) {
                    if (new String(expected, "UTF8").equals(pSentence)) {
                        System.out.println("" + description + " received :" + pSentence);
                        return;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        public void sendStr(String pSentence) {
            try {
                os.write(pSentence.getBytes("UTF8"));
                System.out.println("" + description + " sent :" + pSentence);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void sendNum(int num) {
            try {
                os.write(num);
                System.out.println("" + description + " sent :" + num);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public String recvStr() {
            try {
                byte[] readIn = new byte[128];
                int readCount = is.read(readIn);
                //System.out.println(new String(readIn, "UTF8"));
                return new String(readIn, 0, readCount, "UTF8");
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }

        public long recvLong() {
            try {
                byte[] readIn = new byte[8];
                int readCount = is.read(readIn, 0, 8);
                //System.out.println("file size = " + ByteBuffer.wrap(readIn).getInt());
                return ByteBuffer.wrap(readIn).getLong();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return -1;
        }

    }

    public void send(Object originalData, int id) {
        logger.debug("[BOLD] start serialize");
        byte[] serialData = serialize(originalData);
        logger.debug("[BOLD] finish serialize");
        String ramPath = "/mnt/ramdisk/";
        String ramFileName = ramPath + "data_" + id;
//        String socketFileName = "/home/yx15/local_socket";
        //long fileSize = (long)serialData.length;

//        try {
//            MessageDigest m = MessageDigest.getInstance("MD5");
//            m.update(serialData, 0, serialData.length);
//            System.out.println("MD5: " + new BigInteger(1, m.digest()).toString(16));
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

        //createMMap(mmapFileName, fileSize, serialData);
        //sendMMap(socketFileName, mmapFileName, fileSize);
        logger.debug("[BOLD] start writeRAM");
        writeRAM(ramFileName, serialData);
        logger.debug("[BOLD] finish writeRAM");
        //sendRAM(socketFileName, id);
    }

    public void writeRAM(String fileName, byte[] data) {
        OutputStream outputStream = null;

        try {
            outputStream = new FileOutputStream(new File(fileName));
            outputStream.write(data, 0, data.length);
            System.out.println("@@@@@@@@@@ finish writing " + data.length + " Bytes of data");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (outputStream != null) {
                try {
                    outputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void sendRAM(String socketFileName, int id) {
        try {
            UnixDomainSocketClient clientSocket = new UnixDomainSocketClient(socketFileName, JUDS.SOCK_STREAM);
            SocketConnection connection = new SocketConnection("Client -> Server on " + socketFileName, clientSocket);
            connection.sendNum(id);

            System.out.println("Done");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


        /*public void createMMap (String mmapFileName, long fileSize, byte[] data) {
            try {
                RandomAccessFile memoryMappedFile = new RandomAccessFile(mmapFileName, "rw");

                //Mapping a file into memory

                MappedByteBuffer out = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, fileSize);

                //Writing into Memory Mapped File
                out.put(data);

                System.out.println("Writing to Memory Mapped File is completed");
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }*/

        /*public void sendMMap (String socketFileName, String mmapFileName, long fileSize) {
            try {
                UnixDomainSocketClient clientSocket = new UnixDomainSocketClient(socketFileName, JUDS.SOCK_STREAM);
                SocketConnection connection = new SocketConnection("Client -> Server on " + socketFileName, clientSocket);
                connection.sendStr(mmapFileName);
                connection.sendStr(""+fileSize);

                System.out.println("Done");
            }
            catch (Exception e) {
                    e.printStackTrace();
            }
        }*/

    public static byte[] serialize(Object obj) {
        byte[] dataByte = null;

        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(bos);
            out.writeObject(obj);
            out.close();
            dataByte = bos.toByteArray();

            return dataByte;

        } catch (IOException e) {
            e.printStackTrace();
        }

        return dataByte;
    }

    public static Object deserialize(byte[] data) {
        Object obj = null;

        try {
            ByteArrayInputStream in = new ByteArrayInputStream(data);
            ObjectInputStream is = new ObjectInputStream(in);
            obj = is.readObject();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        return obj;
    }

        /*public static void main (String[] args) {
                String toSend = "abcdefghijklmnopqrstuvwxyz";
                SendAPI sendInter = new SendAPI();
                sendInter.send(toSend, 1);
        }*/

}
