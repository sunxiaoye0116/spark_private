package org.apache.spark.bold.io;

import org.apache.log4j.BasicConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.DeserializationStream;
import org.apache.spark.serializer.JavaSerializer;
import org.apache.spark.serializer.SerializerInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Array;

import java.io.*;
import java.util.Enumeration;
import java.util.Vector;


/**
 * Created by xs6 on 6/28/16.
 */
public class FileChunkInputStream extends FileInputStream {
    private final Logger logger = LoggerFactory.getLogger(FileChunkInputStream.class);
    private final long start;
    private final long end;
    private long pointer;
    private long interval;

    public FileChunkInputStream(File file, long start, long end, long interval) throws FileNotFoundException {
        super(file);
        logger.debug("constructor is called");
        this.start = start;
        this.end = end;
        this.pointer = start;
        try {
            skip(this.start);
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.interval = interval;
    }

    public FileChunkInputStream(String name) throws FileNotFoundException {
        super(name);
        logger.error("not implemented constructor is called");
        this.start = 0;
        this.end = 0;
    }

    public FileChunkInputStream(File file) throws FileNotFoundException {
        super(file);
        logger.error("not implemented constructor is called");
        this.start = 0;
        this.end = 0;
    }

    public FileChunkInputStream(FileDescriptor fdObj) {
        super(fdObj);
        logger.error("not implemented constructor is called");
        this.start = 0;
        this.end = 0;
    }

    public static void main(String args[]) throws IOException, ClassNotFoundException {
        BasicConfigurator.configure();

        int bufferSize = 65536;
        String filename = "/Users/username/github/object";
        File file = new File(filename);
        System.out.println("file length " + file.length());

        /*

         */
        FileChunkInputStream fcis1 = new FileChunkInputStream(new File(filename), 300000027 - 10000000, 300000027, 5);
        FileChunkInputStream fcis2 = new FileChunkInputStream(new File(filename), 0, 300000027 - 10000000, 5);
        FileChunkInputStream fcis3 = new FileChunkInputStream(new File(filename), 300000027, 400000027, 5);

        Vector<InputStream> inputStreams = new Vector<InputStream>();
        inputStreams.add(fcis1);
        inputStreams.add(fcis2);
        inputStreams.add(fcis3);

        Enumeration<InputStream> enu = inputStreams.elements();
        SequenceInputStream sis = new SequenceInputStream(enu);
        
        /*

         */
        InputStream bis = new BufferedInputStream(sis, bufferSize);
        JavaSerializer javaser = new JavaSerializer(new SparkConf(true));
        SerializerInstance ser = javaser.newInstance();
        DeserializationStream serIn = ser.deserializeStream(bis);

        System.out.println("start");
        long t1 = System.currentTimeMillis();
        Object obj = serIn.readObject(scala.reflect.ClassTag$.MODULE$.apply(Array.class));
        long t2 = System.currentTimeMillis();
        System.out.println("done: " + (t2 - t1) + "ms");
        serIn.close();
        System.out.println(obj.getClass());
    }


    @Override
    public int read() throws IOException {
        logger.debug("read() is called");
        int b = 0;
        if (pointer < end) {
            try {
                b = super.read();
            } finally {
                pointer += (b == -1 ? 0 : 1);
            }
        } else {
            b = -1;
        }
        return b;
    }

    @Override
    public int read(byte b[]) throws IOException {
        logger.debug("read(byte[]) " + b.length + " is called");
        int bytesRead = 0;
        long bytesReadMax = Math.max(end - pointer, 0);
        if (pointer < end) {
            try {
                bytesRead = super.read(b);
            } finally {
                pointer += (bytesRead == -1 ? 0 : bytesRead);
            }
        } else {
            bytesRead = -1;
        }
        return (int) Math.min(bytesRead, bytesReadMax);
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        int bytesRead = 0;
        long bytesReadMax = Math.max(end - pointer, 0);
        if (pointer < end) {
            while (super.available() <= 0) {
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            try {
                bytesRead = super.read(b, off, len);
            } finally {
                pointer += (bytesRead == -1 ? 0 : bytesRead);
            }
        } else {
            bytesRead = -1;
        }
        return (int) Math.min(bytesRead, bytesReadMax);
    }

    @Override
    public void reset() throws IOException {
        logger.debug("reset() is called");
        super.reset();
        super.skip(0);
        super.skip(start);
    }
}
