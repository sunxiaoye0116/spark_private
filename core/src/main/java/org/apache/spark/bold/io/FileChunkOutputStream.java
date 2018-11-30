package org.apache.spark.bold.io;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Created by xs6 on 9/28/16.
 */
public class FileChunkOutputStream extends FileOutputStream {


    public FileChunkOutputStream(String name) throws FileNotFoundException {
        super(name);
    }

    @Override
    public void write(byte b[], int off, int len) throws IOException {
        try {
            Thread.sleep(10000);
            System.out.print(".");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        super.write(b, off, len);
    }

}
