package org.apache.spark.bold.io;

import java.io.*;

/**
 * Created by xs6 on 9/28/16.
 */
public class ReadWriteFile {
    private final RandomAccessFile _raf;
    private final long _size;
    private final OutputStream _writer;
    private long _nw;

    ReadWriteFile(File f, long size) throws IOException {
        _raf = new RandomAccessFile(f, "rw");
        _size = size;

        _writer = new OutputStream() {

            @Override
            public void write(int b) throws IOException {
                write(new byte[]{
                        (byte) b
                });
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                if (len < 0)
                    throw new IllegalArgumentException();
                synchronized (_raf) {
                    _raf.seek(_nw);
                    _raf.write(b, off, len);
                    _nw += len;
                    _raf.notify();
                }
            }
        };
    }

    public static void main(String[] args) throws Exception {
        File f = new File("test.bin");
        final long size = 1024;
        final ReadWriteFile rwf = new ReadWriteFile(f, size);

        Thread t1 = new Thread("Writer") {
            public void run() {
                try {
                    OutputStream w = new BufferedOutputStream(rwf.writer(), 16);
                    for (int i = 0; i < size; i++) {
                        w.write(i);
                        sleep(1);
                    }
                    System.out.println("Write done");
                    w.close();
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        };

        Thread t2 = new Thread("Reader") {
            public void run() {
                try {
                    InputStream r = new BufferedInputStream(rwf.reader(), 13);
                    for (int i = 0; i < size; i++) {
                        int b = r.read();
                        assert (b == (i & 255));
                    }
                    int eof = r.read();
                    assert (eof == -1);
                    r.close();
                    System.out.println("Read done");
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        };

        t1.start();
        t2.start();
        t1.join();
        t2.join();
        rwf.close();
    }

    void close() throws IOException {
        _raf.close();
    }

    InputStream reader() {
        return new InputStream() {
            private long _pos;

            @Override
            public int read() throws IOException {
                if (_pos >= _size)
                    return -1;
                byte[] b = new byte[1];
                if (read(b, 0, 1) != 1)
                    throw new IOException();
                return b[0] & 255;
            }

            @Override
            public int read(byte[] buff, int off, int len) throws IOException {
                synchronized (_raf) {
                    while (true) {
                        if (_pos >= _size)
                            return -1;
                        if (_pos >= _nw) {
                            try {
                                _raf.wait();
                                continue;
                            } catch (InterruptedException ex) {
                                throw new IOException(ex);
                            }
                        }
                        _raf.seek(_pos);
                        len = (int) Math.min(len, _nw - _pos);
                        int nr = _raf.read(buff, off, len);
                        _pos += Math.max(0, nr);
                        return nr;
                    }
                }
            }
        };
    }

    OutputStream writer() {
        return _writer;
    }
}