package org.apache.spark.bold.network.broadcast.standalone;

import java.io.InputStream;

/**
 * Created by xs6 on 1/10/17.
 */
public interface IBoldReceiver {
    public InputStream read(long id, long interval);
}
