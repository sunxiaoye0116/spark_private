package org.apache.spark.bold.network.broadcast.standalone;

import java.util.Set;

/**
 * Created by xs6 on 1/10/17.
 */
public interface IBoldSender {
    public boolean write(long id, long size);

    public boolean push(long id, Set<String> executorIPs);
}
