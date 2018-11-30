package org.apache.spark.bold.network.broadcast.standalone;

import org.apache.commons.cli.*;
import org.apache.spark.bold.io.FileChunkInputStream;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BroadcastBlockId;

import java.io.*;
import java.util.*;

/**
 * Created by xs6 on 1/10/17.
 */
public class BoldReceiver extends ABoldManager implements IBoldReceiver {
    public BoldReceiver() {
        super(false);
    }

    public static void main(String[] args) throws IOException {
        Options options = new Options();

        Option sfO = new Option("s", "socket-filename", true, "juds socket filename");
        sfO.setRequired(true);
        options.addOption(sfO);

        Option aidO = new Option("a", "application-id", true, "application id");
        aidO.setRequired(true);
        options.addOption(aidO);

        Option midO = new Option("m", "machine-id", true, "machine id");
        midO.setRequired(true);
        options.addOption(midO);

        Option bdO = new Option("b", "bcd-directory", true, "broadcast directory");
        bdO.setRequired(true);
        options.addOption(bdO);

        Option dipO = new Option("d", "driver-ip", true, "driver ip");
        dipO.setRequired(true);
        options.addOption(dipO);

        CommandLineParser parser = new BasicParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);
            System.exit(1);
            return;
        }

        String sf = cmd.getOptionValue('s');
        String su = cmd.getOptionValue('d');
        String bd = cmd.getOptionValue('b');
        String eid = cmd.getOptionValue('m');
        String aid = cmd.getOptionValue('a');

        System.out.println("socket file:\t" + sf);
        System.out.println("server ip:\t\t" + su);
        System.out.println("bcd directory:\t" + bd);
        System.out.println("machine id:\t\t" + eid);
        System.out.println("application id:\t" + aid);

        Scanner input = new Scanner(System.in);
        String agentCmd;
        BoldReceiver br = new BoldReceiver();
        br.initialize(sf, su, bd);
        br.init(aid, eid);

        long did;
        while (true) {
            System.out.println("==========================================================================");
            System.out.print("input command:\t");

            agentCmd = input.nextLine();

            long a = System.nanoTime();
            if (agentCmd.equals("unreg")) {
                br.stop();
                break;
            } else {
                String[] splited = agentCmd.split("\\s+");
                assert splited.length >= 2;
                String ac = splited[0];

                switch (ac) {
                    // shared methods
                    case "delete":
                        did = Long.parseLong(splited[1]);
                        System.out.println("del id:[" + did + "]");
                        br.remove(did);
                        break;

                    // receiver methods
                    case "pull":
                        did = Long.parseLong(splited[1]);
                        System.out.println("pul id:[" + did + "]");
                        br.read(did, 5);
                        break;

                    // sender methods
                    case "push":
                        did = Long.parseLong(splited[1]);
                        List<String> pushList = new ArrayList<>();
                        for (int j = 2; j < splited.length; j++) {
                            pushList.add(splited[j]);
                        }
                        System.out.print("pus id:[" + did + "] to [");
                        for (String l : pushList) {
                            System.out.print(l + ", ");
                        }
                        System.out.println("]");
                    case "add":
                        did = Long.parseLong(splited[1]);
                        System.out.println("add id:[" + did + "]");

                    default:
                        System.out.println("cmd: " + cmd + " is not supported");
                        break;
                }
            }
            long b = System.nanoTime();
            System.out.println("Done. took " + (b - a) / 1000.0 / 1000.0 + " ms");
        }
    }

    @Override
    public InputStream read(long id, long interval) {
        logger.info("[bold] sending read, id[" + id + "]");

        BlockId bid = new BroadcastBlockId(id, "");
//        String uri = serverUri + ":" + broadcastDir + bid.name();
        String filename = broadcastDir + bid.name();
//        logger.debug("uri:" + uri + ", filename: " + filename);

        MsgTuple mt = new MsgTuple(id, CODE_REPLY_READ);
        Object obj = new Object(); // TODO: this could the reply object

        /* send MsgHdr */
        synchronized (connection) {
            assert !pendingRequest.containsKey(mt);
            pendingRequest.put(mt, obj);
            sendMsgHdr(broadcastIDmsb, broadcastIDlsb, id, CODE_REQUEST_READ, MSG_HDRLEN);
        }
//        logger.info("[BOLD] data read sent, uri: " + uri + ", filename: " + filename);

        /* reply */
//        boolean ret = recvReply(broadcastIDmsb, broadcastIDlsb, id, CODE_REPLY_READ, MSG_HDRLEN + 4 + 4 * 3);
        waitReply(mt, obj);
        logger.info("[BOLD] read successfully");

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
                fcis = new FileChunkInputStream(new File(filename), 0, readStart, interval);
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
    }
}
