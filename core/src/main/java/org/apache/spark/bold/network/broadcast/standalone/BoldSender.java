package org.apache.spark.bold.network.broadcast.standalone;

import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.util.*;

/**
 * Created by xs6 on 1/10/17.
 */
public class BoldSender extends ABoldManager implements IBoldSender {

    public BoldSender() {
        super(true);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
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

        Option esO = new Option("e", "experiments", true, "experimental sender");
        dipO.setRequired(true);
        options.addOption(esO);

        Option ldO = new Option("x", "datas", true, "list of data");
        ldO.setRequired(false);
        options.addOption(ldO);

        Option lslO = new Option("y", "slaves", true, "list of slave lists");
        lslO.setRequired(false);
        options.addOption(lslO);

        Option itO = new Option("i", "interval", true, "interval bewteen actions in seconds");
        itO.setRequired(false);
        options.addOption(itO);

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
        boolean es = Boolean.parseBoolean(cmd.getOptionValue('e'));

        System.out.println("socket file:\t" + sf);
        System.out.println("server ip:\t\t" + su);
        System.out.println("bcd directory:\t" + bd);
        System.out.println("machine id:\t\t" + eid);
        System.out.println("application id:\t" + aid);
        System.out.println("experiment:\t\t" + es);

        BoldSender bs = new BoldSender();
        bs.initialize(sf, su, bd);
        bs.init(aid, eid);
        long did = -1;
        Scanner input = new Scanner(System.in);
        String agentCmd;

        if (!es) {
            while (true) {
                System.out.println("==========================================================================");
                System.out.print("input command:\t");
                agentCmd = input.nextLine();

                long a = System.nanoTime();
                if (agentCmd.equals("unreg")) {
                    bs.stop();
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
                            bs.remove(did);
                            break;

                        // sender methods
                        case "push":
                            did = Long.parseLong(splited[1]);
                            HashSet<String> pushSet = new HashSet<>();
                            for (int j = 2; j < splited.length; j++) {
                                pushSet.add(splited[j]);
                            }
                            System.out.print("pus id:[" + did + "] to [");
                            for (String l : pushSet) {
                                System.out.print(l + ", ");
                            }
                            System.out.println("]");
                            bs.push(did, pushSet);
                            break;
                        case "add":
                            did = Long.parseLong(splited[1]);
                            long size = Long.parseLong(splited[2]);
                            System.out.println("add id:[" + did + "] with size:[" + size + "]");
                            FileUtils.writeByteArrayToFile(new File(bd + "./broadcast_" + did), new byte[(int) size]);
                            bs.write(did, size);
                            break;

                        // receiver methods
                        case "pull":
                            did = Long.parseLong(splited[1]);
//                            System.out.println("pul id:[" + did + "]");
                        default:
                            System.out.println("cmd: " + cmd + " is not supported");
                            break;
                    }
                }
                long b = System.nanoTime();
                System.out.println("Done. took " + (b - a) / 1000.0 / 1000.0 + " ms");
            }
        } else {
            do {
                System.out.println("type [start] to start: ");
            } while (input.nextLine().equals("start\n"));

            String dl = cmd.getOptionValue('x');
            String sl = cmd.getOptionValue('y');
            int interval = Integer.parseInt(cmd.getOptionValue('i'));
            System.out.println("data list filename:\t" + dl);
            System.out.println("slave list filename:\t" + sl);
            System.out.println("interval seconds:\t" + interval);

            BufferedReader brdata = new BufferedReader(new InputStreamReader(new FileInputStream(dl)));
            ArrayList<Long> dataList = new ArrayList<Long>();
            String data;
            while ((data = brdata.readLine()) != null) {
                dataList.add(Long.valueOf(data));
            }

            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(sl)));
            ArrayList<HashSet<String>> slavesList = new ArrayList<HashSet<String>>();
            String slave;
            while ((slave = br.readLine()) != null) {
                String[] slaves = slave.split("\\s+");
                HashSet<String> slavesSet = new HashSet<String>(Arrays.asList(slaves));
                slavesList.add(slavesSet);
            }

            assert slavesList.size() == dataList.size();

            for (int i = 0; i < dataList.size(); i++) {
                if (did >= 0) {
                    bs.remove(did);
                }

                System.out.println("==================== " + i + "/" + dataList.size() + " ====================");
                did = (long) i;

                long a = System.nanoTime();
                FileUtils.writeByteArrayToFile(new File(bd + "./broadcast_" + did), new byte[dataList.get(i).intValue()]);
                System.out.println("write did:[" + did + "], size:[" + dataList.get(i) + "]");
                bs.write(did, dataList.get(i));
                long b = System.nanoTime();
                System.out.println("Write took " + (b - a) / 1000.0 / 1000.0 + " ms");

                Thread.sleep(interval * 1000);

                long c = System.nanoTime();
                System.out.println("push did:[" + did + "], to:" + slavesList.get(i));
                bs.push(did, slavesList.get(i));
                long d = System.nanoTime();
                System.out.println("Push took " + (d - c) / 1000.0 / 1000.0 + " ms");
                Thread.sleep(interval * 1000);
            }
            bs.stop();
        }
    }


    @Override
    public boolean write(long id, long size) {
//        logger.info("[bold] write is called. dir: " + broadcastDir + ", id: " + id + ", eid: " + executorId);
//        BlockId bid = new BroadcastBlockId(id, "");
//        String filename = broadcastDir + bid.name();
//        logger.debug("filename: " + filename + "size: " + size);
        logger.info("[bold] sending write, id[" + id + "]" + " size[" + size + "]");

        MsgTuple mt = new MsgTuple(id, CODE_REPLY_WRITE);
        Object obj = new Object();

        /* send MsgHdr */
        synchronized (connection) {
            assert !pendingRequest.containsKey(mt);
            pendingRequest.put(mt, obj);
            sendMsgHdr(broadcastIDmsb, broadcastIDlsb, id, CODE_REQUEST_WRITE, MSG_HDRLEN + 8);
            connection.sendLong(size);
        }

        /* reply */
//        boolean ret = recvReply(broadcastIDmsb, broadcastIDlsb, id, CODE_REPLY_WRITE, MSG_HDRLEN + 4);
        waitReply(mt, obj);
        logger.info("[BOLD] write successfully");
        return true;
    }

    @Override
    public boolean push(long id, Set<String> executorIPs) {
        logger.info("[bold] sending push, id[" + id + "]");

        MsgTuple mt = new MsgTuple(id, CODE_REPLY_PUSH);
        Object obj = new Object();

        /* send MsgHdr */
        synchronized (connection) {
            assert !pendingRequest.containsKey(mt);
            pendingRequest.put(mt, obj);
            sendMsgHdr(broadcastIDmsb, broadcastIDlsb, id, CODE_REQUEST_PUSH, MSG_HDRLEN + 8 + IP_ADDR_LEN * executorIPs.size());

            /* send number of receivers */
            logger.debug("push list size: " + executorIPs.size());
            connection.sendLong(executorIPs.size());

            /* send slave addresses */
            for (String s : executorIPs) {
                connection.sendStr(s);
                connection.sendBytes(IP_ADDR_LEN - s.length());
            }
        }

        /* reply */
//        boolean ret = recvReply(broadcastIDmsb, broadcastIDlsb, id, CODE_REPLY_PUSH, MSG_HDRLEN + 4);
        waitReply(mt, obj);
        logger.info("[BOLD] push successfully");
        return true;
    }
}
