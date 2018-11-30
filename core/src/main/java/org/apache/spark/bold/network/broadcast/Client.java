package org.apache.spark.bold.network.broadcast;

/**
 * Created by xs6 on 5/11/16.
 */

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Client extends Thread {
    public void run() {

        List<String> destAddr1 = new ArrayList<String>();
        destAddr1.add("10.11.11.2");
        destAddr1.add("10.11.11.3");
        destAddr1.add("10.11.11.13");
        destAddr1.add("10.11.11.14");
        destAddr1.add("10.11.11.25");
        destAddr1.add("10.11.11.26");
        destAddr1.add("10.11.11.37");

        try {
            CommunicationService.AsyncClient client = new CommunicationService.
                    AsyncClient(new TBinaryProtocol.Factory(), new TAsyncClientManager(),
                    new TNonblockingSocket("168.7.23.172", 7913));

            System.out.println("Client registering...");
            client.registerConnection("10.11.11.1", new RegisterCallback());

            client = new CommunicationService.
                    AsyncClient(new TBinaryProtocol.Factory(), new TAsyncClientManager(),
                    new TNonblockingSocket("168.7.23.172", 7913));

            System.out.println("Request 1...");
            client.request("10.11.11.1", 1, 2054736273000000L, destAddr1, new RequestCallback());

        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

	/*public static void main(String[] args) {
        Client appClient = new Client();
		appClient.invoke();
		try{
        	Thread.sleep(60000);
        }
        catch(InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
	}*/

    class RegisterCallback implements AsyncMethodCallback<CommunicationService.AsyncClient.registerConnection_call> {
        public void onComplete(CommunicationService.AsyncClient.registerConnection_call registerConnection_call) {
            try {
                boolean result = registerConnection_call.getResult();
                System.out.println("Register at the controller: " + result);
            } catch (TException e) {
                e.printStackTrace();
            }
        }

        public void onError(Exception e) {
            System.out.println("Error : ");
            e.printStackTrace();
        }
    }

    class RequestCallback implements AsyncMethodCallback<CommunicationService.AsyncClient.request_call> {
        public void onComplete(CommunicationService.AsyncClient.request_call request_call) {
            try {
                RequestReturn result = request_call.getResult();
                if (result.accept)
                    System.out.println("Accepted");
                    //System.out.println("Request at the controller: " + result.accept+" "+result.groupAddr+" ["+Common.groupAddrToIdentifier(result.groupAddr).srcAddr+", "+Common.groupAddrToIdentifier(result.groupAddr).id+"] "+result.time+" "+System.currentTimeMillis());
                else
                    System.out.println("Request at the controller: " + result.accept);
            } catch (TException e) {
                e.printStackTrace();
            }
        }

        public void onError(Exception e) {
            System.out.println("Error : ");
            e.printStackTrace();
        }
    }

    class ExtendCallback implements AsyncMethodCallback<CommunicationService.AsyncClient.extend_call> {
        public void onComplete(CommunicationService.AsyncClient.extend_call extend_call) {
            try {
                RequestReturn result = extend_call.getResult();
                if (result.accept)
                    System.out.println("Accepted");
                    //System.out.println("Extend at the controller: " + result.accept+" "+result.groupAddr+" ["+Common.groupAddrToIdentifier(result.groupAddr).srcAddr+", "+Common.groupAddrToIdentifier(result.groupAddr).id+"] "+result.time+" "+System.currentTimeMillis());
                else
                    System.out.println("Extend at the controller: " + result.accept);
            } catch (TException e) {
                e.printStackTrace();
            }
        }

        public void onError(Exception e) {
            System.out.println("Error : ");
            e.printStackTrace();
        }
    }

    class WithdrawCallback implements AsyncMethodCallback<CommunicationService.AsyncClient.withdraw_call> {
        public void onComplete(CommunicationService.AsyncClient.withdraw_call withdraw_call) {
            try {
                boolean result = withdraw_call.getResult();
                System.out.println("Withdraw at the controller: " + result);
            } catch (TException e) {
                e.printStackTrace();
            }
        }

        public void onError(Exception e) {
            System.out.println("Error : ");
            e.printStackTrace();
        }
    }

}
