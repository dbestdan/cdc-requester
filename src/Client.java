import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Client will fire SocketClientRunnalbe which will interact with each cdc
 * server and updates freshness (time upto which delta has captured)
 * 
 * It will also maintain two request map, request time is used as key and
 * expiration time is used as value. Each map stores request for individual cdc
 * 
 * RequestRunnable issues request across multiple cdc in round robin manner
 * 
 * Every time SocketClinetRunnable reads freshness value from socket, it will go
 * through assigned requests. It will check whether request has been expired or
 * some of them are fulfilled.
 * 
 * It calculates status of the request(staleness, latency and isExpired)
 * accordingly and assigned status to the StatusWriter.
 * 
 * StatisWriter will reads the status of each request, calculates average
 * staleness latency and expiration ratio. Finally it writes those information
 * to the file
 * 
 * @author hadoop
 *
 */

public class Client {

	public static void main(String[] args) {
		ArrayList<Thread> threads = new ArrayList<Thread>();
		long runDuration = Long.parseLong(System.getProperty("runDuration")) * 60000L;
		long sessionEndTime = System.currentTimeMillis() + runDuration;
		System.out.println("run Duration =" + runDuration);

		int numberOfCDCs = Integer.parseInt(System.getProperty("numberOfCDCs"));

		for (int i = 1; i <= numberOfCDCs; i++) {
			int port = Integer.parseInt(System.getProperty("port" + i));
			String serverName = System.getProperty("serverName" + i);
			SocketClientRunnable socket = new SocketClientRunnable(serverName, port);
			threads.add(new Thread(socket));

		}

		StatusWriterRunnable statusWriter = new StatusWriterRunnable();

		threads.add(new Thread(statusWriter));

		for (int i = 0; i < threads.size(); i++) {
			threads.get(i).start();
		}
		try {
			Thread.sleep(runDuration);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.exit(0);

	}

}
