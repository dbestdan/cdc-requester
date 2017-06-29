import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * This is a socket client it will request for a socket connection to the cdc
 * server. After successful connectio of the socket, it will wait for the
 * freshness time. Which indicates upto which data has been captured. we have
 * two cdc, so there will be two threads of SocketClientRunnable running.
 * 
 * @author hadoop
 *
 */
public class SocketClientRunnable implements Runnable {



	private Socket socket = null;;
	private int portNumber;
	private String serverName;
	private DataInputStream inputStream;
	private DataOutputStream outputStream;
	private long requestTime = 0L;
	private long freshness = 0L;
	private long prevFreshness = 0L;
	private long recordedTime = 0L;
	private long requestInterval = 0L;
	private long timeWindow = 0L;
	// public SocketClientRunnable(String serverName, int portNumber) {
	// pc4Port = Integer.parseInt(System.getProperty("pc4Port"));
	// pc5Port = Integer.parseInt(System.getProperty("pc5Port"));
	// pc4Socket = new Socket("pcnode4", pc4Port);
	// pc5Socket = new Socket("pcnode5", pc5Port);
	// logSocketClient = new Socket("pcnode2",portNumber);
	// output = new DataOutputStream(logSocketClient.getOutputStream());
	// input = new DataInputStream(logSocketClient.getInputStream());
	// }

	public SocketClientRunnable(String serverName, int portNumber) {
		this.portNumber = portNumber;
		this.serverName = serverName;
		this.freshness = System.currentTimeMillis();
		this.prevFreshness = System.currentTimeMillis();
		requestInterval = Long.parseLong(System.getProperty("requestInterval"));
		timeWindow = Long.parseLong(System.getProperty("timeWindow"));
		try {
			// connect to the socket
			while (true) {
				try {
					socket = new Socket(this.serverName, this.portNumber);
					if (socket != null) {
						System.out.println("Successfully Connected to " + serverName);
						break;
					}
				} catch (IOException e) {
					try {
						System.out.println("Problem in connecting to server.");
						Thread.sleep(1000);
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
			}
			inputStream = new DataInputStream(socket.getInputStream());
			outputStream = new DataOutputStream(socket.getOutputStream());
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		requestTime = System.currentTimeMillis();
		while (true) {
			try {
				// if (inputStream.available() != 0) {
				freshness = inputStream.readLong();
				recordedTime = System.currentTimeMillis();

				long numberOfRequest = ((recordedTime - requestTime) / requestInterval) + 1;

				for (int i = 0; i < numberOfRequest; i++) {
					long expirationTime = requestTime + timeWindow;
					long staleness = 0L;
					long latency = 0L;
					boolean expired = false;

					if (freshness >= requestTime && recordedTime <= expirationTime) {
						latency = recordedTime - requestTime;
						Status status = new Status(staleness, latency, expired);
						StatusWriterRunnable.requestStatusQueue.put(status);

						System.out.println("ServerName :" + serverName + " Freshness: " + freshness + " PrevFreshness: "+ prevFreshness +" Recorded T: "
								+ recordedTime + " Request T: " + requestTime + " Expiration T: " + expirationTime
								+ " Staleness: " + staleness + " Latency: " + latency + " Expired : " + expired);

						requestTime = requestTime + requestInterval;
					} else if (recordedTime > expirationTime) {
						latency = expirationTime - requestTime;
						staleness = requestTime - prevFreshness;
						expired = true;
						Status status = new Status(staleness, latency, expired);
						StatusWriterRunnable.requestStatusQueue.put(status);

						System.out.println("ServerName :" + serverName + " Freshness: " + freshness + " PrevFreshness: "+ prevFreshness +" Recorded T: "
								+ recordedTime + " Request T: " + requestTime + " Expiration T: " + expirationTime
								+ " Staleness: " + staleness + " Latency: " + latency + " Expired : " + expired);

						requestTime = requestTime + requestInterval;
					} else {
						break;
					}

				}

				prevFreshness = freshness;

			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

}
