package riemann.java.client.tests;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.LinkedBlockingQueue;
import java.net.*;
import java.io.*;

import com.aphyr.riemann.client.RiemannClient;
import com.aphyr.riemann.Proto.Attribute;
import com.aphyr.riemann.Proto.Event;
import com.aphyr.riemann.Proto.Msg;

public abstract class Server {
    public static InetSocketAddress getAddr() {
      final InetAddress loopback = InetAddress.getLoopbackAddress();
      int port = 55433;
      if (System.getProperty("RIEMANN_TEST_PORT") != null) {
          port = Integer.parseInt(System.getProperty("RIEMANN_TEST_PORT"));
      }
      return new InetSocketAddress(loopback, port);
    }

	public int port;
	public Thread thread;
  public ServerSocket serverSocket;
  public LinkedBlockingQueue<Msg> received = new LinkedBlockingQueue<Msg>();

	public InetSocketAddress start() throws IOException {
            final InetSocketAddress addr = getAddr();
            this.port = addr.getPort();
            this.serverSocket = new ServerSocket();
            this.serverSocket.setReuseAddress(true);
            this.serverSocket.bind(addr);
            this.thread = mainThread(this.serverSocket);
            this.thread.start();

            return addr;
	}

	public void stop() {
    if (this.thread != null) {
        this.thread.interrupt();
      this.thread = null;
    }
    try {
        this.serverSocket.close();
    } catch (IOException e) {
        e.printStackTrace();
    }
		this.port = -1;
		this.serverSocket = null;
	}

	public Thread mainThread(final ServerSocket serverSocket) {
    return new Thread() {
      @Override
      public void run() {
        try {
          Socket sock = null;
          DataOutputStream out = null;
          DataInputStream in = null;

          try {
            // Accept connection
            while((sock = serverSocket.accept()) != null) {
              // Set up streams
              out = new DataOutputStream(sock.getOutputStream());
              in = new DataInputStream(sock.getInputStream());

              // Over each message
              while (true) {
                // Read length
                final int len = in.readInt();
                
                // Read message
                final byte[] data = new byte[len];
                in.readFully(data);
                final Msg request = Msg.parseFrom(data);
                
                // Log request
                received.put(request);

                // Handle message
                final Msg response = handle(request);
                
                // Write response
                out.writeInt(response.getSerializedSize());
                response.writeTo(out);
              }
            }
          } catch (EOFException e) {
            // Socket closed.
          } finally {
            if (out  != null) { out.close();  }
            if (in   != null) { in.close();   }
            if (sock != null) { sock.close(); }
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    };
  }

  public abstract Msg handle(final Msg m);
}
