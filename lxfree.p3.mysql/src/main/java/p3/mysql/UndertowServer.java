package p3.mysql;

import org.xnio.Options;

import io.undertow.Handlers;
import io.undertow.Undertow;
//import io.undertow.UndertowOptions;
import query2.Query2Handler;

/**
 * The undertow server
 * @author kayjade
 *
 */
public class UndertowServer {

	private static int ioThread=20;
	private static int workerThread=ioThread*8;
	
	public static void main(String[] args){
		Undertow server=Undertow.builder()
				.addHttpListener(80, "0.0.0.0")
				.setIoThreads(ioThread)
				.setWorkerThreads(workerThread)
				.setWorkerOption(Options.WORKER_TASK_MAX_THREADS, workerThread)
				.setHandler(Handlers.path()	// path routing
						.addPrefixPath("/q1", new Query1Handler())
						.addPrefixPath("/q2", new Query2Handler())
						)
				.build();
		server.start();
	}
}
