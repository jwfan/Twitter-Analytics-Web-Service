package p3.mysql;

import org.xnio.Options;

import io.undertow.Handlers;
import io.undertow.Undertow;
//import io.undertow.UndertowOptions;

/**
 * The undertow server
 * @author kayjade
 *
 */
public class UndertowServer {

	public static void main(String[] args){
		Undertow server=Undertow.builder()
				.addHttpListener(80, "localhost")
				.setIoThreads(8)
				.setWorkerThreads(64)
				.setWorkerOption(Options.WORKER_TASK_MAX_THREADS, 64)
				.setHandler(Handlers.path()	// path routing
						.addPrefixPath("/q1", new Query1Handler())
						.addPrefixPath("/q2", new Query2Handler())
						)
				.build();
		server.start();
	}
}
