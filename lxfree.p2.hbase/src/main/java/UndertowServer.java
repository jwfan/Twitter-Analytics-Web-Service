

import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.PathHandler;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.servlet.api.DeploymentManager;
import query1.FrontDeskServlet;
import query2.HBaseServletQuery2;
import query3.HBaseServletQuery3;

import javax.servlet.ServletException;

import static io.undertow.servlet.Servlets.defaultContainer;
import static io.undertow.servlet.Servlets.deployment;
import static io.undertow.servlet.Servlets.servlet;
import io.undertow.Handlers;

/*
	You don't have to modify this file to finish task 1~5.
*/
public class UndertowServer {

	public UndertowServer() throws Exception {

	}

	public static final String PATH = "/";

	public static void main(String[] args) throws Exception {
		try {
			DeploymentInfo servletBuilder = deployment().setClassLoader(UndertowServer.class.getClassLoader())
					.setContextPath(PATH).setDeploymentName("handler.war")
					.addServlets(
							servlet("FrontDeskServlet", FrontDeskServlet.class).addMapping("/q1"),
							servlet("HBaseServletQuery2", HBaseServletQuery2.class).addMapping("/q2"),
							servlet("HBaseServletQuery3", HBaseServletQuery3.class).addMapping("/q3")
							
							);

			DeploymentManager manager = defaultContainer().addDeployment(servletBuilder);
			manager.deploy();

			HttpHandler servletHandler = manager.start();
			PathHandler path = Handlers.path(Handlers.redirect(PATH)).addPrefixPath(PATH, servletHandler);

			Undertow server = Undertow.builder().addHttpListener(80, "0.0.0.0").setWorkerThreads(50).setHandler(path).build();
			server.start();
		} catch (ServletException e) {
			throw new RuntimeException(e);
		}
	}
}
