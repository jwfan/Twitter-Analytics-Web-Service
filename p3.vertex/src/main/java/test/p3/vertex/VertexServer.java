package test.p3.vertex;

import java.text.SimpleDateFormat;
import java.util.Date;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.Router;

public class VertexServer extends AbstractVerticle {
	
	private String TEAMID = "LXFreee";
	private String TEAM_AWS_ACCOUNT_ID = "7104-6822-7247";
	private String X = "12389084059184098308123098579283204880956800909293831223134798257496372124879237412193918239183928140";
	
	@Override
	public void start(Future<Void> fut) {
		Vertx vertx = Vertx.vertx(new VertxOptions().setWorkerPoolSize(40));
		Router router = Router.router(vertx);
		HttpServerOptions options = new HttpServerOptions();
//		options.setAcceptBacklog(32767);
//		options.setUsePooledBuffers(true);
//		options.setReceiveBufferSize(4 * 1024);
		router.route("/q1").handler(routingContext -> {
			HttpServerResponse response = routingContext.response();
			String key = routingContext.request().getParam("key");
			String message = routingContext.request().getParam("message");
			int Xlength = X.length();
			int Ylength = key.length();
			int layers = 0;
			/* Test the validation of key & message */
			layers = (int) Math.floor(Math.sqrt(message.length()*2));
			if (Ylength > Xlength || layers == 0 || layers*(layers+1) != message.length() *2) {
				response.putHeader("content-type", "text/plain;charset=UTF-8").end(String.format("INVALID"));
			} else {
				/*
				 * 1.Caesarify: step secretKey X =
				 * 12389084059184098308123098579283204880956800909293831223134798257496372124879237412193918239183928140
				 * Y=key cipherText=Z
				 **/
				int[] keyX = new int[Xlength];
				for (int i = 0; i < X.length(); i++) {
					keyX[i] = Integer.valueOf(X.charAt(i)+"");
				}
				int[] keyY = new int[Ylength];
				for (int i = 0; i < key.length(); i++) {
					keyY[i] = Integer.valueOf(key.charAt(i)+"");
				}
				int Z;
				int sum1=0;
				int sum2=0;
				for (int i = Ylength-2; i<Xlength-1; i++) {
					sum1+=keyX[i];
				}
				for (int i = Ylength-1; i<Xlength; i++) {
					sum2+=keyX[i];
				}
				
				Z=(sum1+keyY[Ylength-2])%10*10+(sum2+keyY[Ylength-1])%10;

				/*
				 * 2.KeyGen step: minikey K = 1 + Z % 25
				 */
				int K = 1 + Z % 25;
				 /* 3.Spiralize step: ciphertext=message Use the minikey K & to
				 * cipherText Z to decrypt the message O
				 */

				// initialize the matrix with the message
				StringBuilder sb = new StringBuilder();
				char matrix[][] = new char[layers][layers];
				int x[] = { 1, 0, -1 };
				int y[] = { 0, 1, -1 };
				int col = layers;
				int row = layers;
				int xStart = 0;
				int yStart = 0;
				int direction = 0;
				int addedCol = 0;
				int addedRow = 0;
				int candidateCells = 0;
				int addedCells = 0;
				for (int i = 0; i < message.length(); i++) {
					if (x[direction] == 0) {
						candidateCells = row - addedRow;
					} else {
						candidateCells = col - addedCol;
					}
					if (candidateCells < 0) {
						break;
					}
					matrix[xStart][yStart] = message.charAt(i);
					addedCells++;
					if (addedCells == candidateCells) {
						addedRow += 1;
						addedCol += 1;
						direction = (direction + 1) % 3;
						addedCells = 0;
					}
					xStart += x[direction];
					yStart += y[direction];
				}

				// Read message from matrix by original order and decryption
				for (int i = layers - 1; i >= 0; i--) {
					for (int j = i; j < layers; j++) {
						char c = matrix[j][i];
						if(c - K >= 65) {
							c = (char) (c- K);
						} else{
							c = (char) (90 - (K- (c - 64)));
						}
						sb.append(c);
					}
				}

				/* Write response */
				Date date = new Date();
				SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				String result = TEAMID + "," + TEAM_AWS_ACCOUNT_ID + "\n" + df.format(date) + "\n" + sb.toString()+"\n";
				response.putHeader("content-type", "text/plain;charset=UTF-8").setStatusCode(200).end(result);
			}
		});
		
		vertx.createHttpServer().requestHandler(router::accept).listen(
				config().getInteger("http.port", 80), "0.0.0.0", result -> {
					if (result.succeeded()) {
						fut.complete();
					} else {
						fut.fail(result.cause());
					}
				});
	}

}
