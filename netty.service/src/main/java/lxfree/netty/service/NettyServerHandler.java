package lxfree.netty.service;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.CONTINUE;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpHeaders.Values;

public class NettyServerHandler  extends ChannelInboundHandlerAdapter {
	private static final String TEAMID = "LXFreee";
	private static final String TEAM_AWS_ACCOUNT_ID = "7104-6822-7247";
	private static final String X = "12389084059184098308123098579283204880956800909293831223134798257496372124879237412193918239183928140";

	// private static final byte[] CONTENT = { 'H', 'e', 'l', 'l', 'o', ' ',
	// 'W', 'o', 'r', 'l', 'd' };

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) {
		ctx.flush();
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) {
		String result = new String();
		if (msg instanceof HttpRequest) {
			HttpRequest req = (HttpRequest) msg;

			String requestStr = req.getUri();
			String key = new String();
			String message = new String();

			/* get key Y and cipher text from the request URL */
			Pattern pattern1 = Pattern.compile("key=\\d+");
			Matcher matcher1 = pattern1.matcher(requestStr);
			if (matcher1.find()) {
				key = requestStr.substring(matcher1.start() + 4, matcher1.end());
				System.out.println("key: " + key);
			}
			Pattern pattern2 = Pattern.compile("message=\\w+");
			Matcher matcher2 = pattern2.matcher(requestStr);
			if (matcher2.find()) {
				message = requestStr.substring(matcher2.start() + 8);
				System.out.println("cipher: " + message);
			}
			
			/* decipher */
			int Xlength = X.length();
			int Ylength = key.length();
			int layers = 0;
			/* Test the validation of key & message */
			layers = (int) Math.floor(Math.sqrt(message.length()*2));
			if (Ylength > Xlength || layers == 0 || layers*(layers+1) != message.length() *2) {
				result="INVALID";
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
				System.out.println(sum1);
				System.out.println(sum2);
				
				Z=(sum1+keyY[Ylength-2])%10*10+(sum2+keyY[Ylength-1])%10;
				System.out.println(Z);
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
							c = (char) (90 - (K- (c - 65)));
						}
						sb.append(c);
					}
				}

				/* Write response */
				Date date = new Date();
				SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				result = TEAMID + "," + TEAM_AWS_ACCOUNT_ID + "\n" + df.format(date) + "\n" + sb.toString();
			}

			byte[] CONTENT = result.getBytes();

			if (HttpHeaders.is100ContinueExpected(req)) {
				ctx.write(new DefaultFullHttpResponse(HTTP_1_1, CONTINUE));
			}
			boolean keepAlive = HttpHeaders.isKeepAlive(req);
			FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK, Unpooled.wrappedBuffer(CONTENT));
			response.headers().set(CONTENT_TYPE, "text/plain");
			response.headers().set(CONTENT_LENGTH, response.content().readableBytes());

			if (!keepAlive) {
				ctx.write(response).addListener(ChannelFutureListener.CLOSE);
			} else {
				response.headers().set(CONNECTION, Values.KEEP_ALIVE);
				ctx.write(response);
			}
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		cause.printStackTrace();
		ctx.close();
	}
}