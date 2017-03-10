package flora.cc.teamproj;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;

public class FrontDeskServlet extends HttpServlet {
	String TEAMID = "LXFreee";
	String TEAM_AWS_ACCOUNT_ID = "710468227247";

	@Override
	protected void doGet(final HttpServletRequest request, final HttpServletResponse response)
			throws ServletException, IOException {
		PrintWriter writer = response.getWriter();
		JSONObject result = new JSONObject();

		String key = request.getParameter("key");
		String message = request.getParameter("message");
		String X = "12389084059184098308123098579283204880956800909293831223134798257496372124879237412193918239183928140";
		int Xlength = X.length();
		int Ylength = key.length();
		int layers = 0;
		/* Test the validation of key & message */
		layers = (int) Math.floor(Math.sqrt(message.length()));
		if (Ylength > Xlength || layers == 0) {
			writer.write(String.format("INVALID"));
			writer.close();
		} else {
			/*
			 * 1.Caesarify: step secretKey X =
			 * 12389084059184098308123098579283204880956800909293831223134798257496372124879237412193918239183928140
			 * Y=key cipherText=Z
			 **/
			int[] keyX = new int[Xlength];
			String[] strX = X.split(",");
			for (int i = 0; i < strX.length; i++) {
				keyX[i] = Integer.valueOf(strX[i]);
			}
			int[] keyY = new int[Ylength];
			String[] strY = key.split(",");
			for (int i = 0; i < strY.length; i++) {
				keyY[i] = Integer.valueOf(strY[i]);
			}
			int[] temp = new int[Ylength];
			temp = keyY;
			for (int i = 0; i < Xlength - Ylength; i++) {
				for (int j = 0; j < Ylength; j++) {
					temp[j] = (keyX[i + j] + temp[j]) % 10;
				}
			}
			/*
			 * 2.KeyGen step: minikey K = 1 + Z % 25
			 */
			int K = 1 + (temp[Ylength - 2] * 10 + temp[Ylength - 1]) % 25;

			/*
			 * 3.Spiralize step: ciphertext=message Use the minikey K & to
			 * cipherText Z to decrypt the message O
			 */
			
			// initialize the matrix with the message
			StringBuilder sb = new StringBuilder();
			char matrix[][] = new char[layers][layers];
			int x[] = {1,0,-1};
			int y[] = {0,1,-1};
			int col = layers;
			int row = layers;
			int xStart = 0;
			int yStart = 0;
			int direction = 0;
			int addedCol = 0;
			int addedRow = 0;
			int candidateCells = 0;
			int addedCells = 0;
			for(int i = 0; i < message.length(); i++) {
				if(x[direction] == 0) {
					candidateCells = row - addedRow;
				} else {
					candidateCells = col - addedCol;
				}
				if(candidateCells < 0) {
					break;
				}
				matrix[xStart][yStart] = message.charAt(i);
				addedCells++;
				if(addedCells == candidateCells) {
					addedRow += 1;
					addedCol += 1;
					direction = (direction + 1) % 3;
					addedCells = 0;
				}
				xStart += x[direction];
				yStart += y[direction];
			}
			
			// Read message from matrix by original order and decryption
			for(int i = layers-1; i >= 0; i--) {
				for(int j = i; j < layers; j++) {
					char c = matrix[j][i];
					c = (char) ((c + K) % 65 + 65);
					sb.append(c);
				}
			}
			

			/* Write response */
			Date date = new Date();
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			writer.write(String.format("returnRes(%s)",
					TEAMID + "," + TEAM_AWS_ACCOUNT_ID + "\n" + df.format(date) + "\n" + result.toString()));
			writer.close();
		}
	}

	@Override
	protected void doPost(final HttpServletRequest request, final HttpServletResponse response)
			throws ServletException, IOException {
		doGet(request, response);
	}
}
