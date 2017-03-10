package flora.cc.teamproj;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;

public class ProfileServlet extends HttpServlet {

	@Override
	protected void doGet(final HttpServletRequest request, final HttpServletResponse response)
			throws ServletException, IOException {
		JSONObject result = new JSONObject();

		String key = request.getParameter("key");
		String message = request.getParameter("message");

		/* Test the validation of key & message */

		/*
		 * 1.Caesarify: step secretKey X =
		 * 12389084059184098308123098579283204880956800909293831223134798257496372124879237412193918239183928140
		 * Y=key cipherText=Z
		 **/

		/*
		 * 2.KeyGen step: minikey K = 1 + Z % 25
		 */

		/*
		 * 3.Spiralize step: ciphertext=message Use the minikey K & to
		 * cipherText Z to decrypt the message O
		 */

		/* Write response */

		PrintWriter writer = null;
		writer.write(String.format("returnRes(%s)", result.toString()));
		writer.close();

	}

	@Override
	protected void doPost(final HttpServletRequest request, final HttpServletResponse response)
			throws ServletException, IOException {
		doGet(request, response);
	}
}
