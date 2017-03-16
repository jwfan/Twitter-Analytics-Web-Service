package lxfree.query2.backend;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;
import org.json.JSONArray;

public class MySqlServlet extends HttpServlet {
	
	private static Connection conn;

    public MySqlServlet() {
        try {
			conn = ConnectionManager.getConnection();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
    }

    @Override
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response) 
            throws ServletException, IOException {
        JSONObject result = new JSONObject();

        String id = request.getParameter("id");
        String pwd = request.getParameter("pwd");

        /*
            Task 1:
            This query simulates the login process of a user, 
            and tests whether your backend system is functioning properly. 
            Your web application will receive a pair of UserName and Password, 
            and you need to check in your backend database to see if the 
	        UserName and Password is a valid pair. 
            You should construct your response accordingly:

            If YES, send back the userName and Profile Image URL.
            If NOT, set userName as "Unauthorized" and Profile Image URL as "#".
        */
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            String tableName = "users";
            String sql = "SELECT id, imageurl FROM " + tableName + " WHERE id ='" + id + "' and password ='" + pwd + "'";
            ResultSet rs = stmt.executeQuery(sql);
            String name = "Unauthorized";
            String imageUrl = "#";
            if (rs.next()) {
                name = rs.getString("id");
                imageUrl = rs.getString("imageurl");
            }
            result.put("name", name);
            result.put("profile", imageUrl);
            PrintWriter writer = response.getWriter();
            writer.write(String.format("returnRes(%s)", result.toString()));
            writer.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    protected void doPost(final HttpServletRequest request, final HttpServletResponse response) 
            throws ServletException, IOException {
        doGet(request, response);
    }
}
