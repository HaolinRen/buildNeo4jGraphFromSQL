
import java.sql.*;

public class SQLReader {
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	static final String DB_URL = "jdbc:mysql://localhost:8888/GTest";
	static final String USER = "root";
	static final String PASS = "root";
	Connection conn;
	Statement stmt;
	
	public void connect() {
		try {
			Class.forName(JDBC_DRIVER);
			System.out.println("**************************************");
			System.out.println("Connecting to database...");
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			System.out.println("Connected to the: " + DB_URL + ".");
			System.out.println("**************************************");
		} catch (Exception e) {
			System.out.println("Can't connect to database, please confirm the database address and password.");
			System.exit(0);
		}
	}
	
	public ResultSet excute(String sql) {
		try {
			System.out.println("Creating statement...");
			stmt = conn.createStatement();
			System.out.println(sql);
			ResultSet rs = stmt.executeQuery(sql);
			return rs;
		} catch (Exception e) {
			System.out.println("Faild to excute statement.");
			return null;
		}
		
	}
	
	public void closeConnect() {
		try {
			stmt.close();
			conn.close();
			System.out.println("**************************************");
			System.out.println("Connection closed");
			System.out.println("**************************************");
		} catch (Exception e) {
			System.out.println("Failed to close the connection");
		}
	}
}
