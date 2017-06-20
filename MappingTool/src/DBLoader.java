import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author praveena
 *
 */
public class DBLoader {
	// Database operations
	private Connection conn;
	String databasename;
	String host;
	String uname;
	String pwd;

	public DBLoader(String database_name, String mysqlHost, String uname, String pwd) {
		this.databasename = database_name;
		this.host = mysqlHost;
		this.uname = uname;
		this.pwd = pwd;
	}

	// Generic Constructor
	public DBLoader() {
		// TODO Auto-generated constructor stub
	}

	/*
	 * This block is used to open the database connection.
	 */

	public void connect(String DB_URL, String USER, String PASS, boolean noret) {
		try {
			Class.forName("org.gjt.mm.mysql.Driver");
			System.out.println("Connecting to database...");
			this.conn = DriverManager.getConnection(DB_URL, USER, PASS);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Deprecated
	public Connection connect(String DB_URL, String USER, String PASS) {
		try {
			Class.forName("org.gjt.mm.mysql.Driver");
			System.out.println("Connecting to database...");
			this.conn = DriverManager.getConnection(DB_URL, USER, PASS);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return this.conn;
	}

	/*
	 * Generate URL
	 * 
	 */
	public String GenerateURL(String host, String database) {
		// "jdbc:mysql://localhost:3306/test";
		return "jdbc:mysql://" + host + "/" + database;
	}

	/*
	 * This block is used to execute a query and return the 
	 * query results as a resultset 
	 */
	public ResultSet ExecuteQuery(String sql_query) {

		try {
			/*
			 * if the connection is closed or is not
			 * established
			 */
			if (conn == null || conn.isClosed())  {
				System.err.println("There is problem with connection object");
				System.out.println("con is closed");
				return null;
			} else {
				/* 
				 * This is used to create a statement object, execute the query 
				 * and return the query result
				 */
				Statement stmt = conn.createStatement();
				return stmt.executeQuery(sql_query);
			}
		} catch (SQLException e) {
			/*
			 * This block is used to print the exceptions occurred during 
			 * the statement creation or connection problem or execution of the query 
			 * 
			 */
			System.err.println("There is problem with connection object");
			e.printStackTrace();

			return null;

		}
	}
	/*
	 * End of query execution block
	 */
@Deprecated
	public ResultSet ExecuteQuery(String sql_query, Connection conn1) {
		this.conn = conn1;
		try {
			if (conn == null || conn.isClosed()) {
				System.err.println("There is problem with connection object");
				System.out.println("con is closed");
				return null;
			} else {
				Statement stmt = conn.createStatement();
				return stmt.executeQuery(sql_query);
			}
		} catch (SQLException e) {
			System.err.println("There is problem with connection object");
			e.printStackTrace();

			return null;

		}
	}
/*
 * This method returns a boolean value of true if a
 *  ResultSet object can be retrieved; otherwise, it returns false.
 * 
 */
	public boolean Execute(String sql_query) {

		try {
			/*
			 * if the connection is closed or is not
			 * established
			 */
			if (conn == null || conn.isClosed()) {
				System.err.println("There is problem with connection object");
				System.out.println("con is closed");
				return false;
			} else {
				/* 
				 * This is used to create a statement object, execute the query 
				 * and return the query result
				 */
				Statement stmt = conn.createStatement();
				return stmt.execute(sql_query);
			}
		} catch (SQLException e) {
			/*
			 * This block is used to print the exceptions occurred during 
			 * the statement creation or connection problem or execution of the query 
			 * 
			 */
			System.err.println("There is problem with connection object");
			
			e.printStackTrace();
			return false;
		}

	}
	/*
	 * End of execute block
	 */
	@Deprecated
	public boolean Execute(String sql_query, Connection conn) {
		this.conn = conn;
		try {
			if (conn == null || conn.isClosed()) {
				System.err.println("There is problem with connection object");
				System.out.println("con is closed");
				return false;
			} else {
				Statement stmt = conn.createStatement();
				return stmt.execute(sql_query);
			}
		} catch (SQLException e) {
			System.err.println("There is problem with connection object");
			System.err.println("here1......");
			e.printStackTrace();
			return false;
		}

	}
/* 
 * 
 * This method is used to close the connection
 * 
 */
	public boolean close(Connection conn) {
		try {
			/*
			 * Check if the connection is already closed or is null.
			 * If yes return true.
			 */
			if (conn == null || conn.isClosed()) {
				System.out.println("con is closed");
				return true;
			}
			/*
			 * Close the connection 
			 */
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			/*
			 * Catch any exceptions occurred during the connection close process
			 */
			e.printStackTrace();
			return false;
		}
		return true;
	}
/*
 * End of close connection method
 */
	
	/*
	 * This method returns the number of rows affected by the execution of the SQL statement.
	 * Use this method to execute SQL statements, for which you expect to get a number of rows affected - 
	 * for example, an INSERT, UPDATE, or DELETE statement.
	 */
	public int ExecuteUpdate(String sql_query) {
		try {
			/*
			 * if the connection is closed or is not
			 * established
			 */
			if (conn == null || conn.isClosed()) {
				System.err.println("There is problem with connection object");
				System.out.println("con is closed");
				return 0;
			} else {
				/* 
				 * This is used to create a statement object, execute the query 
				 * and return the query result
				 */
				Statement stmt = conn.createStatement();
				return stmt.executeUpdate(sql_query);
			}
		} catch (SQLException e) {
			/*
			 * This block is used to print the exceptions occurred during 
			 * the statement creation or connection problem or execution of the query 
			 * 
			 */
			System.err.println("There is problem with connection object");
			e.printStackTrace();
			return 0;
		}

	}
	/*
	 * End of Execute Update Method
	 */
	@Deprecated
	public int ExecuteUpdate(String sql_query, Connection conn) {
		this.conn = conn;
		try {
			if (conn == null || conn.isClosed()) {
				System.err.println("There is problem with connection object");
				System.out.println("con is closed");
				return 0;
			} else {
				Statement stmt = conn.createStatement();
				return stmt.executeUpdate(sql_query);
			}
		} catch (SQLException e) {
			System.err.println("There is problem with connection object");
			System.out.println("here2......");
			e.printStackTrace();
			return 0;
		}

	}
}
