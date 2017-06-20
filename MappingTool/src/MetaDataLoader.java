
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class MetaDataLoader {
	DBLoader db = new DBLoader();
	DataObj dobj= new DataObj();
	public Connection getConnectionFromProp(String propfilename) {
		InputStream is;
		Connection conn = null;
		//TODO if some properties are not available log an error and exit program
		try {
			Properties prop = new Properties();
			is = new FileInputStream(propfilename);
			prop.load(is);
			String host = prop.getProperty("mysql_server");
			String jobs_table = prop.getProperty("mysql_jobs_table");
			String questions_table=prop.getProperty("mysql_questions_table");
			String database = prop.getProperty("mysql_database");
			String uname = prop.getProperty("mysql_uname");
			String pwd = prop.getProperty("mysql_pwd");
			String validtypes = prop.getProperty("valid_types");
			String matchtypes = prop.getProperty("match_types");
			String process=prop.getProperty("process");
			
			
			dobj.setMysqlUname(uname);
			dobj.setMysqlPwd(pwd);
			dobj.setMysqlHost(host);
			dobj.setMysqldatabase(database);
			dobj.setProcess(process==null?"pending":process);
			dobj.setMysql_jobs_table(jobs_table);
			dobj.setMysql_questions_table(questions_table);
			dobj.setMatchlist(Arrays.asList(matchtypes.split(",")));
			dobj.setSftpport(prop.getProperty("ftp_port"));
			dobj.setSshport(prop.getProperty("ssh_port"));
			dobj.setRootpwd(prop.getProperty("root_password"));
			dobj.setTypeslist(Arrays.asList(validtypes.split(",")));  
			dobj.setSame(Boolean.valueOf(prop.getProperty("same")));
			dobj.setExportpath(prop.getProperty("export_location"));
			
			String url = db.GenerateURL(host,  database);
			System.out.println(url);
			conn = db.connect(url, dobj.getMysqlUname(), dobj.getMysqlPwd());
		} catch (FileNotFoundException e) {
			System.out.println("file not found");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return conn;

	}

	public List<JobObject> getJobs(Connection conn) {
		List<JobObject> jobs = new ArrayList<>();

		Statement stmt;
		try {
			stmt = conn.createStatement();
			String sql = "select * from "+dobj.getMysql_jobs_table() +" where status = '"+dobj.getProcess()+"'";
			ResultSet rs = stmt.executeQuery(sql);

			while (rs.next()) {
				JobObject jo = new JobObject(rs.getString("jobid"), rs.getString("submitted_by"),
						rs.getString("validationkey"), rs.getString("jobname"), rs.getString("notification_mailids"),
						rs.getTimestamp("timestamp_submitted"), rs.getString("fileinput"), rs.getString("fileoutput"),
						rs.getString("client"), rs.getString("table_name"), rs.getString("database_name").trim(),
						rs.getString("FTPHOST"), rs.getString("FTP_UNAME"),rs.getString("FTPPASSWORD"), true,rs.getString("operation").equalsIgnoreCase("import"));
				jo.setDedupeSet(rs.getString("dedupeset").trim().toLowerCase());
				jo.setEmail_iden(rs.getInt("email_field"));
				
				// Retrieve by column name
				jobs.add(jo);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return jobs;

	}



	public static void main(String[] args) {
		MetaDataLoader me = new MetaDataLoader();
	//TODO make this come from command line use get opts n java
		Connection con = me.getConnectionFromProp("src/Properties.conf");
		try {
			if (con == null || con.isClosed()) {
				System.out.println("some error with connection");
				// TODO retry connection
				//after 3 retires exit program
				// TODO add logging statement
			}
			for (JobObject j : me.getJobs(con)) {
				//TODO make dobj and con syncronized
				 ProcessJob job = new ProcessJob(j, con, me.dobj); 
				 Thread jo= new Thread(job);
			      jo.start();
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}