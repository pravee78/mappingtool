import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;

public class ProcessJob implements Runnable {

	private JobObject jo;
	private Connection conn;
	private DataObj dobj;
	Statement stmt = null;
	boolean checkdir = true;
	FTPClient ftpClient = new FTPClient();

	public ProcessJob(JobObject jo, Connection conn, DataObj dobj) {
		super();
		this.jo = jo;
		this.conn = conn;
		this.dobj = dobj;
	}

	public boolean validateQuestions() {
		List<String> validtypes = dobj.getTypeslist();
		List<String> matchtypes = dobj.getMatchlist();
		// check types are valid or not

		for (DemoQuestionObject qu : jo.getQues().values()) {
			if (!validtypes.contains(qu.getType_of_question())) {
				System.out.println("there is some error in the type please check the given type is "
						+ qu.getType_of_question() + "\t\t the  valid types are" + validtypes);
				return false;
			}
			if (matchtypes.contains(qu.getType_of_question())) {
				if (qu.getAnswerMap().size() < 1) {
					System.out.println("there is problem in answers in onecount and third party answers in question "
							+ qu.getOncount_question());
					return false;
				}
			}
			/*
			 * if(matchtypes.contains(qu.getType_of_question())) {//check answer
			 * mappings are of equal length or not
			 * 
			 * if(qu.getOnecount_answers().length!=qu.getThirt_party_answers().
			 * length) { System.out.println(
			 * "there is mismatch in no of answers in onecount and third party answers in question "
			 * +qu.getOncount_question()); System.out.println("there are "
			 * +qu.getOnecount_answers().length+
			 * " answers in oneocunt and there are "
			 * +qu.getThirt_party_answers().length+
			 * " quesitons in third party answers"); return false; } }
			 */

		}
		return true;

	}
	private static void showServerReply(FTPClient ftpClient) {
	    String[] replies = ftpClient.getReplyStrings();
	    if (replies != null && replies.length > 0) {
	        for (String aReply : replies) {
	            System.out.println("SERVER: " + aReply);
	        }
	    }
	}
	public boolean validateJob() {
		boolean flag = false;
		// check valid client or not

		// check weather the job is authorized or not
		// check database exists or not
		try {
			stmt = conn.createStatement();
			String sql;
			sql = "Show databases";
			System.out.println(jo.getDatabase_name());
			ResultSet rs_db = stmt.executeQuery(sql);
			while (rs_db.next()) {
				if (jo.getDatabase_name().equals(rs_db.getString(1)))
					flag = true;
			}
			if (flag == false) {
				System.out.println("Database not found, Please check configuration");
				return false;
			}
			conn.setCatalog(jo.getDatabase_name());
			// check table exists or not
			/*
			 stmt = conn.createStatement(); String sql_tables; sql_tables =
			 "Show tables"; ResultSet rs_table =
			 stmt.executeQuery(sql_tables); flag = false; while
			 (rs_table.next()) { if
			 (jo.getTable_name().equals(rs_table.getString(1))) flag = true; }
			 if (flag == false) { System.out.println(
			 "Table not found, Please check configuration"); return false; }
			 */

			// check ftp location is valid and we are able to do ftp to that
			// location
			try
			{
				//TODO formalize and add logging statements and test
			ftpClient.connect(jo.getFTPHOST(), dobj.getSftpport());
			showServerReply(ftpClient);
			int replyCode = ftpClient.getReplyCode();
			if (!FTPReply.isPositiveCompletion(replyCode)) {
				System.err.println("Operation failed. Server reply code: " + replyCode);

			}
			boolean success = ftpClient.login(jo.getFTP_UNAME(), jo.getFTPPassword());
			if (!success) {
				System.err.println("Could not login to the server");

			} else {
				System.out.println("LOGGED IN SERVER");
			}

			ftpClient.changeWorkingDirectory(jo.getFileinput());
			int returnCode = ftpClient.getReplyCode();
			if (returnCode == 550) {
				System.err.println("directory not present");
				checkdir = false;
				InputStream inputStream = ftpClient.retrieveFileStream(jo.getFileinput());
				returnCode = ftpClient.getReplyCode();
				if (inputStream == null || returnCode == 550) {
					System.err.println("File not present, recheck file/directory name");
					return false;
				} else
					System.out.println("file present");
			}
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
		
			

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 

		// check notification mail ids are in valid format
		for (String mailid : jo.getNotification_mailids()) {
			try {
				InternetAddress emailAddr = new InternetAddress(mailid);
				emailAddr.validate();
			} catch (AddressException ex) {
				System.out.println("Invalid E-mail address");
				return false;
			}
		}
		return true;
	}

	public boolean getQuestions() {
		try {

			stmt = conn.createStatement();
			//TODO take questions table form jo object
			String sql = "select * from "+ dobj.getMysql_questions_table()+" where jobid ='" + jo.getJobid() + "'";
			ResultSet rs = stmt.executeQuery(sql);

			while (rs.next()) {
				//TODO eliminate dependency on type of question
				DemoQuestionObject qu = new DemoQuestionObject(rs.getString("oncount_question").trim().toLowerCase(),
						rs.getString("third_party_question").trim().toLowerCase(), rs.getString("type_of_question").trim().toLowerCase());
				qu.setValidations(rs.getString("validations").split(","));
				qu.setQid(rs.getInt("oc_qid"));
				if(dobj.getMatchlist().contains(rs.getString("type_of_question").trim().toLowerCase()))qu.setMatchType(true);
				if (jo.isImport()) {
					qu.addFromArrays(rs.getString("thirt_party_answers").split(","),
							rs.getString("onecount_answers").split(","));
					
					System.out.println(qu.getThird_party_question().trim().toLowerCase()+"------" +qu);
					jo.addQues(qu.getThird_party_question().trim().toLowerCase(), qu);
				} else {
					qu.addFromArrays(rs.getString("onecount_answers").split(","),
							rs.getString("thirt_party_answers").split(","));
					
					jo.addQues(qu.getOncount_question().trim().toLowerCase(), qu);
				}
				
			}
			
			return true;
		} catch (Exception e) {
			System.out.println("error getting questions");
			return false;
		}
		// connect to the conection and get questions and update the job object
	}

	@Override
	public void run() {
		System.out.println("started running the job");
		System.out.println(this.jo);
		boolean ret=this.validateJob();
		if(ret)
		{
			boolean questions=this.getQuestions();
			System.out.println("getting questions "+questions);
			if(jo.isImport())
			{
				ImportInputProcessor ip=new ImportInputProcessor(jo, dobj,conn);
				ip.processData();
			}
			else
			{
				ExportTableProcessor ex=new ExportTableProcessor(jo, dobj,conn);
				ex.exportTable();
			}
		}
		
		
		// Initialize info

		// validate job

		// get question and answer maping
		// Initialize validations to be implemented
		// validate questions and validations
		// read data
		// apply validations if needed
		// export csv with correct things
		// update stats
		// load csv into mysql
		// update stats

	}

}
