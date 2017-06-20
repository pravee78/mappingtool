import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.SocketException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

public class FeedProcessor implements Runnable {
	/*
	 * Variables used in this method
	 */
	private FeedObject fo;
	private DataObj dobj;
	private DBLoader db = new DBLoader();
	private String path;
	private ExportInfo eobj = new ExportInfo();
	private HashMap<String, TransResObj> transactionMap = new HashMap<>();
	private JSch jsch = new JSch();
	private ChannelSftp sftpMySqlChannel;
	private ChannelSftp sftpChannel;
	private Session sessionmysql;
	private Session ftpsession;
	private String datepathiden;
	private HashSet<String> ocidlist = new HashSet<>();
	private FTPClient ftpClient = new FTPClient();
	private Mailing mail = new Mailing();
	StringBuffer infoLog_buffer = new StringBuffer();
	StringBuffer errorLog_buffer = new StringBuffer();
	Stats stat = new Stats();
	final Logger log_error = Logger.getLogger("logger_error");
	final Logger log_info = Logger.getLogger("logger_info");
	final Logger mainLog = Logger.getLogger("mainLogger");
	
	//String formatted = df.format(new Date());
	
	/*
	 * Initialize constructor with the FeedObject, Database object and the job
	 * to executed.
	 */
	public FeedProcessor(FeedObject fo, DataObj dobj, String jobname) {
		this.fo = fo;
		fo.setJobname(jobname);
		this.dobj = dobj;
		// TODO Auto-generated constructor stub
	}
	/*
	 * This block reads the feed settings, creates appropriate mappings and
	 * queries. Also establishes the FTP connection.
	 */
	private void initilizejob() {
		this.getMappingInfo(fo.getMap_id());
		this.initilizeFTP();
		// TODO Auto-generated method stub
	}
	/*
	 * End of initialize job method
	 */

	/*
	 * This method is used to get the FTP path , selects the required fields
	 * from database, maps the required values to a tree map and writes the
	 * final mapped values to the file.
	 */
	private void processTransactions() {
		StringBuffer sb = new StringBuffer();
		StringBuffer sb_ctrl = new StringBuffer();
		int transcount = 0;
		try {
			/*
			 * Define the filename to be created Filename format: trans_(jobid
			 * generated)_(ftp username)_(current date of job)
			 */
			infoLog_buffer.append("Started processing transactions");
			String path, ctrlpath, transfilename = null;
			if(fo.isSFTP()){
				System.out.println("Processing SFTP");
			 path = sftpChannel.getHome() + "/trans_" + this.fo.getJobid() + "_" + this.fo.getFTP_UNAME() + "_"
					+ this.datepathiden + ".csv";	
			 ctrlpath = sftpChannel.getHome() + "/trans_ctrl_" + this.fo.getJobid() + "_" + this.fo.getFTP_UNAME()
					+ "_" + this.datepathiden + ".csv";
			 transfilename = "trans_" + this.fo.getJobid() + "_" + this.fo.getFTP_UNAME() + "_"
					+ this.datepathiden + ".csv";
			 /*
			  * This block writes the headers to file
			  */
			sftpChannel.put(new ByteArrayInputStream(("product,Product_status_col,subType\n").getBytes()), path,
					ChannelSftp.APPEND);
			}
			else {
				System.out.println("Processing FTP");
			 path = "trans_" + this.fo.getJobid() + "_" + this.fo.getFTP_UNAME() + "_"
					+ this.datepathiden + ".csv";	
			 ctrlpath = "trans_ctrl_" + this.fo.getJobid() + "_" + this.fo.getFTP_UNAME()
					+ "_" + this.datepathiden + ".csv";
			/*
			 * This block writes the headers to file
			 */
			ftpClient.appendFile(path, new ByteArrayInputStream("product,Product_status_col,subType\n".getBytes()));
			}
			/*
			 * Establish a new connection
			 */
			Connection con1 = db.connect(db.GenerateURL(dobj.getMysqlHost(), fo.getDatabase_name()),
					dobj.getMysqlUname(), dobj.getMysqlPwd());
			/*
			 * This statement is used to execute the query to retrieve the
			 * different parameters from gcn_transactionlog.
			 */
			PreparedStatement ps = con1.prepareStatement(
					"SELECT ProductID,subscribeType, GroupID, product_status_id FROM gcn_transactionlog WHERE UserID = ?");
			/*
			 * This block traverses through the ocids generated through
			 * demographics and performs the required transaction mappings and
			 * write to file for each ocid.
			 */
			for (String ocid : this.ocidlist) {
				/*
				 * To use the same String buffer for each iteration, delete the
				 * values in string buffer every time you iterate.
				 */
				sb.delete(0, sb.length());
				/*
				 * Set the dynamic parameter of OCID in the prepared statement
				 * query.
				 */
				ps.setInt(1, Integer.parseInt(ocid));

				ResultSet rs = ps.executeQuery();
				try {
					while (rs.next()) {
						// {"subscribeType":"r"}
						/*
						 * Map the retrieved values to the tree map.
						 */
						TransWriteObj tw = new TransWriteObj();
						/*
						 * Map the product code and group code to the tree.
						 */
						tw.addValue(this.getProdCode(rs.getString(1), rs.getString(3), "1"));
						/*
						 * Map the product status code to the tree.
						 */
						tw.addValue(this.getprodStatusCode(rs.getString(4), "2"));
						/*
						 * Map the Subscription type to the tree
						 */
						tw.addValue(this.getSubType(rs.getString(2), "3"));
						/*
						 * Retrieve all the values and add it to the String
						 * buffer
						 */
						sb.append(tw.getValue() + "\n");
						transcount++;
					}
					/*
					 * Write the mapped values to the file
					 */
					if(fo.isSFTP()){
					sftpChannel.put(new ByteArrayInputStream(sb.toString().getBytes()), path, ChannelSftp.APPEND);
					}
					else{
						ftpClient.appendFile(path, new ByteArrayInputStream(sb.toString().getBytes()));	
					}
				} catch (SQLException | IOException e) {
					// TODO Auto-generated catch block
					errorLog_buffer.append("Error in retreiving and saving transaction details\n");
				}
			}
			/*
			 * Write the values to transaction control file
			 */
			if(fo.isSFTP()){
				sb_ctrl.append(transfilename+"\n").append(Integer.toString(transcount) + "\n").append(this.getDate("yyyy_MMM_dd_HH_MM_ss") + "\n");
				sftpChannel.put(new ByteArrayInputStream(sb_ctrl.toString().getBytes()), ctrlpath, ChannelSftp.APPEND);
				}
				else{
					sb_ctrl.append(path+"\n").append(Integer.toString(transcount) + "\n").append(this.getDate("yyyy_MMM_dd_HH_MM_ss") + "\n");
					ftpClient.appendFile(ctrlpath, new ByteArrayInputStream(sb_ctrl.toString().getBytes()));
				}
		} // TODO Auto-generated method stub
		catch (SQLException e1) {
			// TODO Auto-generated catch block
			errorLog_buffer.append("Error in retreiving and saving transaction details\n");
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (SftpException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	private void processActivity() {
		infoLog_buffer.append("Performing activity mapping\n");
		/*
		 * To check the banner,message,web queries if they are present or if the
		 * value has been retrieved. If query is present execute the query.
		 */
		if (eobj.getBannerquery() != null && eobj.getBannerquery().length() > 10) {
			db.Execute(eobj.getBannerquery());
		}
		if (eobj.getMessageQuery() != null && eobj.getMessageQuery().length() > 1) {
			db.Execute(eobj.getMessageQuery());
		}
		if (eobj.getWebquery() != null && eobj.getWebquery().length() > 1) {
			db.Execute(eobj.getWebquery());
		}
	}
	/*
	 * End of process activity method.
	 */

	/*
	 * This block is used to start the process of demographic activities.
	 */
	private void processDemographics() {
		infoLog_buffer.append("Performing Demographics mapping\n");
		/*
		 * To execute the query and perform the required mappings
		 */
		this.datepathiden=this.getDate("MMM_dd_HH_MM_ss");
		this.exportTable();
		this.modifyData();
	}
	/*
	 * End of process demographics method.
	 */

	/*
	 * To perform the demographics mappings and write to the file
	 */
	private void modifyData() {
		System.out.println("came to modify data");
		System.out.println(this.path);
		/*
		 * Get the source file with the headers.
		 */
			// TODO Auto-generated method stub
		try {
			/*
			 * Read the source file and append all the question objects to a
			 * String Buffer
			 */
			InputStream stream = sftpMySqlChannel.get(this.path+"demo.csv");
			BufferedReader br = new BufferedReader(new InputStreamReader(stream));
			String line;
			SimpleDateFormat df = new SimpleDateFormat("YYYYMMdd");
			StringBuffer sb1 = new StringBuffer();
			StringBuffer sb1_ctrl = new StringBuffer();
			String ftpPath = null, ctrlpath, filename = null;
			for (QuestionObject qo : this.fo.getQues().values()) {
				sb1.append(qo.getQid() + ",");
			}
			/*
			 * Split the comma separated parameters in CSV file to a String
			 * array.
			 */
			String[] order = eobj.getOC_que_order().split(",");
			/*
			 * Define the filename to be created Filename format: demo_(jobid
			 * generated)_(ftp username)_(current date of job)
			 */
			
			//for(String o:order)System.out.print(o+"\t");
			String temp = "ocid," + eobj.getThirdp_que_order() + "\n";
			if(fo.isSFTP()){
				
				ftpPath = sftpChannel.getHome() + "/Brand_Contact_" + this.fo.getPublisher()+ "_" 
							+ this.fo.getBrand_name()+ "_" +this.fo.getBrand_id()+"_"+ df.format(new Date())+".txt";	
					 ctrlpath = sftpChannel.getHome() + "/Brand_Ctrl_" + this.fo.getPublisher()+ "_" 
								+ this.fo.getBrand_name()+"_" +this.fo.getBrand_id()+"_"+this.fo.getCampaign_id()+"_" +df.format(new Date())+".txt";
					 filename = "Brand_Contact_" + this.fo.getPublisher()+ "_" 
								+ this.fo.getBrand_name()+ "_" +this.fo.getBrand_id()+"_"+ df.format(new Date())+".txt";
					    /*
						 * Write the headers to the file.
						 */
						sftpChannel.put(new ByteArrayInputStream(temp.getBytes()),ftpPath, ChannelSftp.APPEND);
			}
			else {
				ftpPath = "Brand_Contact_" + this.fo.getPublisher()+ "_" 
						+ this.fo.getBrand_name()+ "_" +this.fo.getBrand_id()+"_"+ df.format(new Date())+".txt";
				this.eobj.setFilename(ftpPath);
				 ctrlpath = "Brand_Ctrl_" + this.fo.getPublisher()+ "_" 
							+ this.fo.getBrand_name()+"_" +this.fo.getBrand_id()+"_"+this.fo.getCampaign_id()+"_" +df.format(new Date())+".txt";
				    /*
					 * Write the headers to the file.
					 */
				 ftpClient.appendFile(ftpPath, new ByteArrayInputStream(temp.getBytes()));
			}
			/*
			 * Read the demographics from the source file and perform mapping
			 * line by line.
			 */
			int count = 0;
			while ((line = br.readLine()) != null) {
				try {
					count++;
					// if(count>100) break;
					System.out.println(count);
					/*
					 * To use the same String buffer for each iteration, delete
					 * the values in string buffer every time you iterate.
					 */
					sb1.delete(0, sb1.length());
					line=line.replaceAll("\\\\N", "");
					String[] data_split = line.endsWith(",")?(line+" ").split(","):line.split(",");
					/*
					 * Get the ocids from the demographics file and add them to
					 * the ocidList
					 */
					sb1.append(data_split[0] + ",");
					this.ocidlist.add(data_split[0]);
					for (int i = 0; i < order.length - 1; i++) {
						/*
						 * To check if the fields contain response mapping like
						 * if they are of select,checkbox or radio.If present
						 * get the responses and map them to the String array
						 * and add this response mapping array to the String
						 * Buffer.
						 */
						if (this.fo.getQues().get(order[i]).isMatchType()) {
							data_split[i + 1] = this.fo.getQues().get(order[i]).getAnswerMap()
									.get(data_split[i + 1].toLowerCase());
						}
						sb1.append(data_split[i + 1] + ",");
					}
					// System.out.println(this.fo.getQues().get(order[order.length-1])+"\t\t"+data_split[order.length].toLowerCase());

					if (this.fo.getQues().get(order[order.length - 1]).isMatchType()) {
						// System.out.println(data_split[order.length]);
						data_split[order.length] = this.fo.getQues().get(order[order.length - 1]).getAnswerMap()
								.get(data_split[order.length].toLowerCase());
					}
					sb1.append(data_split[order.length] + "\n");
					/*
					 * Write the final mappings output to the file.
					 */
					if(fo.isSFTP()){
						sftpChannel.put(new ByteArrayInputStream(sb1.toString().getBytes()), ftpPath,
						ChannelSftp.APPEND);
					}
					else{
						ftpClient.appendFile(ftpPath, new ByteArrayInputStream(sb1.toString().getBytes()));
					}
					
					//System.out.println("sb1 is -----------"+sb1);
					
					/*
					 * Updating the counter value.
					 */
					stat.incExportedrecords();

				} catch (ArrayIndexOutOfBoundsException e) {
					errorLog_buffer
							.append("This record doesn't have enough fields as per configuration-----" + line + "\n");
					stat.incFailedrecords();
				}
			}
			if(fo.isSFTP())
			{
				sb1_ctrl.append(filename+"\n").append(Integer.toString(stat.getExportedrecords()) + "\n").append(this.getDate("MM/dd/YYYY HH:mm:ss") + "\n");
				
				sftpChannel.put(new ByteArrayInputStream(sb1_ctrl.toString().getBytes()), ctrlpath,ChannelSftp.APPEND);	
			}
			else{
				sb1_ctrl.append(ftpPath+"\n").append(Integer.toString(stat.getExportedrecords()) + "\n").append(this.getDate("MM/dd/YYYY HH:mm:ss") + "\n");
				ftpClient.appendFile(ctrlpath, new ByteArrayInputStream(sb1_ctrl.toString().getBytes()));
			}
			stream.close();
		} catch (IOException e) {
			errorLog_buffer.append("Error IO Exception while saving demographic details\n");
		} catch (SftpException e) {
			// TODO Auto-generated catch block
			errorLog_buffer.append("Error in SFTP connection while saving demographic details\n");
		}
	}
	/*
	 * End of ModifyData method.
	 */
	private void initilizeFTP() {

		try {
			/*
			 * Logging statement
			 */
			infoLog_buffer.append("Initilizing connections to mysql and client ftp's\n");
			/*
			 * Connect to FTP through Java Secure Shell
			 */
			if(fo.isSFTP()){
				System.out.println("SFTP connect");
			this.ftpsession = jsch.getSession(this.fo.getFTP_UNAME(), this.fo.getFTPHOST(), this.dobj.getSshport());
			this.ftpsession.setPassword(this.fo.getFTPPassword());
			this.ftpsession.setConfig("StrictHostKeyChecking", "no");
			infoLog_buffer.append("Establishing Connection... with client end point\n");
			this.ftpsession.connect();
			this.sftpChannel = (ChannelSftp) ftpsession.openChannel("sftp");
			this.sftpChannel.connect();
			infoLog_buffer.append("SFTP Channel created\n");
			}
			//FTPClient ftpClient = new FTPClient();
			else
			{
			//System.out.println(this.fo.getFTPHOST()+"\t"+this.dobj.getSftpport());
			this.ftpClient.connect(this.fo.getFTPHOST(), this.dobj.getSftpport());
			int replyCode = ftpClient.getReplyCode();
			if (!FTPReply.isPositiveCompletion(replyCode)) {
				System.out.println("Operation failed. Server reply code: " + replyCode + "\n");
			}
			/*
			 * Login to the FTP client with the given username and password. If
			 * not success, appropriate error messages are logged.
			 */
			boolean success = ftpClient.login(this.fo.getFTP_UNAME(), this.fo.getFTPPassword());
			if (!success) {
				System.err.println("Could not login to the FTP server provided\n");

			} else {
				/*
				 * If success, then success message is logged.
				 */
				System.out.println("Logged into FTP server\n");
				System.out.println(ftpClient.isConnected());
				ftpClient.enterLocalPassiveMode();
			}
			}
//			System.out.println("user name: "+this.fo.getFTP_UNAME()+" Host: "+this.fo.getFTPHOST()+" PORt: "+this.dobj.getSftpport()+" Password: "+this.fo.getFTPPassword());
			
			/*
			 * Connect to MySQl server to write the database query to files.
			 */
			this.sessionmysql = jsch.getSession("root", this.dobj.getMysqlHost(), this.dobj.getSshport());
			this.sessionmysql.setPassword(this.dobj.getRootpwd());
			this.sessionmysql.setConfig("StrictHostKeyChecking", "no");
			infoLog_buffer.append("Establishing Connection...with mysql server\n");
			this.sessionmysql.connect();
			this.sftpMySqlChannel = (ChannelSftp) this.sessionmysql.openChannel("sftp");
			sftpMySqlChannel.connect();
			
			infoLog_buffer.append("SFTP Channel for mysql created\n");
			System.out.println("SFTP Channel for mysql created\n");
			infoLog_buffer.append("Connections established\n");

		} catch (JSchException | IOException e) {
			e.printStackTrace();
			errorLog_buffer.append("Problem in finding the file\n");

		}

	}
	/*
	 * End of initialize ftp method.
	 */
	/*
	 * This method is used to disconnect all the connections established.
	 */
	private void closeFTP() {
		this.sftpMySqlChannel.disconnect();
		this.sessionmysql.disconnect();
		if(fo.isSFTP()){
		this.sftpChannel.disconnect();
		infoLog_buffer.append("SFTP connection closed\n");
		this.ftpsession.disconnect();
		infoLog_buffer.append("FTP connection closed\n");
	}}
	/*
	 * End of initialize ftp method.
	 */
	/*
	 * This method is used to disconnect all the connections established.
	 */
	public void exportTable() {
		db.Execute(eobj.getQuery());
	}
	/*
	 * This method is used to create a date and return the date format
	 */
	public String getDate(String format) {
		DateFormat dateFormat = new SimpleDateFormat(format);
		Date date = new Date();
		return dateFormat.format(date);

	}
	/*
	 * Add subscriber type as JSON key and map key value pairs to transactions.
	 */
	private TransResObj getSubType(String subtypevalue, String pos) {
		// TODO Auto-generated method stub
		JSONObject j = new JSONObject();
		j.accumulate("subscribeType", subtypevalue);
		return this.transactionMap.get(j + "") == null ? new TransResObj("NA", pos) : this.transactionMap.get(j + "");

	}
	/*
	 * Add product status id as JSON key and map key value pairs to
	 * transactions.
	 */
	private TransResObj getprodStatusCode(String prodstatuscode, String pos) {
		JSONObject j = new JSONObject();
		j.accumulate("prod_status_id", prodstatuscode);
		return this.transactionMap.get(j + "") == null ? new TransResObj("NA", pos) : this.transactionMap.get(j + "");

	}
	/*
	 * Add GroupID and Product IDs as JSON keys and map key value pairs to
	 * transactions.
	 */
	private TransResObj getProdCode(String productid, String termid, String pos) {
		JSONObject j = new JSONObject();
		j.accumulate("GroupID", termid);
		j.accumulate("ProductID", productid);
		return this.transactionMap.get(j + "") == null ? new TransResObj("NA", pos) : this.transactionMap.get(j + "");
	}
	/*
	 * This method is used to connect to the database and ftp.
	 */
	public boolean validateJob() {
		boolean flag = false;
		// check valid client or not

		// check weather the job is authorized or not
		// check database exists or not
		try {
			/*
			 * Retrieve all the databases and check if the present database is
			 * present. If the database exists then it returns true.
			 */
			String sql;
			sql = "Show databases";
			infoLog_buffer.append("Connecting to databases " + this.fo.getDatabase_name() + "\n");
			ResultSet rs_db = db.ExecuteQuery(sql);
			while (rs_db.next()) {
				if (this.fo.getDatabase_name().equals(rs_db.getString(1)))
					flag = true;
			}
			if (flag == false) {
				/*
				 * If the database does not exists, an error log is returned
				 */
				errorLog_buffer.append("Database not found, Please check configuration\n");
				return false;
			}
			/*
			 * Connect to the FTP client with URL. If not success, appropriate
			 * error messages are logged.
			 */
			ftpClient.connect(this.fo.getFTPHOST(), this.dobj.getSftpport());
			int replyCode = ftpClient.getReplyCode();
			if (!FTPReply.isPositiveCompletion(replyCode)) {
				errorLog_buffer.append("Operation failed. Server reply code: " + replyCode + "\n");
			}
			/*
			 * Login to the FTP client with the given username and password. If
			 * not success, appropriate error messages are logged.
			 */
			boolean success = ftpClient.login(this.fo.getFTP_UNAME(), this.fo.getFTPPassword());
			if (!success) {
				System.err.println("Could not login to the FTP server provided\n");

			} else {
				/*
				 * If success, then success message is logged.
				 */
				infoLog_buffer.append("Logged into FTP server\n");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		/*
		 * This method is used to check if the mail ids provided are valid or
		 * not.
		 */
		for (String mailid : this.fo.getNotification_mailids()) {
			try {
				InternetAddress emailAddr = new InternetAddress(mailid);
				emailAddr.validate();
			} catch (AddressException ex) {
				errorLog_buffer.append("Invalid E-mail address provided\n");
				return false;
			}
		}
		return true;
	}
	/*
	 * This method is used to connect to the database and get the mapper
	 * settings and call appropriate methods. Demographics is mandatory while
	 * transactions and activity are mandatory.
	 */
	private void getMappingInfo(String mapid) {
		// TODO Auto-generated method stub
		System.out.println("In getMappingInfo");
		System.out.println(dobj.getMysqlHost() + "\t\t" + dobj.getMysqldatabase() + "\t\t" + dobj.getMysqlPwd() + "\t\t"
				+ dobj.getMysqlUname() + "\t\t" + fo.getDatabase_name());
		db.connect(db.GenerateURL(dobj.getMysqlHost(), fo.getDatabase_name()), dobj.getMysqlUname(), dobj.getMysqlPwd(),
				true);
		ResultSet rs = db.ExecuteQuery("select * from oc_mapper where id= " + mapid);
		try {
			boolean b=rs.next();
			System.out.println("thewds "+b);
			/*
			 * If mapping is present in oc_mapper, appropriate fields are sent
			 * as parameters to their corresponding methods.
			 */
			if (b) {
				this.getDemoInfo(rs.getString("demo"));
				if (this.fo.isActivity()) {
					this.getActivityInfo(rs.getString("activity"));
					this.getTransactionInfo(new JSONObject(rs.getString("transaction")));
					System.out.println("exit getMappingInfo");
				}

			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println(e);
			e.printStackTrace();
		}
	}
	/*
	 * End of getMapping Info method.
	 */
	/*
	 * This method is used to iterate over the transaction info and map the data
	 * accordingly.
	 */
	private boolean getTransactionInfo(JSONObject trans) {
		Iterator<String> it = trans.keys();
		System.out.println(trans);
		int co = 0;
		
		while (it.hasNext()) {
			String currfield = it.next();
			/*
			 * Get the current fields separated by ###
			 */
			try {
			if (currfield.contains("###")) {
				co++;
				System.out.println(currfield);
				/*
				 * Get the inner JSON array into the JSON object and update it
				 * to the hashmap.
				 */
				JSONObject colcodemap = trans.getJSONObject(currfield);
				Iterator<String> colcodemap_it = colcodemap.keys();
				while (colcodemap_it.hasNext()) {
					String curr_response = colcodemap_it.next();
					TransResObj trob = new TransResObj(curr_response, Integer.toString(co));
					this.transactionMap.put(colcodemap.get(curr_response) + "", trob);

				}

			}
			}
		catch (JSONException e) {
			// TODO Auto-generated catch block
			System.out.println(e);
			//e.printStackTrace();
		}}
		return true;
		}
	private void getActivityInfo(String activity) {
		System.out.println("In getActivityInfo");
		JSONObject jo = new JSONObject(activity);
		StringBuffer bannerqueryfn = new StringBuffer();
		StringBuffer bannerqueryjoin = new StringBuffer();
		bannerqueryjoin.append("from fact_viewability_event fv");
		StringBuffer messagequeryfn = new StringBuffer();
		StringBuffer messagequeryjoin = new StringBuffer();
		messagequeryjoin.append("from oc_email_user_message oum");

		StringBuffer webqueryfn = new StringBuffer();
		StringBuffer webqueryjoin = new StringBuffer();
		webqueryjoin.append("from fact_web_event fw");
		boolean omjoined = false;
		boolean dpgjoined = false;
		StringBuffer activityheader = new StringBuffer();

		for (String key : jo.keySet()) {
			System.out.println("-------------------------");
			activityheader.append(key + ",");
			switch (jo.getJSONObject(key).getString("activity")) {
			case "ocid": {
				bannerqueryfn.append("oc_id,");
				webqueryfn.append("oc_id,");
				messagequeryfn.append("ou.ocid,");
				messagequeryjoin.append(" join oc_users ou on ou.email_engine_id=recipientID");
				System.out.println("add ocid");
				System.out.println(
						"this is generic field in banner and pageview ocid and for messages join the oc_users table");
				break;
			}
			case "activity_time": {
				bannerqueryfn.append("FROM_UNIXTIME(initial_impression_time),");
				webqueryfn.append("FROM_UNIXTIME(request_time),");
				messagequeryfn.append("sent,");
				System.out.println("activity_time");
				break;
			}
			case "advertiser_name": {
				bannerqueryfn.append("oaa.name,");
				bannerqueryjoin.append(" join oc_ad_advertiser oaa on fv.advertiser_id=oaa.advertiser_id");
				webqueryfn.append("'',");
				messagequeryfn.append("'',");
				System.out.println("advertiser_name");
				break;
			}
			case "ad_unit_id": {
				bannerqueryfn.append("oau.name,");
				bannerqueryjoin.append(" join ac_ad_adunit oaad on fv.ad_unit_id=oaad.adunit_id");
				webqueryfn.append("'',");
				messagequeryfn.append("'',");
				System.out.println("ad_unit_id");
				break;
			}
			case "channel": {
				bannerqueryfn.append("'BA',");
				webqueryfn.append("'MS',");
				messagequeryfn.append("'EM',");
				System.out.println("channel");
				System.out.println("static for each type in message banner pageview");
				break;
			}
			case "channel_desc": {
				bannerqueryfn.append("'Banner View',");
				webqueryfn.append("'Microsite Page view',");
				messagequeryfn.append("'Email Sent Event',");
				System.out.println("channel_desc");
				System.out.println("static for each type in message banner pageview");
				break;
			}
			case "delivered": {
				messagequeryfn.append("delivered,");
				webqueryfn.append("'',");
				bannerqueryfn.append("'',");
				System.out.println("delivered");
				break;
			}
			case "email": {
				bannerqueryfn.append("ou.email,");
				webqueryfn.append("ou.email,");
				messagequeryfn.append("ou.email,");
				webqueryjoin.append(" join oc_users ou on ou.ocid=oc_id");
				bannerqueryjoin.append(" join oc_users ou on ou.ocid=oc_id");
				System.out.println("email");
				System.out.println("join from oc_users");
				break;
			}
			case "firstviewed": {
				messagequeryfn.append("firstviewed,");
				webqueryfn.append("'',");
				bannerqueryfn.append("'',");

				System.out.println("firstviewed");
				break;
			}
			case "initial_impression_time": {
				bannerqueryfn.append("FROM_UNIXTIME(initial_impression_time),");
				webqueryfn.append("'',");
				messagequeryfn.append("'',");

				System.out.println("initial_impression_time");
				break;
			}
			case "latestviewed": {
				messagequeryfn.append("latestviewed,");
				webqueryfn.append("'',");
				bannerqueryfn.append("'',");

				System.out.println("latestviewed");
				break;
			}
			case "latest_impression_time": {

				bannerqueryfn.append("FROM_UNIXTIME(latest_impression_time),");
				webqueryfn.append("'',");
				messagequeryfn.append("'',");
				System.out.println("latest_impression_time");
				break;
			}
			case "medium_id": {
				messagequeryfn.append("'',");
				webqueryfn.append("dr.medium,");
				webqueryjoin.append(" join dim_medium dm on dm.medium_id=fw.medium_id");
				bannerqueryfn.append("'',");

				System.out.println("medium_id");
				break;
			}
			case "messageid": {
				System.out.println("messageid");
				break;
			}
			case "opened": {
				messagequeryfn.append("opened,");
				webqueryfn.append("'',");
				bannerqueryfn.append("'',");

				System.out.println("opened");
				break;
			}
			case "platform": {
				bannerqueryfn.append("'NA',");
				messagequeryfn.append("'NA',");
				webqueryfn.append("dpl.platform,");
				webqueryjoin.append(" join dim_platform dpl on fw.platform_id=dpl.platform_id");
				System.out.println("platform");
				System.out.println("join form platform dim table");
				break;
			}
			case "recipientid": {
				messagequeryfn.append("recipientid,");
				webqueryfn.append("'',");
				bannerqueryfn.append("'',");

				System.out.println("recipientid");
				break;
			}
			case "referrer_id": {
				messagequeryfn.append("'',");
				webqueryfn.append("dr.site,");
				webqueryjoin.append(" join dim_referrer dr on dr.referrer_id=fw.referrer_id");
				bannerqueryfn.append("'',");
				System.out.println("referrer_id");
				break;
			}
			case "resource_details": {
				bannerqueryfn.append("dba.name,");
				webqueryfn.append("dpg.title,");
				messagequeryfn.append("om.title,");
				if (!omjoined)
					messagequeryjoin.append(" join oc_email_message om on om.messageID=oum.messageID");
				omjoined = true;
				if (!dpgjoined)
					webqueryjoin.append(" join dim_page dpg on fw.page_id=dpg.page_id");
				dpgjoined = true;
				bannerqueryjoin.append(" join oc_ad_banner dba on dba.banner_id=fv.banner_id");
				System.out.println("resource_details");
				System.out.println("for banner banner title, for message message title, for web view page title");
				break;
			}
			case "sent": {
				messagequeryfn.append("sent,");
				webqueryfn.append("'',");
				bannerqueryfn.append("'',");

				System.out.println("sent");
				break;
			}
			case "site": {
				messagequeryfn.append("'',");
				webqueryfn.append("ds.site,");
				webqueryjoin.append(" join dim_site ds on ds.site_id=fw.site_id");
				bannerqueryfn.append("'',");

				System.out.println("site");
				break;
			}
			case "subject": {
				messagequeryfn.append("om.subject,");
				if (!omjoined)
					messagequeryjoin.append(" join oc_email_message om on om.messageID=oum.messageID");
				omjoined = true;
				webqueryfn.append("'',");
				bannerqueryfn.append("'',");
				System.out.println("subject");
				break;
			}
			case "time_on_page": {
				messagequeryfn.append("'',");
				webqueryfn.append("time_on_page,");
				bannerqueryfn.append("'',");

				System.out.println("time_on_page");
				break;
			}
			case "url": {
				messagequeryfn.append("'',");
				webqueryfn.append("dpg.url,");
				if (!dpgjoined)
					webqueryjoin.append(" join dim_page dpg on fw.page_id=dpg.page_id");
				dpgjoined = true;
				bannerqueryfn.append("'',");

				System.out.println("url");
				break;
			}
			case "viewable": {
				bannerqueryfn.append("viewable,");
				webqueryfn.append("'',");
				messagequeryfn.append("'',");
				System.out.println("viewable");
				break;
			}
			default: {
				errorLog_buffer.append("Not a valid identifier\n");
				break;
			}
			}
		}
		System.out.println("here......." + activityheader);
		StringBuffer buff = new StringBuffer();
		/*
		 * To append the web query for execution
		 */
		webqueryfn.replace(webqueryfn.length() - 1, webqueryfn.length(), " ");
		buff.append(webqueryfn);
		webqueryfn.append(webqueryjoin);
		webqueryfn.append(eobj.getWebquery());
		webqueryfn.append(" group by " + buff);
		buff.delete(0, buff.length());
		/*
		 * To append the mail query for execution
		 */
		messagequeryfn.replace(messagequeryfn.length() - 1, messagequeryfn.length(), " ");
		buff.append(messagequeryfn);
		messagequeryfn.append(messagequeryjoin);
		messagequeryfn.append(eobj.getMessageQuery());
		messagequeryfn.append(" group by " + buff);
		buff.delete(0, buff.length());
		/*
		 * To append the banner query for execution
		 */
		bannerqueryfn.replace(bannerqueryfn.length() - 1, bannerqueryfn.length(), " ");
		buff.append(bannerqueryfn);
		bannerqueryfn.append(bannerqueryjoin);
		bannerqueryfn.append(eobj.getBannerquery());
		bannerqueryfn.append(" group by " + buff);
		buff.delete(0, buff.length());
		/*
		 * Write the query output to the appropriate files
		 */
		webqueryfn.append(
				" INTO OUTFILE '" + this.path + "webactivity.csv' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'");
		messagequeryfn.append(" INTO OUTFILE '" + this.path
				+ "messageactivity.csv' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'");
		bannerqueryfn.append(" INTO OUTFILE '" + this.path
				+ "banneractivity.csv' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'");
		/*
		 * Get the message queries if available and sppend with select
		 */
		if (eobj.getBannerquery() != null && eobj.getBannerquery().length() > 1) {
			eobj.setBannerquery("select " + bannerqueryfn);
		} else
			eobj.setBannerquery(null);

		if (eobj.getMessageQuery() != null && eobj.getMessageQuery().length() > 1) {
			eobj.setMessageQuery("select " + messagequeryfn);
		} else
			eobj.setMessageQuery(null);

		if (eobj.getWebquery() != null && eobj.getWebquery().length() > 1) {
			eobj.setWebquery("select " + webqueryfn);
		} else
			eobj.setWebquery(null);
		eobj.setActivityHeader(activityheader.toString());
System.out.println("1");
		System.out.println("select " + bannerqueryfn);
		System.out.println("select " + webqueryfn);System.out.println("2");
		System.out.println("select " + messagequeryfn);System.out.println("3");

	}
	/*
	 * End of getActivityInfo method
	 */
	/*
	 * This method is used to export the demographics information to the SQL
	 * server file
	 */
	private void getDemoInfo(String string) {
		// TODO Auto-generated method stub
		/*
		 * Export the csv demographics information into the sql server file.
		 */
		System.out.println("In getDemoInfo");
		this.getQuestions(new JSONObject(string));
		ExportDetails ex = new ExportDetails(this.fo.getDuration(), this.db);
		this.path = "/opt/mysqlexport/" + fo.getFTP_UNAME() + "_" + fo.getJobid() + "_" + fo.getMap_id() + "_"
				+ fo.getT_indicator() + "_" + System.currentTimeMillis();
		try {
			eobj = ex.getExportDetails(path, fo.getQues(), fo);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Path" + this.path);
		System.out.println(eobj.getQuery());
		System.out.println("Exit getDemoInfo");
	}
	/*
	 * End of getDemoInfo method.
	 */
	/*
	 * This method is used to map the questions with its corresponding responses
	 * in the demographics.
	 */
	private boolean getQuestions(JSONObject demographics) {
		try {

			JSONObject ques = demographics.getJSONObject("q");
			JSONObject responses ;
			try
			{
				responses= demographics.getJSONObject("r");
			}
			catch(JSONException w)
			{
				responses=new JSONObject();
			}
			
			Iterator<String> key_iterator = ques.keys();
			while (key_iterator.hasNext()) {
				String tpques = key_iterator.next();
				QuestionObject qu = new QuestionObject(ques.getInt(tpques), tpques.toLowerCase());
				if (responses.has(tpques)) {
					qu.setMatchType(true);
					JSONObject answers = responses.getJSONObject(tpques);
					Iterator<String> answer_iterator = answers.keys();
					while (answer_iterator.hasNext()) {
						String tp_answer = answer_iterator.next();
						qu.addAnswer((answers.get(tp_answer) + "").toLowerCase(),
								tp_answer.substring(tp_answer.lastIndexOf("#") + 1).toLowerCase());
					}
				}
				System.out.println(qu);
				this.fo.addQues(ques.getInt(tpques) + "", qu);
			}
			return true;
		} catch (Exception e) {
			errorLog_buffer.append("Error in getting questions\n");
			e.printStackTrace();

			System.out.println(demographics);
			return false;
		}
		// connect to the conection and get questions and update the job object
	}
	/*
	 * End of getQuestions method.
	 */
	@Override
	public void run() {
		/*
		 * set the time at which the job started and log the job name.
		 */
		stat.setStartTime();
		infoLog_buffer.append("Job " + fo.getJobname() + " started\n");
		/*
		 * initialize the job to make it run.
		 */
		this.initilizejob();
		/*
		 * Log the appropriate action if success or an error.
		 */
		errorLog_buffer.append("Job " + fo.getJobname() + "\n");
		infoLog_buffer.append("Processing job started at " + new Date() + "\n");
		System.out.println(this.eobj.getActivityHeader());
		System.out.println(this.eobj.getBannerquery());
		System.out.println(this.eobj.getQuery());
		System.out.println(this.eobj.getWebquery());
		System.out.println(this.eobj.getMessageQuery());
		this.processFeed();
		infoLog_buffer.append("Processing complete\n");
		this.cleanUpFeed();
		System.out.println(fo.isActivity() + "-----------------sdf----------");
		System.out.println("----------------------------");
		System.out.println(fo);
	}
	/*
	 * End of the thread run.
	 */
	/*
	 * This method is used to close connection, get the end time of the job and
	 * sent the mail to the notification mail ids and also log the appropriate
	 * messages.
	 */
	private void cleanUpFeed() {
		this.closeFTP();
		this.stat.setEndTime();
		this.mail.SendMail(this.stat, fo.getNotification_mailids(), fo.getJobname());
		infoLog_buffer.append("sending e-mail to " + fo.getNotification_mailids() + "\n");
		log_error.error(errorLog_buffer);
		log_info.info(infoLog_buffer);
		mainLog.info("Job " + fo.getJobname() + " completed");
	}
	/*
	 * This method is used to call the methods to process the
	 * demographics,activity,transactions.
	 */
	private void processFeed() {
		System.out.println("came to process feed");
		this.processDemographics();
		if (this.fo.isActivity()) {
		this.processActivity();
		this.mergeActivity();
		}
		//this.processTransactions();
		
	}
	/*
	 * This method is used to merge different activities like web,mail and
	 * banner to the file
	 */
	private void mergeActivity() {
		SimpleDateFormat df = new SimpleDateFormat("YYYYMMddHHmmss");
		try {
if(fo.isSFTP()){
	
	StringBuffer command = new StringBuffer();
	String count="";
	command.append("cat ");
	String ftpActivityPath = sftpChannel.getHome() + "/ActivityHCP_Data_" + this.fo.getPublisher() + "_"+ this.fo.getBrand_name()+"_"+this.fo.getBrand_id()+"_"+
				df.format(new Date()) + ".txt";
	String ctrlpath = sftpChannel.getHome() + "/Activity_CTRL_" + this.fo.getPublisher() + "_"+ this.fo.getBrand_name()+"_"+this.fo.getBrand_id()+"_"+
				df.format(new Date()) + ".txt";
	String filename = "ActivityHCP_Data_" + this.fo.getPublisher() + "_"+ this.fo.getBrand_name()+"_"+this.fo.getBrand_id()+"_"+
				df.format(new Date()) + ".txt";
		sftpChannel.put(new ByteArrayInputStream((this.eobj.getActivityHeader().substring(0, this.eobj.getActivityHeader().length() - 1).toString() + "\n").getBytes()),
		ftpActivityPath, ChannelSftp.APPEND);
		try {
			sftpChannel.put(sftpMySqlChannel.get(this.path + "webactivity.csv"), ftpActivityPath,
				ChannelSftp.APPEND);
			command.append(this.path + "webactivity.csv ");
		} catch (SftpException e1) {
			errorLog_buffer.append("File webactivity.csv not found\n");
			e1.printStackTrace();
		}
		try {
			sftpChannel.put(sftpMySqlChannel.get(this.path + "messageactivity.csv"), ftpActivityPath,
				ChannelSftp.APPEND);
			command.append(this.path + "messageactivity.csv ");
		} catch (SftpException e1) {

			errorLog_buffer.append("File messageactivity.csv not found\n");
			e1.printStackTrace();
		}
		try {
			sftpChannel.put(sftpMySqlChannel.get(this.path + "banneractivity.csv"), ftpActivityPath,
				ChannelSftp.APPEND);
			command.append(this.path + "banneractivity.csv ");
		} catch (SftpException e1) {
			errorLog_buffer.append("File banneractivity.csv not found\n");
			e1.printStackTrace();
		
	}
		command.append("| wc -l");
		System.out.println(command);
		Channel channelcount=this.sessionmysql.openChannel("exec");
		((ChannelExec)channelcount).setCommand(command.toString());
		channelcount.setInputStream(null);
		((ChannelExec)channelcount).setErrStream(System.err);
		InputStream in=channelcount.getInputStream();
		channelcount.connect();
		byte[] tmp=new byte[1024];
		while(true){
			System.out.println("in first while");
		  while(in.available()>0){
			  System.out.println("in second while");
		    int i=in.read(tmp, 0, 1024);
		    if(i<0)break;
		     count=new String(tmp, 0, i).trim();
		  }
		  if(channelcount.isClosed()){
		  //  System.out.println("exit-status: "+channel.getExitStatus());
		    break;
		  }
		  try{Thread.sleep(1000);}catch(Exception ee){}
		}
		channelcount.disconnect();	
		StringBuffer sb1_ctrl = new StringBuffer();
		sb1_ctrl.append(filename+"\n").append(count + "\n").append(this.getDate("MM/dd/YYYY HH:mm:ss") + "\n");
		sftpChannel.put(new ByteArrayInputStream(sb1_ctrl.toString().getBytes()), ctrlpath,ChannelSftp.APPEND);	
		System.out.println("done");
}

	else{
		StringBuffer command = new StringBuffer();
		String count="";
		command.append("cat ");
	String ctrlpath = "Activity_CTRL_" + this.fo.getPublisher() + "_"+ this.fo.getBrand_name()+"_"+this.fo.getBrand_id()+"_"+
				df.format(new Date()) + ".txt";
	String filename = "ActivityHCP_Data_" + this.fo.getPublisher() + "_"+ this.fo.getBrand_name()+"_"+this.fo.getBrand_id()+"_"+
				df.format(new Date()) + ".txt";
		String ftpActivityPath = "ActivityHCP_Data_" + this.fo.getPublisher() + "_"+ this.fo.getBrand_name()+"_"+this.fo.getBrand_id()+"_"+
				df.format(new Date()) + ".txt";

		ftpClient.appendFile(ftpActivityPath, new ByteArrayInputStream((this.eobj.getActivityHeader()
				.substring(0, this.eobj.getActivityHeader().length() - 1).toString() + "\n").getBytes()));
		try {

			ftpClient.appendFile(ftpActivityPath, sftpMySqlChannel.get(this.path + "webactivity.csv"));
			command.append(this.path + "webactivity.csv");
		} catch (SftpException e1) {
			errorLog_buffer.append("File webactivity.csv not found\n");

		}
		try {
			ftpClient.appendFile(ftpActivityPath, sftpMySqlChannel.get(this.path + "messageactivity.csv"));
			command.append(this.path + "messageactivity.csv");
		} catch (SftpException e1) {

			errorLog_buffer.append("File messageactivity.csv not found\n");

		}
		try {

			ftpClient.appendFile(ftpActivityPath, sftpMySqlChannel.get(this.path + "banneractivity.csv"));
			command.append(this.path + "banneractivity.csv ");
		} catch (SftpException e1) {
			errorLog_buffer.append("File banneractivity.csv not found\n");

}
		command.append("| wc -l");
		System.out.println(command);
		Channel channelcount=this.sessionmysql.openChannel("exec");
		((ChannelExec)channelcount).setCommand(command.toString());
		channelcount.setInputStream(null);
		((ChannelExec)channelcount).setErrStream(System.err);
		InputStream in=channelcount.getInputStream();
		channelcount.connect();
		byte[] tmp=new byte[1024];
		while(true){
			System.out.println("in first while");
		  while(in.available()>0){
			  System.out.println("in second while");
		    int i=in.read(tmp, 0, 1024);
		    if(i<0)break;
		     count=new String(tmp, 0, i).trim();
		  }
		  if(channelcount.isClosed()){
		  //  System.out.println("exit-status: "+channel.getExitStatus());
		    break;
		  }
		  try{Thread.sleep(1000);}catch(Exception ee){}
		}
		channelcount.disconnect();	
		StringBuffer sb1_ctrl = new StringBuffer();
		sb1_ctrl.append(filename+"\n").append(count + "\n").append(this.getDate("MM/dd/YYYY HH:mm:ss") + "\n");
		ftpClient.appendFile(ctrlpath, new ByteArrayInputStream(sb1_ctrl.toString().getBytes()));
}
} catch (IOException | SftpException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// TODO Auto-generated method stub
 catch (JSchException e) {
	// TODO Auto-generated catch block
	e.printStackTrace();
}
	}
	/*
	 * End of mergeActivity method.
	 */
}
