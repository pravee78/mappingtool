import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;

public class ImportInputProcessor {
	private JobObject jo;
	private DataObj dobj;
	private HashMap<Integer, String> qmap = new HashMap<>();
	private boolean mapped = false;
	private DemoQuestionObject qo;
	private InsertObject iobj=new InsertObject();
	private Connection conn;
	private OCIDGeneration ocidgen= new OCIDGeneration();
	public ImportInputProcessor(JobObject job, DataObj dobj, Connection conn) {
		super();
		this.jo = job;
		this.dobj = dobj;
		this.conn=conn;
	}

	public boolean questionIndexmap(String header) {
		ArrayList<String> ques = new ArrayList<String>();
		for(String temp:header.split(","))
		{
			ques.add(temp.trim().toLowerCase());
		}
		Set<String> keyset = jo.getQues().keySet();
		
		for (int i = 0; i < ques.size(); i++) {
			if (keyset.contains(ques.get(i).trim().toLowerCase())) {
				qmap.put(i, ques.get(i));
				if (jo.isDedupeRequired())
					if (jo.returnDedupeSet().contains(ques.get(i)))
						dobj.addDedupe(i);

			} else {
				System.out.println("there is some problem in mapping the question" + ques.get(i));
				return false;
			}
		}
		
		
		
System.out.println(dobj.getDedupeSet());
		return true;
	}

	public boolean processFile(String path, ChannelSftp sftpChannel) {

		InputStream stream;
		try {

			stream = sftpChannel.get(path);

			BufferedReader br = new BufferedReader(new InputStreamReader(stream));
			String line;
			String header = br.readLine();
			boolean now = false;
			if (!mapped) {
				this.questionIndexmap(header);
				mapped = true;
				now = true;
			}
			if (!now) {
				if (!dobj.isSame())
					questionIndexmap(header);
			}

			while ((line = br.readLine()) != null) {
				InsertObject io = this.existingCheck(line);
				System.out.println(io);
				
				this.insert(io);

			}
			stream.close();
			// read from br
		} catch (SftpException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return true;
	}

	private void insert(InsertObject io) {
		this.iobj=io;
        java.util.Date utilDate = new java.util.Date();
          utilDate=iobj.getDate();
          java.sql.Timestamp ts = new java.sql.Timestamp(utilDate.getTime());
            try {
                conn.setCatalog(jo.getDatabase_name());
                String sql = "Update table oc_users set update_time = ? where OCID = ?";
                
                PreparedStatement preparedStmt = conn.prepareStatement(sql);
                if(iobj.isExisting()==true){
                //OC_USERS update
                  preparedStmt.setTimestamp(1, ts);
                  preparedStmt.setInt(2, iobj.getOcid());
                  preparedStmt.executeUpdate();
                }
                else{
                    // Create new OCIDhash and insert
                    // YET TO UPDATE E-MAIL
                    String sql4 = "Insert into oc_users (ocid, ocid_hash, username, password,"+
                            "time_stamp, update_time,email,remarks,email_engine_id,unconfirmed,blacklisted,known,partner_system_user_id	) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                PreparedStatement preparedStmt4 = conn.prepareStatement(sql4);
                  preparedStmt4.setInt(1, iobj.getOcid());
                  preparedStmt4.setString(2, ocidgen.getHashCode(ocidgen.RandomIpGeneration(), ocidgen.UserAgentGeneration()));
                  preparedStmt4.setString(3, ocidgen.getUsername() );
                  preparedStmt4.setString(4, ocidgen.getPassword());
                  preparedStmt4.setInt(5, (int)System.currentTimeMillis());
                  preparedStmt4.setTimestamp(6, ts);
                  preparedStmt4.setString(7, io.getEmail());
                  preparedStmt4.setString(8, "");
                  preparedStmt4.setInt(9, 0);
                  preparedStmt4.setInt(10, 0);
                  preparedStmt4.setInt(11, 0);
                  preparedStmt4.setInt(12, 0);
                  preparedStmt4.setString(13, "");
                  
                  
                  preparedStmt4.executeUpdate();

                }

                for ( int key : iobj.getInsert().keySet() ) {
                    //OC_USERS_LOG update
                String sql1 = "Insert into oc_users_log (ocid, question_id, varchar_value, time_stamp,"+
                    "batch_id) values (?, ?, ?, ?, ?)";
                PreparedStatement preparedStmt1 = conn.prepareStatement(sql1);
                  preparedStmt1.setInt(1, iobj.getOcid());
                  preparedStmt1.setInt(2, key);
                  preparedStmt1.setString(3, iobj.getInsert().get(key));
                  preparedStmt1.setTimestamp(4, ts);
                  preparedStmt1.setString(5, jo.getJobid());
                  preparedStmt1.executeUpdate();
                //OC_USERS_LATEST update
                String sql2 = "IF EXISTS (SELECT * FROM oc_users_latest WHERE question_id = ? and ocid = ?)"+
                                   "BEGIN"+
                                 "UPDATE table oc_users_latest set time_stamp = ? where time_stamp < ?"+
                                 "END"+
                                 "ELSE"+
                                 "BEGIN"+
                                 "Insert into oc_users_latest (ocid, question_id, varchar_value,"+
                                 "time_stamp) values (?, ?, ?, ?)"+
                                 "END";
                PreparedStatement preparedStmt2 = conn.prepareStatement(sql2);
                  preparedStmt2.setInt(1, key);
                  preparedStmt2.setInt(2, iobj.getOcid());
                  preparedStmt2.setTimestamp(3, ts);
                  preparedStmt2.setTimestamp(4, ts);
                  preparedStmt2.setInt(5, iobj.getOcid());
                  preparedStmt2.setTimestamp(6, ts);
                  preparedStmt2.setString(7, iobj.getInsert().get(key));
                  preparedStmt2.setString(8, jo.getJobid());
                  preparedStmt2.executeUpdate();
                }
                conn.close();
                }
                catch (SQLException | NoSuchAlgorithmException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }



	}
	private InsertObject existingCheck(String line) {
		String[] data_splitted = line.split(",");
		InsertObject io = new InsertObject();
		
System.out.println("------------------------------------------------------------------------------");
	try {
		Statement s= conn.createStatement();
		//SELECT count(ocid) FROM `oc_users_latest` WHERE (ocid=8 and ((question_id=23 and varchar_value='Peter') or(question_id=15 and varchar_value='20852')))
		//sb.append("select count(ocid) from oc_users_latest where ocid ="+)
	 	for(int i:dobj.getDedupeSet())
		{
			System.out.println(qmap.get(i)+"\t"+data_splitted[i]+"\t"+jo.getQues().get(qmap.get(i)).getQid());
			io.setExisting(false);
		}
	} catch (SQLException e1) {
		// TODO Auto-generated catch block
		e1.printStackTrace();
	}

		
	 	io.setEmail(data_splitted[jo.getEmail_iden()]);
		outer: for (int i = 0; i < data_splitted.length; i++) {
			qo = jo.getQues().get(qmap.get(i));
			for (String validate : qo.getValidations()) {
				if(validate.length()>1)
				{
				String feedClassName = "validations";
				Class<?> feedClass;
				try {
					feedClass = Class.forName(feedClassName);
					Object feed = feedClass.newInstance();
					Method setNameMethod = feed.getClass().getMethod(validate, String.class);
					if (!(Boolean.valueOf(setNameMethod.invoke(feed, data_splitted[i]).toString()))) {
						System.err.println("Error in validating " + data_splitted[i] + " as it is" + validate);
						break outer;
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				}
			}
			if (qo.isMatchType()) {
				io.addInsert(jo.getQues().get(qmap.get(i)).getQid(), jo.getQues().get(qmap.get(i)).getAnswerMap().get(data_splitted[i]));
			} else
			{
				io.addInsert(jo.getQues().get(qmap.get(i)).getQid(), data_splitted[i]);
			}
				

		}
		return io;
	}

	public void processData() {

		try {
			JSch jsch = new JSch();
			Session session = jsch.getSession(jo.getFTP_UNAME(), jo.getFTPHOST(), dobj.getSshport());
			session.setPassword(jo.getFTPPassword() );
			session.setConfig("StrictHostKeyChecking", "no");
			System.out.println("Establishing Connection...");
			session.connect();
			System.out.println("Connection established.");
			System.out.println("Crating SFTP Channel.");
			ChannelSftp sftpChannel = (ChannelSftp) session.openChannel("sftp");
			sftpChannel.connect();
			System.out.println("SFTP Channel created.");
			String path = sftpChannel.getHome() + (jo.getFileinput().startsWith("/")?jo.getFileinput():("/"+jo.getFileinput()));
			SftpATTRS attrs = sftpChannel.stat(path);

			if (attrs.isDir()) {
				Vector<LsEntry> fil = sftpChannel.ls(path);
				
				for(LsEntry k:fil)
				{
					System.out.println(k.getFilename());
					if(!k.getFilename().startsWith("."))
					{
						//TODO rethink
						System.out.println(path.endsWith("/")?path+k.getFilename():path+"/"+k.getFilename());
						processFile(path.endsWith("/")?path+k.getFilename():path+"/"+k.getFilename(), sftpChannel);
					}
				}

			} else
				processFile(path, sftpChannel);
	
			sftpChannel.disconnect();
			session.disconnect();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Problem in finding the file");

		}

	}

	

}
