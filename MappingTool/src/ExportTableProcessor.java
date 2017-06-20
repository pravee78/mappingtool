import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

public class ExportTableProcessor {
	private JobObject jo;
	private DataObj dobj;
	private Connection conn;
	private ChannelSftp sftpMySqlChannel;
	private ChannelSftp sftpChannel;
	private ExportInfo eobj;
	private ExportDetails exdet;
	public ExportTableProcessor(JobObject jo, DataObj dobj, Connection conn) {
	
		super();
		this.jo = jo;
		this.dobj = dobj;
		this.conn = conn;
		System.out.println("export initilized");

	}

	public String getDate(String format) {
		DateFormat dateFormat = new SimpleDateFormat(format);
		Date date = new Date();
		return dateFormat.format(date);

	}

	public void exportTable() {

		try {
			JSch jsch = new JSch();
			Session session = jsch.getSession("root", dobj.getMysqlHost(), dobj.getSshport());
			session.setPassword(dobj.getRootpwd());
			session.setConfig("StrictHostKeyChecking", "no");
			System.out.println("Establishing Connection...");
			session.connect();
			System.out.println("Connection established.");
			System.out.println("Crating SFTP Channel.");
			this.sftpMySqlChannel = (ChannelSftp) session.openChannel("sftp");
			sftpMySqlChannel.connect();
			System.out.println("SFTP Channel created.");
			String path = dobj.getExportpath() + jo.getClient() + "_" + jo.getJobid() + "_"
					+ this.getDate("yyyy_MMM_dd") + ".csv";
			this.saveResultsAt(path);
			this.modifyData(path);
			sftpMySqlChannel.disconnect();
			session.disconnect();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Problem in finding the file");

		}

	}



	private void modifyData(String path) {
		// TODO Auto-generated method stub
		try
		{
		InputStream stream = sftpMySqlChannel.get(path);
		BufferedReader br = new BufferedReader(new InputStreamReader(stream));
		String line;
		System.out.println(jo);
		System.out.println("----------------------------------");
		StringBuffer sb1= new StringBuffer();
		for(DemoQuestionObject qo:jo.getQues().values())
		{
			sb1.append(qo.getOncount_question()+",");
			System.out.println(qo.getQid()+"\t\t"+qo.getOncount_question()+"\t\t"+qo.getAnswerMap());
		}
		System.out.println(sb1.substring(0, sb1.length()-1));
		System.out.println("----------------------------------");
		String []order=eobj.getOC_que_order().split(",");
		//zip,address,gender,zone,last_name,telephone,first_name,email,username
		

		JSch jsch = new JSch();
		Session session = jsch.getSession(jo.getFTP_UNAME(), jo.getFTPHOST(), dobj.getSshport());
		session.setPassword(jo.getFTPPassword());
		session.setConfig("StrictHostKeyChecking", "no");
		System.out.println("Establishing Connection...");
		session.connect();
		System.out.println("Connection established.");
		System.out.println("Crating SFTP Channel.");
		this.sftpChannel = (ChannelSftp) session.openChannel("sftp");
		sftpChannel.connect();
		System.out.println("SFTP Channel created.");
		sftpChannel.getHome();
		path=sftpChannel.getHome()+(jo.getFileoutput().startsWith("/")?jo.getFileoutput():"/"+jo.getFileoutput())+(jo.getFileoutput().endsWith("/")?"data_"+jo.getJobid()+this.getDate("yyyy_MMM_dd")+".csv"
		:"/data_"+jo.getJobid()+this.getDate("yyyy_MMM_dd")+".csv");
		System.out.println(path);
		this.addHeader(eobj.getThirdp_que_order(),path);
		//"zip,address,sex,state,last name,phone,first name,email id,userid"
			while ((line = br.readLine()) != null) {
				try
				{
				sb1.delete(0, sb1.length());
				
					String [] data_split=line.split(",");
					for(int i=0;i<order.length-1;i++)
					{
						if(dobj.getMatchlist().contains(jo.getQues().get(order[i]).getType_of_question()))
						{
							data_split[i]=jo.getQues().get(order[i]).getAnswerMap().get(data_split[i]);
						}
						sb1.append(data_split[i]+",");
					}

					if(dobj.getMatchlist().contains(jo.getQues().get(order[order.length-1]).getType_of_question()))
					{
						data_split[order.length-1]=jo.getQues().get(order[order.length-1]).getAnswerMap().get(data_split[order.length-1]);
					}
					sb1.append(data_split[order.length-1]+"\n");
				
					sftpChannel.put(new ByteArrayInputStream( sb1.toString().getBytes() ), path,ChannelSftp.APPEND);

				}
				catch (ArrayIndexOutOfBoundsException e)
				{
					System.out.println("this did not have enough fields as per configuration-----"+line);
				}
		//	
			//this.insert(io);

		}
			
			sftpChannel.disconnect();
			session.disconnect();
		
		stream.close();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		} catch (SftpException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JSchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void addHeader(String header, String path) throws SftpException {
		
		sftpChannel.put(new ByteArrayInputStream( (header+"\n").getBytes() ), path,ChannelSftp.OVERWRITE);
		
	}

	private boolean saveResultsAt(String path) {
		try {
			Statement stmt = conn.createStatement();
			//this.eobj = exdet.getExportDetails(path,jo.getQues());
			System.out.println(eobj.getQuery());
			return stmt.execute(eobj.getQuery());
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}

	



}
