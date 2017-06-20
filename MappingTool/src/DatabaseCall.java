
import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.json.JSONException;
import org.json.JSONObject;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

public class DatabaseCall {
public boolean getTransactionDetails(String[] ocids) throws SQLException, JSONException, SftpException, JSchException{
	//public static void main(String args[]) throws SQLException, JSONException{
		//String ocids[]={"0","54","3"};
	MetaDataLoader me = new MetaDataLoader();
	//TODO make this come from command line use get opts n java
		Connection con = me.getConnectionFromProp("src/Properties.conf");
		Statement stmt;
			stmt = con.createStatement();
			//FTPWrite fw= new FTPWrite();
			JSch jsch = new JSch();
			Session session = jsch.getSession("praveena", "ocdev.onecount.net");
			session.setPassword("Cherry478@");
			session.setConfig("StrictHostKeyChecking", "no");
			//System.out.println("Establishing Connection...");
			session.connect();
			//System.out.println("Connection established.");
			//System.out.println("Crating SFTP Channel.");
			//System.out.println("SFTP Channel created.");
			String path = "transacresult.csv";
			ChannelSftp sftpChannel= (ChannelSftp) session.openChannel("sftp");
			sftpChannel.connect();
			//System.out.println("Length" + ocids.length );
			for(int i=0;i<ocids.length;i++){
				String sql = "select * from gcn_transactionlog where UserID="+ ocids[i];
				System.out.println(ocids[i]);
						ResultSet rs = stmt.executeQuery(sql);
						while (rs.next()) {
							JsonkeyValueBean js= new JsonkeyValueBean(rs.getString("ProductID"),rs.getString("GroupID"),rs.getString("product_status_id"),rs.getString("subscribeType"));
							TransactionObject tr = new TransactionObject();
							String x="{"+ "\"ProductID\":\"" +js.getOc_field() + "\",\"GroupID\":\"" + js.getOc_value() + "\"}";
							String y="{"+ "\"prod_status_id\":\"" +js.getProd_status_id() + "\"}";
							String z="{"+ "\"subscribeType\":\"" + js.getSubscriberType() + "\"}";
							//System.out.println(y);
							//System.out.println(z);
							JSONObject js1 =tr.readJSON(x);
							JSONObject js2 =tr.readJSON(y);
							JSONObject js3 =tr.readJSON(z);
							sftpChannel.put(new ByteArrayInputStream(ocids[i].concat(",").getBytes() ), path,ChannelSftp.APPEND);
							//System.out.println( "ocid " + ocids[i] +" " + js.getOc_field() + " " + js.getOc_value() + "\n");
							if(js1!=null){
							//System.out.println(js1.get("response").toString());
							//fw.WriteToFile(js1.get("response").toString() + "\t");
							sftpChannel.put(new ByteArrayInputStream(js1.get("response").toString().concat(",").getBytes() ), path,ChannelSftp.APPEND);
							}
							else{
								//fw.WriteToFile("Not available\t");
								sftpChannel.put(new ByteArrayInputStream("NA,".getBytes() ), path,ChannelSftp.APPEND);
							}
							if(js2!=null){
								//System.out.println(js2.toString());
								//fw.WriteToFile(js2.get("response").toString()+ "\t");
								sftpChannel.put(new ByteArrayInputStream(js2.get("response").toString().concat(",").getBytes() ), path,ChannelSftp.APPEND);
								}
							else{
								//fw.WriteToFile("Not available\t");
								sftpChannel.put(new ByteArrayInputStream("NA,".getBytes() ), path,ChannelSftp.APPEND);
							}
							if(js3!=null){
								//System.out.println(js3.toString());
								//fw.WriteToFile(js3.get("response").toString()+"\t");
								sftpChannel.put(new ByteArrayInputStream(js3.get("response").toString().getBytes() ), path,ChannelSftp.APPEND);
								}
							else{
								//fw.WriteToFile("Not available\t");
								sftpChannel.put(new ByteArrayInputStream("NA,".getBytes() ), path,ChannelSftp.APPEND);
							}
							sftpChannel.put(new ByteArrayInputStream("\n".getBytes() ), path,ChannelSftp.APPEND);
						}
						
			}
			sftpChannel.disconnect();
			session.disconnect();
			return true;		
			}
			


}

