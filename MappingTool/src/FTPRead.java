
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import org.json.JSONException;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

public class FTPRead {
	public static void main(String args[]) throws JSchException, SftpException, SQLException, JSONException{
				try
				{
					//TODO formalize and add logging statements and test
					JSch jsch = new JSch();
					Session session = jsch.getSession("pravee", "ocdev.onecount.net");
					session.setPassword("Cherry478@");
					session.setConfig("StrictHostKeyChecking", "no");
					//System.out.println("Establishing Connection...");
					session.connect();
					//System.out.println("Connection established.");
					//System.out.println("Crating SFTP Channel.");
					//System.out.println("SFTP Channel created.");
					String path = "data_65_pravee_2016_Jun_30.csv";
					ChannelSftp sftpChannel= (ChannelSftp) session.openChannel("ftp");
					sftpChannel.connect();
				String ocid = null;
				InputStream inputStream=sftpChannel.get(path);
				
					if (inputStream == null) {
						System.err.println("File not present, recheck file/directory name");
						//return false;
					} else{
						
						String line = "";
						String cvsSplitBy = ",";
						int l=0;
						BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
						while ((line = br.readLine()) != null) {
						l++;
						}
						br.close();
						inputStream.close();
						//System.out.println("Number of lines" + l);
						String file_ocids[]=new String[l];
						//InputStream inputStream1 = ftpClient.retrieveFileStream("/www/transaction/hello.csv");
						InputStream inputStream1=sftpChannel.get(path);
						BufferedReader br1 = new BufferedReader(new InputStreamReader(inputStream1, "UTF-8"));
							System.out.println("file present");
							int i=0;
							br1.readLine();
							while ((line = br1.readLine()) != null) {
								file_ocids[i] = line.split(cvsSplitBy)[0];
						 i++;
							}
							DatabaseCall db= new DatabaseCall();
							Boolean result=db.getTransactionDetails(file_ocids);
							
							br1.close();
							inputStream1.close();
							if(result){
								System.out.println("Completed Successfully");
							}
							else
								System.out.println("Unsuccessful");
							}
					sftpChannel.disconnect();
					session.disconnect();
					
					
				}
				catch (IOException e)
				{
					e.printStackTrace();
				}
	}

			

}
