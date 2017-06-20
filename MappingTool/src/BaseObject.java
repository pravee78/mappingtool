import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class BaseObject {
// Generic Bean for feed,import and export
private HashMap<String,QuestionObject> demoques = new HashMap<String,QuestionObject>();
//object for transaction Questions
private String jobid;
private List<String> notification_mailids;
private Timestamp timestamp_submitted;
private String database_name;
private String FTPHOST;
private String FTP_UNAME;
private String FTPPassword;
private String publisher;
public String getPublisher() {
	return publisher;
}

public void setPublisher(String publisher) {
	this.publisher = publisher;
}

public String getBrand_name() {
	return brand_name;
}

public void setBrand_name(String brand_name) {
	this.brand_name = brand_name;
}

public String getBrand_id() {
	return brand_id;
}

public void setBrand_id(String brand_id) {
	this.brand_id = brand_id;
}

public String getCampaign_id() {
	return campaign_id;
}

public void setCampaign_id(String campaign_id) {
	this.campaign_id = campaign_id;
}

private String brand_name;
private String brand_id;
private String campaign_id;



public String getJobid() {
	return jobid;
}

public void setJobid(String jobid) {
	this.jobid = jobid;
}


public List<String> getNotification_mailids() {
	return notification_mailids;
}

public void setNotification_mailids(String notification_mailids) {
	this.notification_mailids = Arrays.asList(notification_mailids.split(","));
}

public Timestamp getTimestamp_submitted() {
	return timestamp_submitted;
}

public void setTimestamp_submitted(Timestamp timestamp_submitted) {
	this.timestamp_submitted = timestamp_submitted;
}



public String getDatabase_name() {
	return database_name;
}

public void setDatabase_name(String database_name) {
	this.database_name = database_name;
}


public HashMap<String,QuestionObject> getQues() {
	return demoques;
}

public void addAll(HashMap<String,QuestionObject> ques) {
	this.demoques.putAll(ques);
}

public void addQues(String key,QuestionObject ques) {
	this.demoques.put(key,ques);
}

public String getFTPHOST() {
	return FTPHOST;
}

public void setFTPHOST(String fTPHOST) {
	FTPHOST = fTPHOST;
}

public String getFTP_UNAME() {
	return FTP_UNAME;
}

public void setFTP_UNAME(String fTP_UNAME) {
	FTP_UNAME = fTP_UNAME;
}

@Override
public String toString()
{
	return  this.jobid+" "+  this.notification_mailids+"	"+ this.timestamp_submitted+"	"+ this.database_name;
}

public String getFTPPassword() {
	return FTPPassword;
}

public void setFTPPassword(String fTPPassword) {
	FTPPassword = fTPPassword;
}
}
