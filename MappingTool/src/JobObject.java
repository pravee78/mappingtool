
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class JobObject {
	private HashMap<String,DemoQuestionObject> ques = new HashMap<String,DemoQuestionObject>();
	private HashSet<String> dedupeques= new HashSet<>();
	private String jobid;
	private String submitted_by;
	private String validationkey;
	private String jobname;
	private List<String> notification_mailids;
	private Timestamp timestamp_submitted;
	private String fileinput;
	private String fileoutput;
	private String client;
	private String table_name;
	private String database_name;
	private String FTPHOST;
	private String FTP_UNAME;
 	private boolean isImport;
	private boolean dedupe;
	private String FTPPassword;
	private int email_iden;
	private Date start_date;
	private Date end_date;
	private int duration;
	
	public Date getStart_date() {
		return start_date;
	}


	public void setStart_date(Date start_date) {
		this.start_date = start_date;
	}


	public Date getEnd_date() {
		return end_date;
	}


	public void setEnd_date(Date end_date) {
		this.end_date = end_date;
	}


	public int getDuration() {
		return duration;
	}


	public void setDuration(int duration) {
		this.duration = duration;
	}


	public int getEmail_iden() {
		return email_iden;
	}


	public void setEmail_iden(int email_iden) {
		this.email_iden = email_iden;
	}


	public String getFTPPassword() {
		return FTPPassword;
	}


	public void setFTPPassword(String fTPPassword) {
		FTPPassword = fTPPassword;
	}


	public JobObject( String jobid, String submitted_by, String validationkey,
			String jobname, String notification_mailids, Timestamp timestamp_submitted, String fileinput,
			String fileoutput, String client, String table_name, String database_name, String FTPHOST, String FTP_UNAME,String FTPPassword,boolean dedupe,boolean isImport) {
		super();
		
		this.jobid = jobid;
		this.submitted_by = submitted_by;
		this.validationkey = validationkey;
		this.jobname = jobname;
		this.notification_mailids = Arrays.asList(notification_mailids.split(","));
		this.timestamp_submitted = timestamp_submitted;
		this.fileinput = fileinput;
		this.fileoutput = fileoutput;
		this.client = client;
		this.table_name = table_name;
		this.database_name = database_name;
		this.FTP_UNAME = FTP_UNAME;
		this.FTPHOST = FTPHOST;
		this.dedupe=dedupe;
		this.isImport=isImport;
		this.FTPPassword=FTPPassword;
	}

	
	public boolean isDedupeRequired()
	{
		return this.dedupe;
	}
	public void setDedupe(boolean dedupe)
	{
		this.dedupe=dedupe;
	}
	public boolean isImport()
	{
		return isImport;
	}
	public void setIsImport(boolean isImport)
	{
		this.isImport=isImport;
	}

	
	public String getJobid() {
		return jobid;
	}

	public void setJobid(String jobid) {
		this.jobid = jobid;
	}

	public String getSubmitted_by() {
		return submitted_by;
	}

	public void setSubmitted_by(String submitted_by) {
		this.submitted_by = submitted_by;
	}

	public String getValidationkey() {
		return validationkey;
	}

	public void setValidationkey(String validationkey) {
		this.validationkey = validationkey;
	}

	public String getJobname() {
		return jobname;
	}

	public void setJobname(String jobname) {
		this.jobname = jobname;
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

	public String getFileinput() {
		return fileinput;
	}

	public void setFileinput(String fileinput) {
		this.fileinput = fileinput;
	}

	public String getFileoutput() {
		return fileoutput;
	}

	public void setFileoutput(String fileoutput) {
		this.fileoutput = fileoutput;
	}

	public String getClient() {
		return client;
	}

	public void setClient(String client) {
		this.client = client;
	}

	public String getTable_name() {
		return table_name;
	}

	public void setTable_name(String table_name) {
		this.table_name = table_name;
	}

	public String getDatabase_name() {
		return database_name;
	}

	public void setDatabase_name(String database_name) {
		this.database_name = database_name;
	}
	

	public HashMap<String,DemoQuestionObject> getQues() {
		return ques;
	}

	public void addAll(HashMap<String,DemoQuestionObject> ques) {
		this.ques.putAll(ques);
	}
	
	public void addQues(String key,DemoQuestionObject ques) {
		this.ques.put(key,ques);
	}

	public String getFTPHOST() {
		return FTPHOST;
	}

	public void setFTPHOST(String fTPHOST) {
		FTPHOST = fTPHOST;
	}
	public HashSet<String> returnDedupeSet()
	{
		return this.dedupeques;
	}
	
	public void setDedupeSet(String list)
	{
		this.dedupeques.addAll(Arrays.asList(list.split(",")));
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
		return  this.jobid+" "+ this.submitted_by+"	"+ this.validationkey+"	"+
				this.jobname+"	"+ this.notification_mailids+"	"+ this.timestamp_submitted+"	"+ this.fileinput+"	"+
				this.fileoutput+"	"+ this.client+"	"+ this.table_name+"	"+ this.database_name;
	}
	
}
