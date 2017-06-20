import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.HashSet;

import org.json.JSONObject;

public class FeedObject extends BaseObject {
	private Date start_date;
	private Date end_date;
	private int duration;
	private String map_id;
	private String frequency;
	private String t_indicator;
	private HashSet<String> pagetitle;
	private HashSet<String> resourceid;
	private HashSet<String> banner_id;
	private HashSet<String> message_id;
	private HashMap<String,QuestionObject> ques = new HashMap<String,QuestionObject>();
	private boolean control;
	private boolean transaction;
	private boolean demo;
	private boolean activity;
	private String jobname;
	private boolean isSFTP;
	
	
	
	/**
	 * @return the isSFTP
	 */
	public boolean isSFTP() {
		return isSFTP;
	}

	/**
	 * @param isSFTP the isSFTP to set
	 */
	public void setSFTP(boolean isSFTP) {
		this.isSFTP = isSFTP;
	}

	/**
	 * @return the activity
	 */
	public boolean isActivity() {
		return activity;
	}

	/**
	 * @param activity the activity to set
	 */
	public void setActivity(boolean activity) {
		this.activity = activity;
	}

	public HashMap<String,QuestionObject> getQues() {
		return ques;
	}

	public void addAll(HashMap<String,QuestionObject> ques) {
		this.ques.putAll(ques);
	}
	
	public void addQues(String key,QuestionObject ques) {
		this.ques.put(key,ques);
	}
	/**
	 * @return the start_date
	 */
	public Date getStart_date() {
		return start_date;
	}
	/**
	 * @param start_date the start_date to set
	 */
	public void setStart_date(Date start_date) {
		this.start_date = start_date;
	}
	/**
	 * @return the end_date
	 */
	public Date getEnd_date() {
		return end_date;
	}
	/**
	 * @param end_date the end_date to set
	 */
	public void setEnd_date(Date end_date) {
		this.end_date = end_date;
	}
	/**
	 * @return the duration
	 */
	public int getDuration() {
		return duration;
	}
	/**
	 * @param duration the duration to set
	 */
	public void setDuration(int duration) {
		this.duration = duration;
	}
	/**
	 * @return the map_id
	 */
	public String getMap_id() {
		return map_id;
	}
	/**
	 * @param map_id the map_id to set
	 */
	public void setMap_id(String map_id) {
		this.map_id = map_id;
	}
	/**
	 * @return the frequency
	 */
	public String getFrequency() {
		return frequency;
	}
	/**
	 * @param frequency the frequency to set
	 */
	public void setFrequency(String frequency) {
		this.frequency = frequency;
	}
	/**
	 * @return the t_indicator
	 */
	public String getT_indicator() {
		return t_indicator;
	}
	/**
	 * @param t_indicator the t_indicator to set
	 */
	public void setT_indicator(String t_indicator) {
		this.t_indicator = t_indicator;
	}
	/**
	 * @return the pagetitle
	 */
	public HashSet<String> getPagetitle() {
		return pagetitle;
	}
	/**
	 * @param pagetitle the pagetitle to set
	 */
	public void setPagetitle(HashSet<String> pagetitle) {
		this.pagetitle = pagetitle;
	}
	/**
	 * @return the resourceid
	 */
	public HashSet<String> getResourceid() {
		return resourceid;
	}
	/**
	 * @param resourceid the resourceid to set
	 */
	public void setResourceid(HashSet<String> resourceid) {
		this.resourceid = resourceid;
	}
	/**
	 * @return the banner_id
	 */
	public HashSet<String> getBanner_id() {
		return banner_id;
	}
	/**
	 * @param banner_id the banner_id to set
	 */
	public void setBanner_id(HashSet<String> banner_id) {
		this.banner_id = banner_id;
	}
	/**
	 * @return the message_id
	 */
	public HashSet<String> getMessage_id() {
		return message_id;
	}
	/**
	 * @param message_id the message_id to set
	 */
	public void setMessage_id(HashSet<String> message_id) {
		this.message_id = message_id;
	}
	
	public FeedObject(JSONObject analytics_filter,JSONObject feed_setting,String timestamp,int jobid,String database)
	{
		this.pagetitle= new HashSet<>();
		this.resourceid=new HashSet<>();
		this.banner_id= new HashSet<>();
		this.message_id= new HashSet<>();
		System.out.println(analytics_filter+"\n"+feed_setting+"\n"+timestamp+"\n"+jobid);
		for(Object ptarray:analytics_filter.getJSONArray("pt"))
		{
			this.pagetitle.add(ptarray+"");
		}
		for(Object ptarray:analytics_filter.getJSONArray("r"))
		{
			this.resourceid.add(ptarray+"");
		}
		for(Object ptarray:analytics_filter.getJSONArray("b"))
		{
			this.banner_id.add(ptarray+"");
		}
		for(Object ptarray:analytics_filter.getJSONArray("e"))
		{
			this.message_id.add(ptarray+"");
		}
		this.map_id=feed_setting.getString("m");
		this.frequency=feed_setting.getString("fs");
		this.t_indicator=feed_setting.getString("fts");
		switch((String)feed_setting.get("rs"))
		{
			case "previousmonth" :
					this.duration=30;break;
			case "7days":
					this.duration=7;break;
			case "enterdays":
					this.duration=Integer.parseInt(feed_setting.get("r")+"");break;
			default: this.duration=0;
					System.err.println("Invalid specifier found for rs please check configuration");break;
		}
		this.setFTPHOST(feed_setting.getString("furl"));
		this.setFTP_UNAME(feed_setting.getString("fun"));
		this.setFTPPassword(feed_setting.getString("fpw"));
		this.setNotification_mailids(feed_setting.getString("ne"));
		this.setBrand_id(feed_setting.getString("brandid"));
		this.setBrand_name(feed_setting.getString("brandname"));
		this.setPublisher(feed_setting.getString("publisher"));
		this.setCampaign_id(feed_setting.getString("campaignid"));
		if(feed_setting.getString("typeofftp").equals("sftp")){
			this.isSFTP=true;
		}
		else{
			this.isSFTP=false;}
		for(Object ptarray:feed_setting.getJSONArray("cf"))
			{
				if((ptarray+"").equals("control"))this.setControl(true);
				else if((ptarray+"").equals("activity"))
					{
					this.setTransaction(true);
					this.setActivity(true);
					}
				else if((ptarray+"").equals("contact"))this.setDemo(true);
			}
		this.setTimestamp_submitted(new Timestamp(Long.parseLong(timestamp)));
		this.setDatabase_name(database);
		this.setJobid(jobid+"");
		
	}
	
	/**
	 * @return the control
	 */
	public boolean isControl() {
		return control;
	}
	/**
	 * @param control the control to set
	 */
	public void setControl(boolean control) {
		this.control = control;
	}
	/**
	 * @return the transaction
	 */
	public boolean isTransaction() {
		return transaction;
	}
	/**
	 * @param transaction the transaction to set
	 */
	public void setTransaction(boolean transaction) {
		this.transaction = transaction;
	}
	/**
	 * @return the demo
	 */
	public boolean isDemo() {
		return demo;
	}
	/**
	 * @param demo the demo to set
	 */
	public void setDemo(boolean demo) {
		this.demo = demo;
	}
	@Override
	public String toString()
	{
		return this.getJobid()+"\t"+this.getDatabase_name()+"\t"+this.duration+"\t"+this.frequency+"\t"+this.map_id+"\t"+this.t_indicator;
		
	}

	/**
	 * @return the jobname
	 */
	public String getJobname() {
		return jobname;
	}

	/**
	 * @param jobname the jobname to set
	 */
	public void setJobname(String jobname) {
		this.jobname = jobname;
	}
	
}
