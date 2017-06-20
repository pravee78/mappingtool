import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.json.JSONObject;

public class FeedEntryPoint {
private Connection initilizationconnection;
public static String PropFileName;
private DBLoader db;
private final String query = "select * from oc_feed_export where job_status='active'";

public FeedEntryPoint() {
	/*
	 * Initialize the constructor
	 */
	// TODO Auto-generated constructor stub
}
/*
 * Check the active feed from the database and check if the feed job has to run now.
 * Call the feed processor if the job has to run at this time.
 */
	private void initilizeFeeds()
	{
		final Logger mainLog = Logger.getLogger("mainLogger");
		/*
		 * retrieve the tables from the properties configuration file and load the metadata.
		 */
		MetaDataLoader meta= new MetaDataLoader();
		/*
		 * To check the current time, compare it with the time received from the feed and execute if both are equal.
		 */
		FeedUtil util= new FeedUtil();
		this.initilizationconnection=meta.getConnectionFromProp(PropFileName);
		mainLog.info("Reading connection information from property file "+PropFileName);
		/*
		 * Initialize the database connection with the details obtained through the metadata loader
		 */
		this.db= new DBLoader(meta.dobj.getMysqldatabase(), meta.dobj.getMysqlHost(), meta.dobj.getMysqlUname(),
				meta.dobj.getMysqlPwd());
		mainLog.info("Loading active feeds from Database using query "+query);
		/*
		 * Get the active jobs present in the database
		 */
		ResultSet rs = this.db.ExecuteQuery(query, this.initilizationconnection);
try {
			while (rs.next()) {
				/*
				 * Check if the job has to run now and if yes call the processor class
				 */
				if(util.runcheck(rs.getString("feed_setting")))
				{
					FeedProcessor fp=new FeedProcessor(new FeedObject(new JSONObject(rs.getString("analytics_filter")),
							new JSONObject(rs.getString("feed_setting")), rs.getString("timestamp"), rs.getInt("jobid"),
							rs.getString("database")),meta.dobj,rs.getString("job_name"));
					System.out.println("a new job "+rs.getString("job_name")+"started ");
					Thread feed=new Thread(fp);
					feed.start();
					mainLog.info("Job "+rs.getString("job_name")+" started");	
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
			mainLog.info("Unable to make database connection");
		}
	}
	/*
	 * End of initilizeFeeds method
	 */
	public static void main(String[] args) {
		/*
		 * Entry point for the feed processing.
		 */
		FeedEntryPoint fep= new FeedEntryPoint();
		
		PropFileName=args.length==0?"src/Properties.conf":args[0];
		fep.initilizeFeeds(); 
	}

}
