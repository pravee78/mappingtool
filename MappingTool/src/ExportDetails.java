
import java.sql.Date;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeSet;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.apache.log4j.Logger;

public class ExportDetails {
    private boolean type1;
    private boolean type2;
    private boolean type3;
    private boolean type4;

    //private Connection conn;
    private DBLoader db;
    String listtostring = "";
    final static Logger logger = Logger.getLogger(ExportDetails.class);
//yyyymmdd
    public ExportDetails(Date start_date, Date end_date, DBLoader db)

    {
        this.db = db;
        type1 = true;
    }

    public ExportDetails(Date start_date, DBLoader db) {
        this.db = db;
        type2 = true;
    }

    public ExportDetails(int duration, DBLoader db) {
        this.db = db;
        type3 = true;
    }

    public ExportDetails(DBLoader db) {
        this.db = db;
        type4 = true;
    }
    public String getDate(String format, java.util.Date date) {
		DateFormat dateFormat = new SimpleDateFormat(format);
		//Date date = new Date(System.currentTimeMillis());
		
		return dateFormat.format(date);

	}
    public ExportInfo getExportDetails(String path, HashMap<String, QuestionObject> ques, FeedObject fo)
            throws SQLException {
        int interval = fo.getDuration() + 1;
        path += "demo.csv";
        ExportInfo eo = new ExportInfo();
        HashSet<String> tmp = new HashSet<String>();
        StringBuffer cols = new StringBuffer();
        StringBuffer sb1 = new StringBuffer();
        StringBuffer sb = new StringBuffer();
        StringBuffer sb2 = new StringBuffer();
        HashSet<String> ques_id = new HashSet<String>();
        HashSet<String> table_names = new HashSet<String>();
        TreeSet<String> column_names = new TreeSet<String>();
        for (QuestionObject qo : ques.values()) {
            ques_id.add(Integer.toString(qo.getQid()));
        }
        String sql_getquesid = "SELECT table_name, column_name,question_id from oc_users_demographic_mapping WHERE question_id in "+listtostring(ques_id);
        ResultSet quesdetails = db.ExecuteQuery(sql_getquesid);
        while(quesdetails.next()){
            table_names.add(quesdetails.getString(1));
            column_names.add(quesdetails.getString(2));
            sb1.append(quesdetails.getString(3)+",");
 sb.append(ques.get(quesdetails.getString(3)).getThird_party_question()+",");
 cols.append(quesdetails.getString(1)).append(".").append(quesdetails.getString(2)).append(",");
        }
        System.out.println("Table names are      "+table_names);
        System.out.println("Column names are      "+column_names);

        System.out.println("cols values are--------" + cols);
        String[] tmps = table_names.toArray(new String[tmp.size()]);
        String sql;
        if (fo.getResourceid().size() > 0) {
            String ridfinl = listtostring(fo.getResourceid());
            System.out.println(ridfinl + "-----");
            //SELECT distinct(page_title_id) FROM `dim_page` WHERE url like '%www.ecardiologynews.com/md-iq-quizzes%'
            String presql="select resourceID from site_resources where RID in "+ridfinl;
            System.out.println(presql);
            ResultSet rs = this.db.ExecuteQuery(presql);
            StringBuffer sbrids= new StringBuffer();
            boolean enc=false;
            while(rs.next())
            {
                if(enc)sbrids.append(" or ");
                enc=true;
                sbrids.append("url like '%"+rs.getString(1)+"%'");

            }

            String sql1 = "SELECT distinct(dim_page.page_title_id) FROM dim_page  WHERE "+sbrids.toString();
            System.out.println("------------liking------------");
            System.out.println(sql1);
            System.out.println("---------------------------");
            sql = "SELECT dim_page.page_title_id FROM dim_page JOIN site_resources ON dim_page.url = site_resources.resourceID WHERE site_resources.RID IN "
                    + ridfinl;
             rs = this.db.ExecuteQuery(sql1);
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            System.out.println("no of cols is " + columnsNumber);

            while (rs.next()) {
                fo.getPagetitle().add(rs.getString(1));
            }
        }
        String pagefinl = null, messagefinl = null, bannerfinl = null;
        ArrayList<String> wheres = new ArrayList<>();
        Calendar c= Calendar.getInstance();
        String cur=this.getDate("yyyyMMdd",c.getTime());
        c.add(Calendar.DAY_OF_MONTH, (-1*interval));
        if (fo.getPagetitle().size() > 0) {
            pagefinl = listtostring(fo.getPagetitle());
           
            wheres.add(
                    "SELECT u.ocid, u.update_time FROM oc_users u JOIN fact_web_event w ON u.ocid=w.oc_id WHERE w.page_title_id IN "
                            + pagefinl+" and date_id < "+cur+ " and date_id > "+this.getDate("yyyyMMdd",c.getTime()));
            
            eo.setWebquery(" WHERE fw.page_title_id IN " + pagefinl+" and date_id < "+cur+ " and date_id > "+this.getDate("yyyyMMdd",c.getTime()));
        }
        if (fo.getMessage_id().size() > 0) {
            messagefinl = listtostring(fo.getMessage_id());
            System.out.println(messagefinl);
            wheres.add(
                    " SELECT u.ocid, u.update_time FROM oc_users u JOIN oc_email_user_message e ON u.email_engine_id=e.recipientID WHERE e.messageid IN "
                            + messagefinl);
            eo.setMessageQuery(" WHERE oum.messageID IN " + messagefinl+" AND DATE_SUB( CURDATE( ) , INTERVAL " + interval + " DAY ) <= ou.update_time");
        }
        if (fo.getBanner_id().size() > 0) {
            bannerfinl = listtostring(fo.getBanner_id());
            System.out.println(bannerfinl);
            wheres.add(
                    "SELECT u.ocid, u.update_time FROM oc_users u JOIN fact_viewability_event v ON u.ocid=v.oc_id WHERE v.banner_id IN "
                            + bannerfinl);
            eo.setBannerquery(" WHERE fv.banner_id IN " + bannerfinl+" and date_id < "+cur+ " and date_id > "+this.getDate("yyyyMMdd",c.getTime()));
        }
        for (int i = 0; i < tmps.length - 1; i++) {
            sb2.append(" left outer JOIN " + tmps[i + 1] + " ON a.ocid=" + tmps[i + 1] + ".ocid");
        }
        System.out.println(cols + "-------");
        String colsfin = cols.substring(0, cols.length() - 1);
        eo.setThirdp_que_order(sb.substring(0, sb.length() - 1));
        eo.setOC_que_order(sb1.substring(0, sb1.length() - 1));

        String init = "SELECT distinct(a.ocid)," + colsfin;
        if (wheres.size() > 0) {
            init += " FROM (";
            if (wheres.size() == 1)
                init += wheres.get(0);
            else if (wheres.size() == 2) {
                init += (wheres.get(0) + " UNION " + wheres.get(1));
            } else {
                init += (wheres.get(0) + " UNION " + wheres.get(1) + " UNION " + wheres.get(2));
            }

            init += ")a ";
        }
        init = init + ("left outer JOIN " + tmps[0] + " ON a.ocid=" + tmps[0] + ".ocid");
        System.out.println("initial query is--------------" + init);
        if (type1) {
            sql = init + " INTO OUTFILE '" + path
                    + "' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'";
            eo.setQuery(sql);
            System.out.println("full query is     " + sql);
            return eo;
        } else if (type2) {
            sql = init + sb2 +" INTO OUTFILE '" + path
                    + "' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'";
            eo.setQuery(sql);
            return eo;
        } else if (type3) {
            System.out.println(fo.getDuration());

            sql = init + sb2 + " INTO OUTFILE '" + path + "' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'";

            System.out.println("full query is     " + sql);
            eo.setQuery(sql);
            return eo;
        } else if (type4) {
            sql = "SELECT " + colsfin + " FROM oc_users_latest_" + tmps[0] + sb2 + " where " + " INTO OUTFILE '" + path
                    + "' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'";
            eo.setQuery(sql);
            return eo;
        } else {
            logger.error("Issue with generating query for retreiving data");
            return null;
        }
    }

    public static String listtostring(HashSet<String> hashSet) {
        if (hashSet.size() == 0)
            return "";
        String processstring = "";
        for (String s : hashSet) {
            processstring += s + ",";
        }
        processstring = processstring.substring(0, processstring.length() - 1);
        String strfinl = "(" + processstring + ")";
        return strfinl;
    }
}

