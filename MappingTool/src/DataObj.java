import java.util.HashSet;
import java.util.List;

public class DataObj { 
//Bean for Configurations of database, FTP settings (external settings bean)
	private List<String> typeslist;
	private List<String> matchlist;
	private HashSet<Integer> dedupeset;
	private boolean same;
	private int sftpport;
	private String sshpwd; 
	private String mysql_jobs_table;
	private String mysql_questions_table;
	private String process;
	private String rootpwd;
	private int sshport;
	private String exportpath;
	private String mysqlHost;
	private String mysqlUname;
	private String mysqlPwd;
	private String mysqldatabase;
	
	public String getMysqlHost() {
		return mysqlHost;
	}
	public void setMysqlHost(String mysqlHost) {
		this.mysqlHost = mysqlHost;
	}
	public String getExportpath() {
		return exportpath;
	}
	public void setExportpath(String exportpath) {
		this.exportpath = exportpath;
	}
	public String getRootpwd() {
		return rootpwd;
	}
	public void setRootpwd(String rootpwd) {
		this.rootpwd = rootpwd;
	}
	public int getSshport() {
		return sshport;
	}
	public void setSshport(String sshport) {
		this.sshport = Integer.parseInt(sshport);
	}
	public String getProcess() {
		return process;
	}
	public void setProcess(String process) {
		this.process = process;
	}
	public String getMysql_jobs_table() {
		return mysql_jobs_table;
	}
	public void setMysql_jobs_table(String mysql_jobs_table) {
		this.mysql_jobs_table = mysql_jobs_table;
	}
	public String getMysql_questions_table() {
		return mysql_questions_table;
	}
	public void setMysql_questions_table(String mysql_questions_table) {
		this.mysql_questions_table = mysql_questions_table;
	}
	public HashSet<Integer> getDedupeSet()
	{
		return this.dedupeset;
	}
	public void addAllDedupe(HashSet<Integer> ded)
	{
		this.dedupeset.addAll(ded);
	}
	public void addDedupe(int ded)
	{
		if(this.dedupeset==null) this.dedupeset=new HashSet<>();
		this.dedupeset.add(ded);
	}
	public boolean isSame() {
		return same;
	}
	public void setSame(boolean same) {
		this.same = same;
	}
	public List<String> getMatchlist() {
		return matchlist;
	}
	public void setMatchlist(List<String> matchlist) {
		this.matchlist = matchlist;
	}
	public List<String> getTypeslist() {
		return typeslist;
	}
	public void setTypeslist(List<String> typeslist) {
		this.typeslist = typeslist;
	}
	public int getSftpport() {
		return sftpport;
	}
	public void setSftpport(String sshport) {
		this.sftpport = Integer.parseInt(sshport);
	}
	public String getSshpwd() {
		return sshpwd;
	}
	public void setSshpwd(String sshpwd) {
		this.sshpwd = sshpwd;
	}
	/**
	 * @return the mysqlUname
	 */
	public String getMysqlUname() {
		return mysqlUname;
	}
	/**
	 * @param mysqlUname the mysqlUname to set
	 */
	public void setMysqlUname(String mysqlUname) {
		this.mysqlUname = mysqlUname;
	}
	/**
	 * @return the mysqlPwd
	 */
	public String getMysqlPwd() {
		return mysqlPwd;
	}
	/**
	 * @param mysqlPwd the mysqlPwd to set
	 */
	public void setMysqlPwd(String mysqlPwd) {
		this.mysqlPwd = mysqlPwd;
	}
	/**
	 * @return the mysqldatabase
	 */
	public String getMysqldatabase() {
		return mysqldatabase;
	}
	/**
	 * @param mysqldatabase the mysqldatabase to set
	 */
	public void setMysqldatabase(String mysqldatabase) {
		this.mysqldatabase = mysqldatabase;
	}

}
