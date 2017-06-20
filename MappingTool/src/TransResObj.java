
public class TransResObj {
private String response;
private String colname;
public TransResObj(String curr_response, String substring) {
	// TODO Auto-generated constructor stub
	this.colname=substring;
	this.response=curr_response;
}
/**
 * @return the response
 */
public String getResponse() {
	return response;
}
/**
 * @param response the response to set
 */
public void setResponse(String response) {
	this.response = response;
}
/**
 * @return the colname
 */
public String getColname() {
	return colname;
}
/**
 * @param colname the colname to set
 */
public void setColname(String colname) {
	this.colname = colname;
}
@Override
public String toString()
{
	return this.colname+"\t\t"+this.response;
}
}
