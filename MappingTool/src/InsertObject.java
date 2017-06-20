import java.util.Date;
import java.util.HashMap;

public class InsertObject {
private boolean isExisting;
private int ocid;
private Date recordDate;
private String email;
public String getEmail() {
	return email;
}
public void setEmail(String email) {
	this.email = email;
}
private HashMap<Integer,String> insert= new HashMap<>();
public Date getDate()
{
	return new Date();
	//return this.recordDate;
	
}
public void setDate(Date date)
{
	this.recordDate=date;
}
public boolean isExisting() {
	return isExisting;
}
public void setExisting(boolean isExisting) {
	this.isExisting = isExisting;
}
public int getOcid() {
	return ocid;
}
public void setOcid(int ocid) {
	this.ocid = ocid;
}
public HashMap<Integer, String> getInsert() {
	return insert;
}
public void setInsert(HashMap<Integer, String> insert) {
	this.insert = insert;
}
public void addInsert(Integer questionid,String answer) {
	this.insert.put(questionid, answer);
}
@Override
public String toString()
{
	return this.email+"\t"+this.isExisting+"\t"+this.ocid+"\t"+this.recordDate+"\t"+this.insert;
}
}
