import java.util.Date;

public class Stats {
private int failedrecords;
private Date startTime;
private Date endTime;
/**
 * @return the failedrecords
 */
public int getFailedrecords() {
	return failedrecords;
}
/**
 * @param failedrecords the failedrecords to set
 */
public void incFailedrecords() {
	this.failedrecords++;
}
/**
 * @return the exportedrecords
 */
public int getExportedrecords() {
	return exportedrecords;
}
/**
 * @param exportedrecords the exportedrecords to set
 */
public void incExportedrecords() {
	this.exportedrecords++;
}
/**
 * @return the startTime
 */
public Date getStartTime() {
	return startTime;
}
/**
 * @param startTime the startTime to set
 */
public void setStartTime() {
	this.startTime = new Date();
}
/**
 * @return the endTime
 */
public Date getEndTime() {
	return endTime;
}
/**
 * @param endTime the endTime to set
 */
public void setEndTime() {
	this.endTime = new Date();
}
private int exportedrecords;
}
