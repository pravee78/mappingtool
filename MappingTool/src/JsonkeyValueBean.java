

public class JsonkeyValueBean {
	
	private String oc_field; 
	private String oc_value;
	private String prod_status_id;
	private String subscriberType;
	
	public JsonkeyValueBean(String oc_field, String oc_value, String prod_status_id,String subscriberType) {
		// TODO Auto-generated constructor stub
super();
		
		this.oc_field = oc_field;
		this.oc_value = oc_value;
		this.prod_status_id = prod_status_id;
		this.subscriberType = subscriberType;
	}
	public String getOc_field() {
		return oc_field;
	}
	public String getProd_status_id() {
		return prod_status_id;
	}
	public void setProd_status_id(String prod_status_id) {
		this.prod_status_id = prod_status_id;
	}
	public String getSubscriberType() {
		return subscriberType;
	}
	public void setSubscriberType(String subscriberType) {
		this.subscriberType = subscriberType;
	}
	public void setOc_field(String oc_field) {
		this.oc_field = oc_field;
	}
	public String getOc_value() {
		return oc_value;
	}
	public void setOc_value(String oc_value) {
		this.oc_value = oc_value;
	} 
}
