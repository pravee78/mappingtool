
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

public class TransactionObject {
	private static String transactions;
	
	public TransactionObject() {
		super();
		//this.transactions = transaction;

	}

	public String getTransaction() {
		return transactions;
	}
	public void setTransaction(String transactions) {
		this.transactions = transactions;
	}
	
	public JSONObject readJSON( String CSVPROD) throws JSONException{
	//public static void main (String args[]) throws JSONException {
		/*Map<String,Map<String, Map<String, String>>> map = 
			    new HashMap<String,Map<String, Map<String, String>>>();*/
		JSONObject import_key = new JSONObject();
		JSONObject import_key_main = new JSONObject();
		transactions="{\"CSVPROD\":{\"100\":{\"GroupID\":\"722\",\"ProductID\":\"726\"},\"102\":{\"GroupID\":\"1010\",\"ProductID\":\"1010\"}},\"CSVPRODSTAT\":{\"201\":{\"prod_status_id\":\"1\"},\"203\":{\"prod_status_id\":\"0\"}},\"CSVSUBSTYPE\":{\"new\":{\"subscribeType\":\"n\"},\"renew\":{\"subscribeType\":\"r\"},\"expire\":{\"subscribeType\":\"u\"}},\"dc\":\"CSVDATE\",\"df\":\"mm/dd/yyyy\"}";
		//String test = "{\"ProductID\":\"726\",\"GroupID\":\"722\"}";
		JSONObject jsonObj = new JSONObject(transactions);
		JSONObject CSVPRODjson = new JSONObject(CSVPROD);
		Map<String, JSONObject> map = new HashMap<String, JSONObject>();
		Iterator itr = jsonObj.keys();
		 while(itr.hasNext()) {
			 Object x=itr.next();
	         if ( jsonObj.get(x.toString()) instanceof JSONObject ) {
	        	 JSONObject j=jsonObj.getJSONObject(x.toString());
		         Iterator itr1 = j.keys();
		         while(itr1.hasNext()) {
					 Object x1=itr1.next();
			        // System.out.print(x1 + " \n");
			         JSONObject k=new JSONObject();
			         k.put("response", x1.toString().toLowerCase());
			         k.put("tp_id", x.toString().toLowerCase());
			        // System.out.println(k);
			import_key.put(x.toString(), x1);
			         if ( j.get(x1.toString()) instanceof JSONObject ) {
			        	 JSONObject j1=j.getJSONObject(x1.toString());
			        	 //map.put(k.toString().toLowerCase(), j1);   import
			        	 map.put(j1.toString().toLowerCase(),k);
			         }
			 
		         }
	         }
	     
	    }
			
		 return map.get(CSVPRODjson.toString().toLowerCase());
	}
}
