import java.util.TreeMap;

public class TransWriteObj {
TreeMap<String,String> writeValues= new TreeMap<>();
public void addValue(TransResObj tr)
{
	this.writeValues.put(tr.getColname(), tr.getResponse());
}
public String getOrder()
{
	StringBuffer sb= new StringBuffer();
	for(String v:this.writeValues.keySet())
	{
		sb.append(v+",");
	}
	return sb.substring(0,sb.length()-1);
}
public String getValue()
{

	StringBuffer sb= new StringBuffer();
	for(String v:this.writeValues.keySet())
	{
		sb.append(writeValues.get(v)+",");
	}
	return sb.substring(0,sb.length()-1);

}
}
