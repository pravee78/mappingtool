import java.util.HashMap;

public class QuestionObject {
	@Override
	public boolean equals(Object other)
	{
		if(other instanceof QuestionObject)
		{
			QuestionObject oth=(QuestionObject) other;
			if (oth.getOncount_question().equals(this.oncount_question) && oth.getThird_party_question().equals(this.third_party_question)) return true;
			else return false;
		}
		return false;
		
	}

private String oncount_question ;
private String third_party_question ;
private String[] validations ;

private boolean matchType;
private int qid;
private boolean dedupeQues;
private HashMap<String,String> AnswerMap = new HashMap<>();
public boolean isDedupeQues()
{
	return this.dedupeQues;
}
public int getQid() {
	return qid;
}

public void setQid(int qid) {
	this.qid = qid;
}
public boolean isMatchType() {
	return matchType;
}
public void setMatchType(boolean matchType) {
	this.matchType = matchType;
}
public void addAnswer(String key,String value)
{
	this.AnswerMap.put(key, value);
}
public void addAll(HashMap<String,String> existing)
{
	this.AnswerMap.putAll(existing);
}
public void addFromArrays(String [] keys,String [] values)
{
	for (int i = 0; i < keys.length; i++) {
		this.AnswerMap.put(keys[i], values[i]);
	}
}
public HashMap<String,String> getAnswerMap()
{
	return this.AnswerMap;
}
public String getOncount_question() {
	return oncount_question;
}
public void setOncount_question(String oncount_question) {
	this.oncount_question = oncount_question;
}
public String getThird_party_question() {
	return third_party_question;
}
public void setThird_party_question(String third_party_question) {
	this.third_party_question = third_party_question;
}


public String[] getValidations() {
	return validations;
}
public void setValidations(String[] validations) {
	this.validations = validations;
}
public QuestionObject(int oncount_qid, String third_party_question) {
	super();
	this.qid = oncount_qid;
	this.third_party_question = third_party_question;

}
@Override
public String toString()
{
	return this.qid+"\t"+this.third_party_question+"\t"+this.dedupeQues+"\t"+this.matchType+"\t\t"+this.AnswerMap;
}
}
