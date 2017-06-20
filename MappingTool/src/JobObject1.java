import java.util.Arrays;
import java.util.HashSet;

public class JobObject1 {
	private HashSet<String> dedupeques= new HashSet<>();
	private boolean isImport;
	private boolean dedupe;
	private int email_iden;

	public boolean isDedupeRequired()
	{
		return this.dedupe;
	}
	public void setDedupe(boolean dedupe)
	{
		this.dedupe=dedupe;
	}
	public boolean isImport()
	{
		return isImport;
	}
	public void setIsImport(boolean isImport)
	{
		this.isImport=isImport;
	}

	public HashSet<String> returnDedupeSet()
	{
		return this.dedupeques;
	}

	public void setDedupeSet(String list)
	{
		this.dedupeques.addAll(Arrays.asList(list.split(",")));
	}
	public int getEmail_iden() {
		return email_iden;
	}
	public void setEmail_iden(int email_iden) {
		this.email_iden = email_iden;
	}
}
