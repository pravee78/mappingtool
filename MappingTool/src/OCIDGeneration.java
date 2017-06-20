

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

public class OCIDGeneration {
	Random r =new Random();
	 public String getUsername(){
	        char[] username = "abcdefghijklmnopqrstuvwxyz1234567890".toCharArray();
	        StringBuilder sb_username = new StringBuilder();
	        Random random = new Random();
	        for (int i = 0; i < 8; i++) {
	            char u = username[random.nextInt(username.length)];
	            sb_username.append(u);
	        }
	        String usernameout = sb_username.toString();
	        return usernameout;
	    }
	    public String getPassword(){
	        char[] passwrd = "abcdefghijklmnopqrstuvwxyz1234567890!@#$%^&*".toCharArray();
	        StringBuilder sb_passwrd = new StringBuilder();
	        Random random = new Random();
	        for (int i = 0; i < 8; i++) {
	            char p = passwrd[random.nextInt(passwrd.length)];
	            sb_passwrd.append(p);
	        }
	        String passwordout = sb_passwrd.toString();
	        return passwordout;
	    } 
	public String UserAgentGeneration()
	{
		//this method is used to generate the random useragent 
		//this initally creates a array with some user agents in an array and using random class to select one of them
		String useragents[]={

"ABACHOBot",
"Firebird",
"Firefox",
"Fireweb Navigator",
"Flock",
"Fluid",
"Galaxy",
"Galeon",
"GranParadiso",
"GreenBrowser",
"Hana",
"HotJava",
"IBM WebExplorer",
"IBrowse",
"iCab",
"Iceape",
"IceCat",
"Iceweasel",
"iNet Browser",
"Internet Explorer",
"iRider",
"Iron",
"K-Meleon",
"K-Ninja",
"Kapiko",
"Kazehakase",
"Kindle Browser",
"KKman",
"KMLite",
"Konqueror",
"LeechCraft",
"Links",
"Lobo",
"lolifox",
"Lorentz",
"Lunascape",
"Lynx",
"Madfox",
"Maxthon",
"Midori",
"Minefield",
"Mozilla",
"myibrow",
"MyIE2",
"Namoroka",
"Navscape",
"NCSA_Mosaic",
"NetNewsWire",
"NetPositive",
"Netscape",
"NetSurf",
"OmniWeb",
"Opera",
"Orca",
"Oregano",
"osb-browser",
"Palemoon",
"Phoenix",
"Pogo",
"Prism",
"QtWeb Internet Browser",
"Rekonq",
"retawq",
"RockMelt",
"Safari",
"SeaMonkey",
"Shiira",
"Shiretoko",
"Sleipnir",
"SlimBrowser",
"Stainless",
"Sundance",
"Sunrise",
"surf",
"Sylera",
"Tencent Traveler",
"TenFourFox",
"theWorld Browser",
"uzbl",
"Vimprobable",
"Vonkeror",
"w3m",
"WeltweitimnetzBrowser",
"WorldWideWeb",
"Wyzo",
"Android Webkit Browser",
"BlackBerry",
"Blazer",
"Bolt",
"Browser for S60",
"Doris",
"Dorothy",
"Fennec",
"Go Browser",
"IE Mobile",
"Iris",
"Maemo Browser",
"MIB",
"Minimo",
"NetFront",
"Opera Mini",
"Opera Mobile",
"SEMC-Browser",
"Skyfire",
"TeaShark",
"Teleca-Obigo",
"uZard Web",
"Bunjalloo",
"Playstation 3",
"Playstation Portable",
"Wii",
"Nitro PDF",
"Snoopy",
"URD-MAGPIE",
"WebCapture",
"Windows-Media-Player",};
		return useragents[r.nextInt(useragents.length)];
		
	}
public String RandomIpGeneration()
{
	// this method is used to generate random IP 
	// this is using random class to select one number from 1-255 
	//this is generated 4 times and a random IP is created and returned
	StringBuffer sb= new StringBuffer();
	sb.append(r.nextInt(255)+1);
	sb.append(".");
	sb.append(r.nextInt(255)+1);
	sb.append(".");
	sb.append(r.nextInt(255)+1);
	sb.append(".");
	sb.append(r.nextInt(255)+1);
	
	return sb.toString();
}
	public String getHashCode(String ip,String useragent) throws NoSuchAlgorithmException
	{
		
		//this hashcode mentod is used to geenrate the OCID we are invoking random IP generation and random useragent generation methods and also generating random nu from 0-1000000
		// and passing them through SHA-256 code and generating the OCID

String input = ip+"."+useragent+"."+System.currentTimeMillis()+"."+r.nextInt(1000000);
    	
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        md.update(input.getBytes());
        
        byte byteData[] = md.digest();
 
        //convert the byte to hex format method 1
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < byteData.length; i++) {
         sb.append(Integer.toString((byteData[i] & 0xff) + 0x100, 16).substring(1));
        }
     
        
	return sb.toString();
	}
	

}
