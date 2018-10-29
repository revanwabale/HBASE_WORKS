package blms.hbase.test.inprogress;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateSimulation {
	public static void main(String args[]) throws IOException, ParseException{
	
	    Date dNow = new Date( );
	      SimpleDateFormat ft = 
	      new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss.S");

	      System.out.println("Current Date: " + ft.format(dNow));
	      
	      String oldstring = "2011-01-18 00:00:00.433";
	      Date date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").parse(oldstring);
	      System.out.println(date);
	}
}
