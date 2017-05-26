package com.cloudera.sa.Message;


import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Hashtable;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CEFMessage extends Message {
	final  static  Pattern date_ = Pattern.compile("^(\\w\\w\\w\\s{1,2}\\d{1,2}) (\\d\\d:\\d\\d:\\d\\d) (.+) (.+)");
	
	
	
	
	public CEFMessage(Hashtable<Integer, DictPojo> dictHash, String delim) {
			super (dictHash, delim);
	}

	@Override
	protected void processHeader ()
	{
		 Matcher m = date_.matcher(msg[0]);
		 DateFormat inDf = new SimpleDateFormat("MMM dd yyyy", Locale.ENGLISH);
		 DateFormat outDateDf  = new SimpleDateFormat("yyyy-MM-dd");
		 
		 Date dt = new Date ();
		 
		 if (m.find());
		   {

			  try {
			   dt = inDf.parse(m.group(1) + " " + Calendar.getInstance().get(Calendar.YEAR));
			   
			   //  Assumption is all messages will have: date time host version in the first field   
			   msgHash_.put(dictHash_.get(1).getName(), outDateDf.format(dt) );  // Date 
			   msgHash_.put(dictHash_.get(2).getName(), m.group(2));  // Time
			   msgHash_.put(dictHash_.get(3).getName(), m.group(3));  // Host 
			   msgHash_.put(dictHash_.get(4).getName(), m.group(4));  // Version
			  }
			  catch (Exception e)
			  {
				  System.out.println(e.getMessage());
			  }
		   }
		   
		   // Process header, ignore first element and last
		   for (int i = 0; i < msg.length-2; i++)
		   {
			   msgHash_.put(dictHash_.get(i+5).getName(), msg[i+1]);
		   }
	}
	
	@Override
	protected void processBody ()
	{
		int lastwht = 0;
		int valuestart = 0;
		int lasteq = 0;
		String key = "";
		String value = "";
		String line = msg[msg.length-1];
		try {
		// Iterate through key value pairs and split them up based on = and key preceding space
		for (int i = 0; i<line.length(); i++)
		{
			if (line.charAt(i) == '=' && line.charAt(i-1) != '\\')
			{
			
				lasteq = i;
				
				// Extract the key for first element only
				if (lastwht == 0) 
				{
					key = line.substring(++lastwht, lasteq--);
				}
				else 
				{
					value = line.substring(valuestart, lastwht);
					msgHash_.put(key,value); 
					key = line.substring(++lastwht, lasteq--);
				}
				valuestart = ++i;
			}
			if (line.charAt(i) == ' ')  
			{
				lastwht = i;
			}
		}
		if (valuestart > line.length())
		{
			valuestart = line.length();
		}
		// Extract value for last element only
		value = line.substring(valuestart,line.length());
		msgHash_.put(key,value);
		}catch (Exception e)
		{
			logger.error("Could not process message: " + Arrays.toString(msg));
		}
		
	}
	
	
}
