package com.cloudera.sa.Message;

import java.util.Hashtable;

public class MessageBuilder {	 		
	 		
		    public  static Message buildMessage( String type, Hashtable<Integer,DictPojo> dictHash, String delim) 
		    {		
		    	
			    if (type.equals("CEF"))
			    {
			    	  return new CEFMessage(dictHash, delim);
			    }
			    else 
			    	return null;
			     
			}
	
}
