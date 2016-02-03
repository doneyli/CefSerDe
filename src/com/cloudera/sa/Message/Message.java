package com.cloudera.sa.Message;

import java.util.Hashtable;

import org.apache.log4j.Logger;


public abstract  class  Message {
	protected static final Logger logger = Logger.getLogger(Message.class);
	
	final private String printDelim_ = "|";
	public Hashtable<String,String> msgHash_;
	protected Hashtable<Integer,DictPojo> dictHash_;
	protected String delimiter_;
	protected String [] msg;
	
	protected  abstract void processHeader();
	protected  abstract void processBody();
	
	public void resetMsg (String line)
	{
		msg = line.split(delimiter_);
		for (int i = 1; i<dictHash_.size()+1; i++)
		{
			msgHash_.put(dictHash_.get(i).getName(), "");				
		}
	}
	
	protected Message( Hashtable<Integer,DictPojo> dictHash, String delim)
	{
		this.dictHash_ = new Hashtable<Integer,DictPojo> (dictHash);
		this.msgHash_ = new Hashtable<String,String> ();
		this.delimiter_ = delim;
	}
	
	
	public void processMsg ()
	{
		processHeader();
		processBody(); 
	}
	
	public Hashtable<String,String> getHeader()
	{
		Hashtable<String,String> header = new Hashtable<String,String> ();
		for (Integer i : dictHash_.keySet())
		{
			if(dictHash_.get(i).getHeader())
			{
				String key = dictHash_.get(i).getName();
				header.put(key, msgHash_.get(key));
			}
		}
		return header;
		
	}
	
	public String toString()
	{
		StringBuffer buffer = new StringBuffer();
		for (int i = 1; i<dictHash_.size()+1; i++)
		{
			buffer.append(msgHash_.get(dictHash_.get(i).getName()));
			buffer.append(printDelim_);
		}
		
		// Strip last delimiter 
		buffer.deleteCharAt(buffer.length()-1);   
		return buffer.toString();
	}
	

}
	

