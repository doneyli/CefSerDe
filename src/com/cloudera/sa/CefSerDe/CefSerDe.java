package com.cloudera.sa.CefSerDe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.cloudera.sa.Message.DictPojo;
import com.cloudera.sa.Message.Message;
import com.cloudera.sa.Message.MessageBuilder;

public class CefSerDe implements SerDe {
	final static String defaultDelim_ = "\\";
	final static String defaultNumHeaders_ = "0";
	final static String defaultKeyNames_ = "";
	
	private ObjectInspector inspector_;
	private int numCols_;
	private List<String> row_;
	private List<String> columnNames_ ;
	private List<TypeInfo> columnTypes_;
	private String separator_;
	private int numHeader_;
	private Hashtable<Integer, DictPojo> cefDict_;
	private Message cf_;
	private String [] keyNames_;
	
	@Override
	public void initialize(final Configuration conf, final Properties tbl)
			throws SerDeException {
	
	    columnNames_ = Arrays.asList(tbl.getProperty(serdeConstants.LIST_COLUMNS).split(","));
	    
	    // Leaving this one here just for an example of how to get the types
	    columnTypes_ = TypeInfoUtils.getTypeInfosFromTypeString(tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES));
	    
	    numCols_ = columnNames_.size();
	    
	    final List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(numCols_);
	    
	    for (int i=0; i< numCols_; i++) {
	      columnOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
	    }
	    
	    this.inspector_= ObjectInspectorFactory.getStandardStructObjectInspector(columnNames_, columnOIs);
	    row_ = new ArrayList<String>(numCols_);
	    
	    for (int i=0; i< numCols_; i++) {
	      row_.add(null);
	    }
	    
	    separator_ = getProperty(tbl, "separator", defaultDelim_);
	    numHeader_ = Integer.parseInt(getProperty(tbl, "numHeaders", defaultNumHeaders_)); 
	    // keyNames must match keys exactly how they are in CEF message.  Each entry must map to 1 hive column
	    // We need keyNames because:
	    //  1.  We want to have the flexibility to name hive columns differently
	    //  2.  Some key names in CEF cannot be used as HIve columns (for example containing dots)
	    keyNames_ = getProperty (tbl, "keys", defaultKeyNames_).split(",");
	    
	    
	    this.cefDict_ = new Hashtable<Integer, DictPojo> ();
	    
	    int i = 1;
	    String pojo = "";
	    for (String column : keyNames_)
	    {
	    	if (i < numHeader_)
	    	{
	    		pojo = i+","+column+",TRUE";;
	    	}
	    	else
	    	{
	    		pojo = i+","+column+",FALSE";
	    	}
	    	i++;
	    	DictPojo cd = new DictPojo(pojo);
	    	cefDict_.put(cd.getOrder(), cd);
	    }
	    
	    cf_ = MessageBuilder.buildMessage("CEF", cefDict_, separator_);
	}
	  
	 private final String getProperty(final Properties tbl, final String property, final String def) {
		    final String val = tbl.getProperty(property);
		    
		    if (val != null) {
		      return val;
		    }
		    
		    return def;
		  }
	
	@Override
	public Object deserialize(Writable blob) throws SerDeException {
		  Text rowText = (Text) blob;
		    
		  cf_.resetMsg(rowText.toString());
		  cf_.processMsg();
		  
		      
		    for (int i=0; i< numCols_; i++) {
		    	row_.set(i, cf_.msgHash_.get(keyNames_[i]));
		        
		      }
		      
		      return row_;
	}

	@Override
	public Writable serialize(Object arg0, ObjectInspector arg1)
			throws SerDeException {
		//  Do not really need to serialize at this point
		return null;
	}
	
	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		return inspector_;
	}
	
	@Override
	public Class<? extends Writable> getSerializedClass() {
		return Text.class;
	}
	
	@Override
	public SerDeStats getSerDeStats() {
		return null;
	}
}
