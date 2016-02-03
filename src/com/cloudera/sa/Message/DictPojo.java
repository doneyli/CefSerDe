package com.cloudera.sa.Message;

	public class DictPojo {

		private String name_;
		private Integer order_;
		private Boolean header_;
		
		public DictPojo (String line)
		{
			String [] header = line.split(",");
			this.name_ = header[1];
			this.order_ = Integer.parseInt(header[0]);
			this.header_ = Boolean.parseBoolean(header[2]);
		}
		public String getName()
		{
			return name_;
		}
		public Integer getOrder()
		{
			return order_;
		}
		
		public Boolean getHeader()
		{
			return header_;
		}
		
		public String toString()
		{
			return (order_+ "-" + name_ ); 
		}
}


