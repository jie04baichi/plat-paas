package com.largeyes.plat.paas.rpc.server;  
  
import org.codehaus.jackson.map.annotate.JsonRootName;

@JsonRootName( "serviceMetadata" )
public class ThriftServiceMetadata{

	private String version;
	 
    public ThriftServiceMetadata() {
    }
    
    public ThriftServiceMetadata( final String version ) {
        this.version = version;
    }
    
    public void setVersion( final String version ) {
        this.version = version;
    }
    
    public String getVersion() {
        return version;
    }    
}