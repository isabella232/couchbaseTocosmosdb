package com.azure.cosmosdbspringexample;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class AppConfiguration 
{
	public String HostName;
    public String key;
    public String dbName;
    public String collName;
     
    	public AppConfiguration() throws IOException {
    	    InputStream inputStream=null;
    		try {
    			Properties prop = new Properties();
    			String propFileName = "application.properties";
     
    			inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
     
    			if (inputStream != null) {
    				prop.load(inputStream);
    			} else {
    				throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
    			}
     
    			// get the property value and print it out
    			this.HostName = prop.getProperty("azure.cosmosdb.uri");
    			this.key = prop.getProperty("azure.cosmosdb.key");
    			this.dbName = prop.getProperty("azure.cosmosdb.database");
    			this.collName=prop.getProperty("azure.cosmosdb.collection");
    			
    		}
    		 finally {
    			inputStream.close();
    		}
    	}
    }

