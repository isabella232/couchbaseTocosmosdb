package com.azure.cosmosdbspringexample;

import java.io.IOException;
import java.util.List;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.microsoft.azure.cosmosdb.Document;

@RestController
@RequestMapping("/documents")
public class DocumentsController {
	public static AppConfiguration objAppConfig;
	
	DocumentsController()
	{
		if(objAppConfig==null)
		{
			try {
				objAppConfig=new AppConfiguration();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	@RequestMapping(value = "/getDocuments", method = RequestMethod.GET)
	public List<Document> getDocuments()
	{

		CosmosDBHelper obj=new CosmosDBHelper(objAppConfig.HostName,objAppConfig.key,
				objAppConfig.dbName,objAppConfig.collName);
		
		return obj.getDocumentToVerify("Approved", "ghs-uk");
	}
	
}