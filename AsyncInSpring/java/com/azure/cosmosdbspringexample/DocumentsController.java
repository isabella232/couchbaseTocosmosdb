package com.azure.cosmosdbspringexample;

import java.io.IOException;
import java.util.List;

import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/documents")
public class DocumentsController {
	public static AppConfiguration objAppConfig;
	private CosmosDBHelper objCosmosDBHelper=null;
	DocumentsController()
	{
		if(objAppConfig==null)
		{
			try {
				objAppConfig=new AppConfiguration();

				objCosmosDBHelper=new CosmosDBHelper(objAppConfig.HostName,objAppConfig.key,objAppConfig.dbName,objAppConfig.collName);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	@RequestMapping(value = "/createDocument", method = RequestMethod.POST)
	public int createDocument(@RequestBody Review doc, @RequestHeader("tenant") String partitionKey) throws InterruptedException
	{
		return objCosmosDBHelper.insertDocument(doc,partitionKey);
	}
	
	
	@RequestMapping(value = "/upsertDocument", method = RequestMethod.POST)
	public int upsertDocument(@RequestBody Review doc, @RequestHeader("tenant") String partitionKey) throws InterruptedException
	{
		return objCosmosDBHelper.upsertDocument(doc, partitionKey);
	}

	@RequestMapping(value = "/getDocuments", method = RequestMethod.GET)
	public List<Review> getDocuments(@RequestHeader String status, @RequestHeader("tenant") String partitionKey) throws InterruptedException
	{
		return objCosmosDBHelper.getDocumentToVerify(status, partitionKey);
	}

	@RequestMapping(value = "/getListOfDocuments", method = RequestMethod.GET)
	public List<Review> getDocuments(@RequestBody List<String> lstStatus, @RequestHeader("tenant") String partitionKeyValue) throws InterruptedException
	{
		return objCosmosDBHelper.getDocumentsToVerify(lstStatus, partitionKeyValue);
	}

	
	@RequestMapping(value = "/deleteDocument", method = RequestMethod.POST)
	public int deleteDocument(@RequestBody Review doc, @RequestHeader("tenant") String partitionKey) throws InterruptedException
	{
		return objCosmosDBHelper.deleteDocument(doc, partitionKey);
	}

	
	
}