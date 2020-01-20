package com.azure.cosmosdbkeyvalueex;

import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.azure.data.cosmos.NotFoundException;

@RestController
@RequestMapping("/documents")
public class DocumentsController {
	private static final String HOST = "https://tesco-reviews.documents.azure.com:443/";
	private static final String MASTER_KEY = "4x101U4iFo8bvE11YuEj5LfG9bhdXMMlJwnQlALcN7vAkDVUyl1Y4LFc7PpNW2Emk1KO6C0QEpxLmUFIewWmqg==";
	private static final String DB_NAME="contentdb";
	private static final String COLL_NAME="keyvalue";
	
	
	@RequestMapping(value = "/createDocument", method = RequestMethod.POST)
	public int createDocument(@RequestBody UserModel doc) throws InterruptedException, NotFoundException
	{
		CosmosDBHelper obj=new CosmosDBHelper(HOST,MASTER_KEY,DB_NAME,COLL_NAME);
		return obj.insertDocument(doc);
	}
	
	@RequestMapping(value = "/upsertDocument", method = RequestMethod.POST)
	public int upsertDocument(@RequestBody UserModel doc) throws InterruptedException
	{
		CosmosDBHelper obj=new CosmosDBHelper(HOST,MASTER_KEY,DB_NAME,COLL_NAME);
		try {
		return obj.upsertDocument(doc);
	}
	catch(NotFoundException ex)
	{
		return 404;
	}
	}
	
	@RequestMapping(value = "/getDocument", method = RequestMethod.GET)
	public UserModel getDocument(@RequestHeader("id") String id) throws InterruptedException
	{
		CosmosDBHelper obj=new CosmosDBHelper(HOST,MASTER_KEY,DB_NAME,COLL_NAME);
		
		try{
			return obj.getDocument(id);
		}
		catch(NotFoundException ex)
		{
			return null;
		}
	}
	
	@RequestMapping(value = "/deleteDocument", method = RequestMethod.POST)
	public int deleteDocument(@RequestHeader("id") String id) throws InterruptedException
	{
		CosmosDBHelper obj=new CosmosDBHelper(HOST,MASTER_KEY,DB_NAME,COLL_NAME);
		try {
		return obj.deleteDocument(id);
		}
		catch(NotFoundException ex)
		{
			return 404;
		}
	}
}
