package com.azure.cosmosdbspringexample;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import com.microsoft.azure.cosmosdb.ConnectionMode;
import com.microsoft.azure.cosmosdb.ConnectionPolicy;
import com.microsoft.azure.cosmosdb.ConsistencyLevel;
import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.cosmosdb.FeedOptions;
import com.microsoft.azure.cosmosdb.FeedResponse;
import com.microsoft.azure.cosmosdb.PartitionKey;
import com.microsoft.azure.cosmosdb.SqlParameter;
import com.microsoft.azure.cosmosdb.SqlParameterCollection;
import com.microsoft.azure.cosmosdb.SqlQuerySpec;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;

import rx.Observable;
import rx.schedulers.Schedulers;

public class CosmosDBHelper {
	static AsyncDocumentClient client=null;
	String _dbName="";
	String _collName="";
	List<Document> _docs;
	
	public CosmosDBHelper(String Host, String MasterKey, String dbName, String collName)
	{
		_dbName=dbName;
		_collName=collName;
		ConnectionPolicy cp=new ConnectionPolicy();
		cp.setConnectionMode(ConnectionMode.Direct);

		if(client==null)
			client= new AsyncDocumentClient.Builder()
		            .withServiceEndpoint(Host)
		            .withMasterKeyOrResourceToken(MasterKey)
		            .withConnectionPolicy(cp)
		            .withConsistencyLevel(ConsistencyLevel.Eventual)
		            .build();	

	}
	//Generic method to execute the queries
	public List<Document> executeQuery(SqlQuerySpec query, String partitionKeyValue)
	{
		FeedOptions fo=new FeedOptions();
		if(partitionKeyValue!="")
		{		
			fo.setPartitionKey(new PartitionKey(partitionKeyValue));
			fo.setEnableCrossPartitionQuery(false);
		}
		else
		{
			fo.setEnableCrossPartitionQuery(true);
		}
		String collLink=String.format("/dbs/%s/colls/%s", _dbName,_collName);
		CountDownLatch latch=new CountDownLatch(1);
		
		Observable<FeedResponse<Document>> obs= client.queryDocuments(collLink, query, fo);
		obs .subscribeOn(Schedulers.computation())
			.subscribe(resourceResponse->
						{
							_docs=resourceResponse.getResults();
						},
						Throwable::printStackTrace,latch::countDown);
		try 
		{
			latch.await();
		} 
		catch (InterruptedException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return _docs;
	}
	
	//Generic method to execute the queries
		public List<Document> executeQueries(List<SqlQuerySpec> queries, String partitionKeyValue)
		{
			FeedOptions fo=new FeedOptions();
			if(partitionKeyValue!="")
			{		
				fo.setPartitionKey(new PartitionKey(partitionKeyValue));
				fo.setEnableCrossPartitionQuery(false);
			}
			else
			{
				fo.setEnableCrossPartitionQuery(true);
			}
			
			String collLink=String.format("/dbs/%s/colls/%s", _dbName,_collName);
			
			ArrayList<Observable<FeedResponse<Document>>> readDocumentObservables = new ArrayList<>();
			CountDownLatch latch=new CountDownLatch(queries.size()+1);
			
			for(SqlQuerySpec query:queries)
			{

			Observable<FeedResponse<Document>> obs= client.queryDocuments(collLink, query, fo);
			obs .subscribeOn(Schedulers.computation())
				.subscribe(resourceResponse->
							{
								_docs.addAll(resourceResponse.getResults());
							},
							Throwable::printStackTrace,latch::countDown);
			readDocumentObservables.add(obs);
			}
			
			try 
			{
				Observable.merge(readDocumentObservables, readDocumentObservables.size());
				latch.await();
			} 
			catch (InterruptedException e) 
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return _docs;
		}
	//Spring Style methods with custom query
	public List<Document> getDocumentToVerify(String Name, String partitionKeyValue)
	{
		SqlQuerySpec objSQL=new SqlQuerySpec();
		
		String query="select * from c where c.status=@status";
		objSQL.setQueryText(query);
		SqlParameterCollection objParams=new SqlParameterCollection();
		objParams.add(new SqlParameter("@status",Name));
		objSQL.setParameters(objParams);
		return executeQuery(objSQL, partitionKeyValue);
	}
}
