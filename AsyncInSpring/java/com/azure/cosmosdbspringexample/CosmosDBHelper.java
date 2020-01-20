package com.azure.cosmosdbspringexample;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.springframework.http.HttpStatus;

import com.azure.data.cosmos.ConnectionMode;
import com.azure.data.cosmos.ConnectionPolicy;
import com.azure.data.cosmos.ConsistencyLevel;
import com.azure.data.cosmos.CosmosClient;
import com.azure.data.cosmos.CosmosContainer;
import com.azure.data.cosmos.CosmosItem;
import com.azure.data.cosmos.CosmosItemProperties;
import com.azure.data.cosmos.CosmosItemRequestOptions;
import com.azure.data.cosmos.CosmosItemResponse;
import com.azure.data.cosmos.FeedOptions;
import com.azure.data.cosmos.FeedResponse;
import com.azure.data.cosmos.PartitionKey;
import com.azure.data.cosmos.SqlParameter;
import com.azure.data.cosmos.SqlParameterList;
import com.azure.data.cosmos.SqlQuerySpec;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class CosmosDBHelper {
	static CosmosClient client=null;
	String _dbName="";
	String _collName="";
	List<CosmosItemProperties> _docs;
	Review doc=null;
	static CosmosContainer container=null;
	
	public CosmosDBHelper(String Host, String MasterKey, String dbName, String collName)
	{
		_dbName=dbName;
		_collName=collName;
		ConnectionPolicy cp=new ConnectionPolicy();
		cp.connectionMode(ConnectionMode.DIRECT);

		if(client==null)
			client= CosmosClient.builder()
					.endpoint(Host)//(Host, MasterKey, dbName, collName).Builder()
		            .connectionPolicy(cp)
		            .key(MasterKey)
		            .consistencyLevel(ConsistencyLevel.EVENTUAL)
		            .build();	

		container = client.getDatabase(_dbName).getContainer(_collName);
		
	}
	
	//Generic method to execute the queries
	public List<CosmosItemProperties> executeQuery(SqlQuerySpec query, String partitionKeyValue) throws InterruptedException
	{
		FeedOptions fo=new FeedOptions();
		if(partitionKeyValue!="")
		{		
			fo.partitionKey(new PartitionKey(partitionKeyValue));
			fo.enableCrossPartitionQuery(false);
		}
		else
		{
			fo.enableCrossPartitionQuery(true);
		}
		CountDownLatch latch=new CountDownLatch(1);
		_docs=new ArrayList<CosmosItemProperties>();
		Flux<FeedResponse<CosmosItemProperties>> objFlux= container.queryItems(query, fo);
		 objFlux.publishOn(Schedulers.elastic())
	
		 .subscribe(feedResponse ->{
			 if(feedResponse.results().size()>0)
			 {
				 _docs.addAll(feedResponse.results());
			 }
		 },Throwable::printStackTrace,latch::countDown);
		
		latch.await();
		
		return  _docs;
	}
	
	//Generic method to execute the queries
		public List<CosmosItemProperties> executeQueries(List<SqlQuerySpec> queries, String partitionKeyValue) throws InterruptedException
		{
			_docs=new ArrayList<CosmosItemProperties>();
			FeedOptions fo=new FeedOptions();
			if(partitionKeyValue!="")
			{		
				fo.partitionKey(new PartitionKey(partitionKeyValue));
				fo.enableCrossPartitionQuery(false);
			}
			else
			{
				fo.enableCrossPartitionQuery(true);
			}
			
			ArrayList<Flux<FeedResponse<CosmosItemProperties>>> lstFlux = new ArrayList<>();
			CountDownLatch latch=new CountDownLatch(queries.size());
			Flux<FeedResponse<CosmosItemProperties>> objFlux = null;
			
			for(SqlQuerySpec query:queries)
			{
					objFlux= container.queryItems(query, fo);
				
				objFlux .publishOn(Schedulers.elastic())
						.subscribe(feedResponse->
							{
								if(feedResponse.results().size()>0)
								{
									_docs.addAll(feedResponse.results());
								}
							
							},
							Throwable::printStackTrace,latch::countDown);
			lstFlux.add(objFlux);
			}
						
			Flux.merge(lstFlux);
			latch.await();

			return _docs;
		}

		/**
	     * This is the core example method which is required to demonstrate 
	     * execution of the custom query in the most optimal way.
	     * <p>
	     * After subscription the operation will be performed.
	     * The {@link Observable} upon successful completion will contain a single resource response with the replaced document.
	     * In case of failure the {@link Observable} will error.
	     *
	     * @param Status to fetch the documents.
	     * @param PartitionKeyValue to specify the partition to execute the query
		 * @throws InterruptedException 
	     */
	public List<Review> getDocumentToVerify(String Status, String partitionKeyValue) throws InterruptedException
	{
		SqlQuerySpec objSQL=new SqlQuerySpec();
		
		String query="select * from c where c.status=@status order by c.lastModeratedTime OFFSET 3 LIMIT 1";
		objSQL.queryText(query);
		SqlParameterList objParams=new SqlParameterList();
		objParams.add(new SqlParameter("@status", Status));
		objSQL.parameters(objParams);
		List<Review> lstReview=new ArrayList<Review>();
		List<CosmosItemProperties> gg= executeQuery(objSQL, "");
		for(CosmosItemProperties obj:gg)
		{
			lstReview.add(obj.toObject(Review.class));
		}
		return lstReview;
	}
	
	/**
     * This is the core example method which is required to demonstrate 
     * execution of the custom query in the most optimal way.
     * <p>
     * After subscription the operation will be performed.
     * The {@link Observable} upon successful completion will contain a single resource response with the replaced document.
     * In case of failure the {@link Observable} will error.
     *
     * @param Status to fetch the documents.
     * @param PartitionKeyValue to specify the partition to execute the query
	 * @throws InterruptedException 
     */
public List<Review> getDocumentsToVerify(List<String> lstStatus, String partitionKeyValue) throws InterruptedException
{
	List<SqlQuerySpec> lstSQL=new ArrayList<SqlQuerySpec>();
	for(String objStatus:lstStatus)
	{
		SqlQuerySpec objSQL=new SqlQuerySpec();
		String query="select * from c where c.status=@status order by c.lastModeratedTime";
		objSQL.queryText(query);
		SqlParameterList objParams=new SqlParameterList();
		objParams.add(new SqlParameter("@status", objStatus));
		objSQL.parameters(objParams);
		lstSQL.add(objSQL);
		
	}
	
	List<Review> lstReview=new ArrayList<Review>();
	List<CosmosItemProperties> lstCosmosItemProperties = executeQueries(lstSQL, partitionKeyValue);
if(lstCosmosItemProperties!=null)
	for(CosmosItemProperties objCosmosItemProperty:lstCosmosItemProperties)
	{
		lstReview.add(objCosmosItemProperty.toObject(Review.class));
	}
	return lstReview;
}

	/**
     * Insert a document.
     * <p>
     * After subscription the operation will be performed.
     * The {@link Observable} upon successful completion will contain a single resource response with the replaced document.
     * In case of failure the {@link Observable} will error.
     *
     * @param doc which needs to be updated.
	 * @throws InterruptedException 
     */
	public <T> int insertDocument(Review doc,T partitionKeyValue) throws InterruptedException  
	{
		CosmosItemRequestOptions ro=new CosmosItemRequestOptions();
		ro.partitionKey(new PartitionKey(partitionKeyValue));
		
		Mono<CosmosItemResponse> objMono= container.createItem(doc,ro);
		return executeMono(objMono,HttpStatus.CREATED.value());
	}
	
	/**
     * Replaces a document with the passed in document.
     * <p>
     * After subscription the operation will be performed.
     * The {@link Observable} upon successful completion will contain a single resource response with the replaced document.
     * In case of failure the {@link Observable} will error.
     *
     * @param document (only document ID is required to be populated) which needs to be updated.
     * @param values which needs to be updated.
	 * @throws InterruptedException 
     */
	public <T> int upsertDocument(Review doc, T partitionKeyValue) throws InterruptedException  
		{
			CosmosItemRequestOptions ro=new CosmosItemRequestOptions();
			ro.partitionKey(new PartitionKey(partitionKeyValue));
			
			Mono<CosmosItemResponse> obs= container.upsertItem(doc, ro);
			
			return executeMono(obs,HttpStatus.OK.value());
		}

	/**
     * deletes a document with the passed in document.
     * <p>
     * After subscription the operation will be performed.
     * The {@link Observable} upon successful completion will contain a single resource response with the replaced document.
     * In case of failure the {@link Observable} will error.
     *
     * @param document (only document ID is required to be populated) which needs to be updated.
     * @param values which needs to be updated.
	 * @throws InterruptedException 
     */
	public <T> int deleteDocument(Review doc, T partitionKey) throws InterruptedException
	{
		CosmosItemRequestOptions ro=new CosmosItemRequestOptions();
		ro.partitionKey(new PartitionKey(partitionKey));
	
		CosmosItem objItem= container.getItem(doc.Id, doc.Tenant);
		Mono<CosmosItemResponse> objMono = objItem.delete(ro);
		return executeMono(objMono,HttpStatus.NO_CONTENT.value());
	}
	 
		/**
	     * Private utility method to execute observer
	     * <p>
	     * * The {@link Observable} upon successful completion will contain a single resource response with the replaced document.
	     * In case of failure the {@link Observable} will error.
	     *
	     * @param doc which needs to be updated.
		 * @throws InterruptedException 
	     */
		private int executeMono(Mono<CosmosItemResponse> objMono, int successStatus) throws InterruptedException {
			CountDownLatch latch=new CountDownLatch(1);
		objMono .publishOn(Schedulers.elastic())
				.subscribe(resourceResponse->
							{
								if(resourceResponse.statusCode()!=successStatus)
									{
										throw new RuntimeException(resourceResponse.toString());
									}
								},
							Throwable::printStackTrace,latch::countDown);
				latch.await();
				
				return successStatus;
		}
}
