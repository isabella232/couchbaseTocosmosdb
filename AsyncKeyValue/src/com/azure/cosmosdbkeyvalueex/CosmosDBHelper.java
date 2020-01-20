package com.azure.cosmosdbkeyvalueex;

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
import com.azure.data.cosmos.NotFoundException;
import com.azure.data.cosmos.PartitionKey;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
public class CosmosDBHelper {
	
	static CosmosClient client=null;
	String _dbName="";
	String _collName="";
	List<CosmosItemProperties> _docs;
	UserModel doc=null;
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
	public <T> int insertDocument(UserModel doc) throws InterruptedException,NotFoundException
	{
		CosmosItemRequestOptions ro=new CosmosItemRequestOptions();
		ro.partitionKey(new PartitionKey(doc.Id));
		
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
	public <T> int upsertDocument(UserModel doc) throws InterruptedException, NotFoundException 
		{
			CosmosItemRequestOptions ro=new CosmosItemRequestOptions();
			ro.partitionKey(new PartitionKey(doc.Id));
			
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
	public <T> int deleteDocument(String id) throws InterruptedException, NotFoundException
	{
		CosmosItemRequestOptions ro=new CosmosItemRequestOptions();
		ro.partitionKey(new PartitionKey(id));
	
		CosmosItem objItem= container.getItem(id, id);
		Mono<CosmosItemResponse> objMono = objItem.delete(ro);
		return executeMono(objMono,HttpStatus.NO_CONTENT.value());
	}


	/**
     * Retrieve a document with the specified documentID.
     * <p>
     * After subscription the operation will be performed.
     * The {@link Observable} upon successful completion will contain a single resource response with the replaced document.
     * In case of failure the {@link Observable} will error.
     *
     * @param document ID which needs to be retrieved.
	 * @throws InterruptedException 
     */	
	public UserModel getDocument(String documentId) throws InterruptedException, NotFoundException
	{
		CosmosItemRequestOptions ro=new CosmosItemRequestOptions();
		ro.partitionKey(new PartitionKey(documentId));
		CountDownLatch latch=new CountDownLatch(1);
		
		var objCosmosItem= container.getItem(documentId, documentId);
		Mono<CosmosItemResponse> objMono = objCosmosItem.read(ro);
		objMono .publishOn(Schedulers.elastic())
			.doOnError(e -> {

                if (e instanceof NotFoundException) {

                    // This is signal to the upper logic either to refresh

                    // collection cache and retry.
                	doc=null;
                   }})
			.subscribe(resourceResponse->
						{
							if(resourceResponse.item()!=null)
							{
								doc= resourceResponse.properties().toObject(UserModel.class);
							}
						},
						Throwable::printStackTrace,latch::countDown);
		latch.await();
		return doc;
	}

	int status;
		/**
	     * Private utility method to execute observer
	     * <p>
	     * * The {@link Observable} upon successful completion will contain a single resource response with the replaced document.
	     * In case of failure the {@link Observable} will error.
	     *
	     * @param doc which needs to be updated.
		 * @throws InterruptedException 
	     */
		private int executeMono(Mono<CosmosItemResponse> objMono, int successStatus) throws InterruptedException, NotFoundException {
			status = successStatus;
			CountDownLatch latch=new CountDownLatch(1);
		objMono .publishOn(Schedulers.elastic())
				.doOnError(e -> {
					if(e  instanceof NotFoundException)
					{
						status= 404;
						}
					})
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
