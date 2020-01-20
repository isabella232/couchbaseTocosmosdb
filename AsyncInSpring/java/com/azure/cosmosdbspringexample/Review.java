package com.azure.cosmosdbspringexample;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Review {
	@JsonProperty(value = "id")
	public String Id;
	@JsonProperty(value = "tenant")
	public String Tenant;
	@JsonProperty(value = "status")
	public String Status;
}
