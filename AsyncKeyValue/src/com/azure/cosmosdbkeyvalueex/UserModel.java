package com.azure.cosmosdbkeyvalueex;

import com.fasterxml.jackson.annotation.JsonProperty;

public class UserModel {
	@JsonProperty(value = "id")
	public String Id;
	@JsonProperty(value = "name")
	public String Name;
}
