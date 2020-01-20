package com.azure.cosmosdbspringexample;
import java.util.List;

import org.springframework.stereotype.Repository;
import com.microsoft.azure.spring.data.cosmosdb.repository.DocumentDbRepository;

@Repository
public interface ICustomDocumentRepository extends DocumentDbRepository<CustomDoc, String> {

	List<CustomDoc> findAllByStatus(String status);

	List<CustomDoc> findByIdAndName(String id, String name);


}
