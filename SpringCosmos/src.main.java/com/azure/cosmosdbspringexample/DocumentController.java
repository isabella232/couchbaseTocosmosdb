package com.azure.cosmosdbspringexample;



import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/document")
public class DocumentController {
	ICustomDocumentRepository _repo=null;
	public DocumentController(ICustomDocumentRepository repo)
	{
		_repo=repo;
	}
	
	@RequestMapping(value = "/getDocument", method = RequestMethod.GET)
	public CustomDoc getDocumentbyId(@RequestBody(required = true) CustomDoc objDoc)
	{
		List<CustomDoc> obj=_repo.findByIdAndName(objDoc.getId(),objDoc.getName());

		if(obj.size()>0)
			return obj.get(0);
		else
			return null;
	}
	
	@RequestMapping(value = "/getDocumentsByStatus", method = RequestMethod.GET)
	public List<CustomDoc> getDocumentsbyStatus(@RequestBody CustomDoc objDoc)
	{
		List<CustomDoc> obj=_repo.findAllByStatus(objDoc.getStatus());
		return obj;
	}
	@RequestMapping(value = "/saveDocument", method = RequestMethod.GET)
	public CustomDoc saveDocument(@RequestBody CustomDoc doc)
	{
		return _repo.save(doc);
	}

	@RequestMapping(value = "/upsertDocument", method = RequestMethod.GET)
	public CustomDoc upsertDocument(@RequestBody CustomDoc doc)
	{
		return _repo.save(doc);
	}
	
	@RequestMapping(value = "/deleteDocument", method = RequestMethod.GET)
	public int deleteDocument(@RequestBody CustomDoc doc)
	{
		_repo.delete(doc);
		return HttpStatus.NO_CONTENT.value();
	}

	
}