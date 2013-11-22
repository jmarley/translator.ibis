package org.jboss.teiid.translator.ibis;

import java.io.IOException;
import java.util.List;

import javax.resource.cci.Connection;

import org.apache.http.HttpException;
import org.apache.http.client.ClientProtocolException;

public interface IbisConnection extends Connection {
	
	/**
	 * Executes a solr like query against ibis api
	 * @return List<json docs>
	 * @throws IOException 
	 * @throws HttpException 
	 * @throws ClientProtocolException 
	 * @throws Exception 
	 */
	public List<String> executeQuery(String query) throws ClientProtocolException, HttpException, IOException, Exception;
	
}
