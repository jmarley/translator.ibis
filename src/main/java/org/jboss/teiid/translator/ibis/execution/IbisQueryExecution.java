package org.jboss.teiid.translator.ibis.execution;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.logging.LogManager;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;
import org.jboss.teiid.translator.ibis.IbisConnection;
import org.jboss.teiid.translator.ibis.IbisExecutionFactory;

/**
 * @author Jason Marley, Red Hat, Inc.
 * @author Syed Iqbal, Red Hat, Inc.
 * 
 */
public class IbisQueryExecution implements ResultSetExecution {

	private RuntimeMetadata metadata;
	private Select query;
	@SuppressWarnings("unused")
	private ExecutionContext executionContext;
	private IbisConnection connection;
	private IbisSQLHierarchyVistor visitor;
	private String queryParams;
	private List<String> queryResponse;
	private Iterator<String> docItr;
	private Class<?>[] expectedTypes;
	private IbisExecutionFactory executionFactory;

	public IbisQueryExecution(QueryExpression command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			IbisConnection connection) {
		this.metadata = metadata;
		this.query = (Select) command;
		this.executionContext = executionContext;
		this.connection = connection;
		this.expectedTypes = command.getColumnTypes();

	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public void cancel() throws TranslatorException {
		// TODO Auto-generated method stub

	}

	@SuppressWarnings("static-access")
	@Override
	public void execute() throws TranslatorException {
		this.visitor = new IbisSQLHierarchyVistor(metadata);

		// translate sql query into ibis query string
		this.visitor.visitNode(query);

		// get ibis query string
		queryParams = this.visitor.getResolvedPath();
		
		LogManager.logInfo("This is the ibis query", queryParams);
		// TODO set offset
		// TODO set row result limit 

		// execute ibis query
		try {
			queryResponse = connection.executeQuery(queryParams);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LogManager.logCritical("query execution issue",
					queryParams.toString());
			e.printStackTrace();
		}

		this.docItr = queryResponse.iterator();

	}

	/*
	 * This iterates through the documents from CouchDB and maps their fields to
	 * rows in the Teiid table
	 * 
	 * @see org.teiid.translator.ResultSetExecution#next()
	 */
	@Override
	public List<?> next() throws TranslatorException, DataNotAvailableException {

		final List<Object> row = new ArrayList<Object>();
		String columnName;
		String columnValue;

		// is there any couchdb docs
		if (this.docItr != null && this.docItr.hasNext()) {
			columnValue=this.docItr.next();
			LogManager.logInfo("this is json document in string format", " "
					+ columnValue);

			for (int i = 0; i < this.visitor.fieldNameList.size(); i++) {
				// TODO handle multiple tables
				columnName = this.visitor.getShortFieldName(i);
			
				LogManager.logInfo("this is json document in string format", " "
						+ columnValue);
				row.add(columnValue);}

			return row;
		}
		return null;
	}
}
