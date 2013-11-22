package org.jboss.teiid.translator.ibis.execution;

import java.io.IOException;
import java.util.List;

import org.jboss.teiid.translator.ibis.IbisConnection;
import org.jboss.teiid.translator.ibis.IbisExecutionFactory;
import org.junit.Test;

import org.teiid.query.unittest.RealMetadataFactory;
import org.mockito.Mockito;
import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.cdk.CommandBuilder;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;

public class TestIbisQueryExecution {
	private TransformationMetadata metadata;
	private IbisExecutionFactory translator;
	private TranslationUtility utility;

	private QueryMetadataInterface setUp(String ddl, String vdbName,
			String modelName) throws Exception {

		this.translator = new IbisExecutionFactory();
		this.translator.start();

		metadata = RealMetadataFactory.fromDDL(ddl, vdbName, modelName);
		this.utility = new TranslationUtility(metadata);

		

		return metadata;
	}

	private void helpExecute(String query) throws IOException, Exception {
		Select cmd = (Select) getCommand(query);
		ExecutionContext context = Mockito.mock(ExecutionContext.class);
		IbisConnection connection = Mockito.mock(IbisConnection.class);
		
		ResultSetExecution execution = this.translator
				.createResultSetExecution((QueryExpression) cmd, context,
						this.utility.createRuntimeMetadata(), connection);
		execution.execute();
		

	}

	public Command getCommand(String sql) throws IOException, Exception {

		CommandBuilder builder = new CommandBuilder(setUp(
				ObjectConverterUtil.convertFileToString(UnitTestUtil
						.getTestDataFile("exampleTBL.ddl")), "exampleVDB",
				"exampleModel"));
		return builder.getCommand(sql);

	}
	@Test
	public void testSimpleSelectNoAssosiations() throws Exception {
		String query = "SELECT * FROM example";
		helpExecute(query);
	}
}
