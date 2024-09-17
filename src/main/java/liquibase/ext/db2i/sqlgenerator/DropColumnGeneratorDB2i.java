package liquibase.ext.db2i.sqlgenerator;

import liquibase.database.Database;
import liquibase.ext.db2i.database.DB2iDatabase;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.DropColumnGenerator;
import liquibase.statement.core.DropColumnStatement;

public class DropColumnGeneratorDB2i extends DropColumnGenerator {

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(DropColumnStatement statement, Database database) {
        return database instanceof DB2iDatabase;
    }
	
	@Override
    public Sql[] generateSql(DropColumnStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
		boolean droppingColumns = !Boolean.parseBoolean(System.getProperty("db2i.dont.drop"));
		if (!droppingColumns) {
			return new Sql[0];
		}
        return super.generateSql(statement, database, sqlGeneratorChain);
    }
	
}
