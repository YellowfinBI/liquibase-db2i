package liquibase.ext.db2i.sqlgenerator;

import java.util.ArrayList;
import java.util.List;

import liquibase.database.Database;
import liquibase.datatype.DataTypeFactory;
import liquibase.datatype.DatabaseDataType;
import liquibase.ext.db2i.database.DB2iDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.RenameColumnGenerator;
import liquibase.statement.core.RenameColumnStatement;

public class RenameColumnGeneratorDB2i extends RenameColumnGenerator {
    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(RenameColumnStatement statement, Database database) {
        return database instanceof DB2iDatabase;
    }

    @Override
    public Sql[] generateSql(RenameColumnStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
    		
       	boolean droppingColumns = !Boolean.parseBoolean(System.getProperty("db2i.dont.drop"));
    	
    	    DatabaseDataType dataType = DataTypeFactory.getInstance().fromDescription(statement.getColumnDataType(), database).toDatabaseDataType(database);
    	
    	    List<Sql> sql = new ArrayList<Sql>();
    	    
        sql.add(new UnparsedSql(
                        "ALTER TABLE "
                                + database.escapeTableName(
                                        statement.getCatalogName(), statement.getSchemaName(), statement.getTableName())
                                + " ADD COLUMN "
                                + database.escapeColumnName(statement.getCatalogName(), statement.getSchemaName(),
                                        statement.getTableName(), statement.getNewColumnName())
                                + " " 
                                + database.escapeDataTypeName(dataType.toSql()),
                        getAffectedOldColumn(statement), getAffectedNewColumn(statement)));
        
        sql.add(new UnparsedSql(
                        "UPDATE "
                                + database.escapeTableName(
                                        statement.getCatalogName(), statement.getSchemaName(), statement.getTableName())
                                + " SET "
                                + database.escapeColumnName(statement.getCatalogName(), statement.getSchemaName(),
                                        statement.getTableName(), statement.getNewColumnName())
                                + " = "
                                + database.escapeColumnName(statement.getCatalogName(), statement.getSchemaName(),
                                        statement.getTableName(), statement.getOldColumnName()),
                        getAffectedOldColumn(statement), getAffectedNewColumn(statement)));
        
        if (droppingColumns) {
        	
        	    sql.add(new UnparsedSql(
                        "ALTER TABLE "
                                + database.escapeTableName(
                                        statement.getCatalogName(), statement.getSchemaName(), statement.getTableName())
                                + " DROP COLUMN "
                                + database.escapeColumnName(statement.getCatalogName(), statement.getSchemaName(),
                                        statement.getTableName(), statement.getOldColumnName())
                                ,
                        getAffectedOldColumn(statement), getAffectedNewColumn(statement)));
        }
        
        return sql.toArray(new Sql[0]);
                
    }
}