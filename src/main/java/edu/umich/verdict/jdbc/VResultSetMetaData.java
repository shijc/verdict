package edu.umich.verdict.jdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class VResultSetMetaData implements ResultSetMetaData {
    private final ResultSetMetaData originalMetaData;
    private final ExtraColumnsHandler extraColumns;
    private final int originalColumnsCount;

    public VResultSetMetaData(ResultSet originalResultSet, ExtraColumnsHandler extraColumns) throws SQLException {
        this.originalMetaData = originalResultSet.getMetaData();
        this.extraColumns = extraColumns;
        this.originalColumnsCount = extraColumns.getOriginalCount();
    }

    @Override
    public int getColumnCount() throws SQLException {
        return extraColumns.getTotalCount();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return !extraColumns.isExtra(column)? originalMetaData.isAutoIncrement(column): false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return !extraColumns.isExtra(column)? originalMetaData.isCaseSensitive(column): true;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return !extraColumns.isExtra(column)? originalMetaData.isSearchable(column): false;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return !extraColumns.isExtra(column)? originalMetaData.isCurrency(column): false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return !extraColumns.isExtra(column)? originalMetaData.isNullable(column): columnNoNulls;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return !extraColumns.isExtra(column)? originalMetaData.isSearchable(column): false;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return !extraColumns.isExtra(column)? originalMetaData.getColumnDisplaySize(column): extraColumns.getDisplaySize(column);
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return !extraColumns.isExtra(column)? originalMetaData.getColumnLabel(column): extraColumns.getLabel(column);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return !extraColumns.isExtra(column)? originalMetaData.getColumnName(column): extraColumns.getLabel(column);
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return !extraColumns.isExtra(column)? originalMetaData.getSchemaName(column): "";
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return !extraColumns.isExtra(column)? originalMetaData.getPrecision(column): extraColumns.getPrecision(column);
    }

    @Override
    public int getScale(int column) throws SQLException {
        return !extraColumns.isExtra(column)? originalMetaData.getScale(column): extraColumns.getScale(column);
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return !extraColumns.isExtra(column)? originalMetaData.getTableName(column): "";
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return !extraColumns.isExtra(column)? originalMetaData.getCatalogName(column): "";
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return !extraColumns.isExtra(column)? originalMetaData.getColumnType(column): extraColumns.getType(column);
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return !extraColumns.isExtra(column)? originalMetaData.getColumnTypeName(column): extraColumns.getTypeName(column);
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return !extraColumns.isExtra(column)? originalMetaData.getColumnClassName(column): extraColumns.getClassName(column);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return iface.cast(originalMetaData);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(originalMetaData);
    }
}
