package edu.umich.verdict.jdbc;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.InvalidConfigurationException;
import edu.umich.verdict.transformation.TransformedQuery;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;

//TODO: complete
public class VResultSet implements ResultSet {

    private final ExtraColumnsHandler extraColumns;
    private final ResultSet originalResultSet;
    private final int originalColumnCount;
    private TransformedQuery transformedQuery;

    public VResultSet(ResultSet resultSet, TransformedQuery q, Configuration conf) throws InvalidConfigurationException, SQLException {
        this.originalResultSet = resultSet;
        extraColumns = new ExtraColumnsHandler(resultSet, q, conf);
        originalColumnCount = q.getOriginalColumnsCount();
        transformedQuery = q;
    }

    public String getApproximationInfo() {
        StringBuilder buf = new StringBuilder();
        buf.append("Sample Size: ").append((double) Math.round(transformedQuery.getSample().getCompRatio() * 100000) / 1000 + "%").append("\n");
        buf.append("Sample Name: ").append(transformedQuery.getSample().getName()).append("\n");
        buf.append("Bootstrap Trials: ").append(transformedQuery.getBootstrapTrials()).append("\n");
        buf.append("Bootstrap Method: ").append(transformedQuery.getMethod()).append("\n");
        buf.append("Estimation Confidence: ").append(transformedQuery.getConfidence() * 100 + "%").append("\n");

        return buf.toString();
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        return new VResultSetMetaData(originalResultSet, extraColumns);
    }

    public boolean next() throws SQLException {
        if (!originalResultSet.next())
            return false;
        extraColumns.invalidateValues();
        return true;
    }

    public void beforeFirst() throws SQLException {
        originalResultSet.beforeFirst();
        extraColumns.invalidateValues();
    }

    public void afterLast() throws SQLException {
        originalResultSet.afterLast();
        extraColumns.invalidateValues();
    }

    public boolean first() throws SQLException {
        if (!originalResultSet.first())
            return false;
        extraColumns.invalidateValues();
        return true;
    }

    public boolean last() throws SQLException {
        if (!originalResultSet.last())
            return false;
        extraColumns.invalidateValues();
        return true;
    }

    public boolean absolute(int row) throws SQLException {
        if (!originalResultSet.absolute(row))
            return false;
        extraColumns.invalidateValues();
        return true;
    }

    public boolean relative(int rows) throws SQLException {
        if (!originalResultSet.relative(rows))
            return false;
        extraColumns.invalidateValues();
        return true;
    }

    public boolean previous() throws SQLException {
        if (!originalResultSet.previous())
            return false;
        extraColumns.invalidateValues();
        return true;
    }

    public int findColumn(String columnLabel) throws SQLException {
        try {
            return originalResultSet.findColumn(columnLabel);
        } catch (SQLException e) {
            int col = extraColumns.findColumn(columnLabel);
            if (col != -1)
                return col;
            throw e;
        }
    }

    public String getString(int columnIndex) throws SQLException {
        if (!extraColumns.isExtra(columnIndex))
            return originalResultSet.getString(columnIndex);
        Double val = extraColumns.get(columnIndex);
        return val == null ? null : val + "";
    }

    public boolean getBoolean(int columnIndex) throws SQLException {
        return !extraColumns.isExtra(columnIndex) ? originalResultSet.getBoolean(columnIndex) : extraColumns.getDouble(columnIndex) != 0;
    }

    public byte getByte(int columnIndex) throws SQLException {
        return !extraColumns.isExtra(columnIndex) ? originalResultSet.getByte(columnIndex) : (byte) extraColumns.getDouble(columnIndex);
    }

    public short getShort(int columnIndex) throws SQLException {
        return !extraColumns.isExtra(columnIndex) ? originalResultSet.getShort(columnIndex) : (short) extraColumns.getDouble(columnIndex);
    }

    public int getInt(int columnIndex) throws SQLException {
        return !extraColumns.isExtra(columnIndex) ? originalResultSet.getInt(columnIndex) : (int) extraColumns.getDouble(columnIndex);
    }

    public long getLong(int columnIndex) throws SQLException {
        return !extraColumns.isExtra(columnIndex) ? originalResultSet.getLong(columnIndex) : (long) extraColumns.getDouble(columnIndex);
    }

    public float getFloat(int columnIndex) throws SQLException {
        return !extraColumns.isExtra(columnIndex) ? originalResultSet.getFloat(columnIndex) : (float) extraColumns.getDouble(columnIndex);
    }

    public double getDouble(int columnIndex) throws SQLException {
        return !extraColumns.isExtra(columnIndex) ? originalResultSet.getDouble(columnIndex) : extraColumns.getDouble(columnIndex);
    }

    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return !extraColumns.isExtra(columnIndex) ? originalResultSet.getBigDecimal(columnIndex) : BigDecimal.valueOf(extraColumns.getDouble(columnIndex));
    }

    public byte[] getBytes(int columnIndex) throws SQLException {
        return originalResultSet.getBytes(columnIndex);
    }

    public Date getDate(int columnIndex) throws SQLException {
        return originalResultSet.getDate(columnIndex);
    }

    public Time getTime(int columnIndex) throws SQLException {
        return originalResultSet.getTime(columnIndex);
    }

    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return originalResultSet.getTimestamp(columnIndex);
    }

    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        return originalResultSet.getAsciiStream(columnIndex);
    }

    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return originalResultSet.getUnicodeStream(columnIndex);
    }

    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return originalResultSet.getBinaryStream(columnIndex);
    }

    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getAsciiStream(findColumn(columnLabel));
    }

    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return getUnicodeStream(findColumn(columnLabel));
    }

    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getBinaryStream(findColumn(columnLabel));
    }

    public Object getObject(int columnIndex) throws SQLException {
        return !extraColumns.isExtra(columnIndex) ? originalResultSet.getObject(columnIndex) : extraColumns.get(columnIndex);
    }

    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    public Reader getCharacterStream(int columnIndex) throws SQLException {
        return originalResultSet.getCharacterStream(columnIndex);
    }

    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return getCharacterStream(findColumn(columnLabel));
    }

    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return originalResultSet.getBigDecimal(columnIndex);
    }

    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return originalResultSet.getObject(columnIndex);
    }

    public Ref getRef(int columnIndex) throws SQLException {
        return originalResultSet.getRef(columnIndex);
    }

    public Blob getBlob(int columnIndex) throws SQLException {
        return originalResultSet.getBlob(columnIndex);
    }

    public Clob getClob(int columnIndex) throws SQLException {
        return originalResultSet.getClob(columnIndex);
    }

    public Array getArray(int columnIndex) throws SQLException {
        return originalResultSet.getArray(columnIndex);
    }

    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return getObject(findColumn(columnLabel), map);
    }

    public Ref getRef(String columnLabel) throws SQLException {
        return getRef(findColumn(columnLabel));
    }

    public Blob getBlob(String columnLabel) throws SQLException {
        return getBlob(findColumn(columnLabel));
    }

    public Clob getClob(String columnLabel) throws SQLException {
        return getClob(findColumn(columnLabel));
    }

    public Array getArray(String columnLabel) throws SQLException {
        return getArray(findColumn(columnLabel));
    }

    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return originalResultSet.getDate(columnIndex);
    }

    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(findColumn(columnLabel), cal);
    }

    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return originalResultSet.getTime(columnIndex);
    }

    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(findColumn(columnLabel), cal);
    }

    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return originalResultSet.getTimestamp(columnIndex);
    }

    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(findColumn(columnLabel), cal);
    }

    public URL getURL(int columnIndex) throws SQLException {
        return originalResultSet.getURL(columnIndex);
    }

    public URL getURL(String columnLabel) throws SQLException {
        return getURL(findColumn(columnLabel));
    }

    public NClob getNClob(int columnIndex) throws SQLException {
        return originalResultSet.getNClob(columnIndex);
    }

    public NClob getNClob(String columnLabel) throws SQLException {
        return getNClob(findColumn(columnLabel));
    }

    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        return originalResultSet.getSQLXML(columnIndex);
    }

    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return getSQLXML(findColumn(columnLabel));
    }

    public String getNString(int columnIndex) throws SQLException {
        return originalResultSet.getNString(columnIndex);
    }

    public String getNString(String columnLabel) throws SQLException {
        return getNString(findColumn(columnLabel));
    }

    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return originalResultSet.getNCharacterStream(columnIndex);
    }

    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return getNCharacterStream(findColumn(columnLabel));
    }

    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return originalResultSet.getObject(columnIndex, type);
    }

    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getObject(findColumn(columnLabel), type);
    }

    public RowId getRowId(int columnIndex) throws SQLException {
        return originalResultSet.getRowId(columnIndex);
    }

    public RowId getRowId(String columnLabel) throws SQLException {
        return getRowId(findColumn(columnLabel));
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    public void close() throws SQLException {
        originalResultSet.close();
    }

    public boolean wasNull() throws SQLException {
        return originalResultSet.wasNull();
    }

    public SQLWarning getWarnings() throws SQLException {
        return originalResultSet.getWarnings();
    }

    public void clearWarnings() throws SQLException {
        originalResultSet.clearWarnings();
    }

    public String getCursorName() throws SQLException {
        return originalResultSet.getCursorName();
    }

    public boolean isBeforeFirst() throws SQLException {
        return originalResultSet.isBeforeFirst();
    }

    public boolean isAfterLast() throws SQLException {
        return originalResultSet.isAfterLast();
    }

    public boolean isFirst() throws SQLException {
        return originalResultSet.isFirst();
    }

    public boolean isLast() throws SQLException {
        return originalResultSet.isLast();
    }

    public int getRow() throws SQLException {
        return originalResultSet.getRow();
    }

    public void setFetchDirection(int direction) throws SQLException {
        originalResultSet.setFetchDirection(direction);
    }

    public int getFetchDirection() throws SQLException {
        return originalResultSet.getFetchDirection();
    }

    public void setFetchSize(int rows) throws SQLException {
        originalResultSet.setFetchSize(rows);
    }

    public int getFetchSize() throws SQLException {
        return originalResultSet.getFetchSize();
    }

    public int getType() throws SQLException {
        return originalResultSet.getType();
    }

    public int getConcurrency() throws SQLException {
        if (isClosed())
            throw new SQLException("Connection is closed.");
        return CONCUR_READ_ONLY;
    }

    public boolean rowUpdated() throws SQLException {
        if (isClosed())
            throw new SQLException("Connection is closed.");
        return false;
    }

    public boolean rowInserted() throws SQLException {
        if (isClosed())
            throw new SQLException("Connection is closed.");
        return false;
    }

    public boolean rowDeleted() throws SQLException {
        if (isClosed())
            throw new SQLException("Connection is closed.");
        return false;
    }

    public Statement getStatement() throws SQLException {
        return null;
    }

    public int getHoldability() throws SQLException {
        return originalResultSet.getHoldability();
    }

    public boolean isClosed() throws SQLException {
        return originalResultSet.isClosed();
    }


    // BELOW METHODS ARE NOT SUPPORTED

    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateNull(String columnLabel) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateString(String columnLabel, String x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void insertRow() throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateRow() throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void deleteRow() throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void refreshRow() throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void cancelRowUpdates() throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void moveToInsertRow() throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void moveToCurrentRow() throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }

    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLException("This result set is read-only.");
    }
}