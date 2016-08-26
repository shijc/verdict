package edu.umich.verdict.jdbc;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.InvalidConfigurationException;
import edu.umich.verdict.transformation.TransformedQuery;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;

class ExtraColumnsHandler {
    private final ResultSet originalResultSet;
    private final ResultSetMetaData originalMetaData;
    private final TransformedQuery q;
    private final int originalCount;
    private final int aggregatesCount;
    private final boolean autoMode;
    private boolean showIntervals = false, showErrors = false, showRelativeErrors = false, showVariances = false;
    private final ConfidenceInterval[] intervals;
    private final double[] errors, relativeErrors, variances;
    private final boolean[] isNulls, isExtras;
    private final String[] originalAliases;
    private final int[] refColumns;
    private boolean areValuesValid = false;
    private int extraColumnsPerAggregate;
    private TransformedQuery.ExtraColumnType[] extraColumnsTypes;
    private HashMap<String, Integer> columnLabels = new HashMap<>();

    public ExtraColumnsHandler(ResultSet rs, TransformedQuery q, Configuration conf) throws InvalidConfigurationException, SQLException {
        this.originalResultSet = rs;
        this.q = q;
        this.originalCount = q.getOriginalColumnsCount();
        this.originalMetaData = rs.getMetaData();

        extraColumnsPerAggregate = 0;
        aggregatesCount = q.getAggregates().size();

        autoMode = conf.get("approximation").toLowerCase().equals("auto") && q.getExtraColumns().isEmpty();

        if (autoMode) {
            String[] extraColumns = conf.get("error_columns").split(",");
            ArrayList<TransformedQuery.ExtraColumnType> extraColumnsTypesList = new ArrayList<>();
            for (String extraColumn : extraColumns) {
                if (extraColumn.trim().isEmpty())
                    continue;
                extraColumnsPerAggregate++;
                switch (extraColumn.trim()) {
                    case "conf_inv":
                        extraColumnsTypesList.add(TransformedQuery.ExtraColumnType.LOWER_BOUND);
                        extraColumnsTypesList.add(TransformedQuery.ExtraColumnType.UPPER_BOUND);
                        extraColumnsPerAggregate++;
                        showIntervals = true;
                        break;
                    case "abs_err":
                        extraColumnsTypesList.add(TransformedQuery.ExtraColumnType.ABSOLUTE_ERROR);
                        showErrors = true;
                        break;
                    case "rel_err":
                        extraColumnsTypesList.add(TransformedQuery.ExtraColumnType.RELATIVE_ERROR);
                        showRelativeErrors = true;
                        break;
                    case "variance":
                        extraColumnsTypesList.add(TransformedQuery.ExtraColumnType.VARIANCE);
                        showVariances = true;
                        break;
                    default:
                        throw new InvalidConfigurationException("Invalid value for 'error_columns': " + extraColumn);
                }
            }

            this.extraColumnsTypes = extraColumnsTypesList.toArray(new TransformedQuery.ExtraColumnType[extraColumnsTypesList.size()]);

            isExtras = null;
            refColumns = null;
            originalAliases = null;

            for (int i = originalCount + 1; i <= getTotalCount(); i++)
                columnLabels.put(getLabel(i), i);
        } else {
            this.extraColumnsTypes = new TransformedQuery.ExtraColumnType[originalCount];
            isExtras = new boolean[originalCount];
            refColumns = new int[originalCount];
            originalAliases = new String[originalCount];

            for (TransformedQuery.ExtraColumnInfo col : q.getExtraColumns()) {
                int index = col.getColumn() - 1;
                switch (col.getType()) {
                    case ABSOLUTE_ERROR:
                        showErrors = true;
                        break;
                    case RELATIVE_ERROR:
                        showRelativeErrors = true;
                        break;
                    case LOWER_BOUND:
                    case UPPER_BOUND:
                        showIntervals = true;
                        break;
                    case VARIANCE:
                        showVariances = true;
                        break;
                }
                isExtras[index] = true;
                for (int j = 0; j < aggregatesCount; j++) {
                    TransformedQuery.AggregateInfo aggr = q.getAggregates().get(j);
                    if (aggr.getColumn() == col.getAggregateColumn())
                        refColumns[index] = j;
                }
                extraColumnsTypes[index] = col.getType();
                originalAliases[index] = col.getAlias();
                columnLabels.put(getLabel(index + 1), index + 1);
            }
        }
        intervals = showIntervals ? new ConfidenceInterval[aggregatesCount] : null;
        errors = showErrors ? new double[aggregatesCount] : null;
        relativeErrors = showRelativeErrors ? new double[aggregatesCount] : null;
        variances = showVariances ? new double[aggregatesCount] : null;
        isNulls = new boolean[aggregatesCount];
    }

    public void updateValues() throws SQLException {
        int baseIndex = q.getOriginalColumnsCount() + 1;
        int trials = q.getBootstrapTrials();
        for (int j = 0; j < aggregatesCount; j++) {
            TransformedQuery.AggregateInfo aggr = q.getAggregates().get(j);
            isNulls[j] = false;
            if (originalResultSet.getObject(aggr.getColumn()) == null)
                isNulls[j] = true;
            else {
                Double[] bootstrapResults = new Double[trials];
                for (int i = 0; i < trials; i++)
                    bootstrapResults[i] = originalResultSet.getDouble(i + baseIndex);
                baseIndex += trials;
                if (showIntervals || showErrors || showRelativeErrors) {
                    double estimatedAnswer = originalResultSet.getDouble(aggr.getColumn());
                    Arrays.sort(bootstrapResults, Comparator.comparing((Double x) -> Math.abs(x - estimatedAnswer)));
                    double bound = bootstrapResults[(int) Math.ceil(trials * q.getConfidence() - 1)];
                    ConfidenceInterval confidenceInterval = new ConfidenceInterval(estimatedAnswer, bound);
                    if (showIntervals)
                        intervals[j] = confidenceInterval;
                    if (showErrors)
                        errors[j] = Math.abs(bound - estimatedAnswer);
                    if (showRelativeErrors)
                        relativeErrors[j] = (double) Math.round(100 * Math.abs(bound - estimatedAnswer) / estimatedAnswer) / 100;
                }
                if (showVariances) {
                    variances[j] = getVariance(bootstrapResults);
                }
            }
        }
        areValuesValid = true;
    }

    double getMean(Double[] data) {
        double sum = 0.0;
        for (double a : data)
            sum += a;
        return sum / data.length;
    }

    double getVariance(Double[] data) {
        double mean = getMean(data);
        double temp = 0;
        for (double a : data)
            temp += (mean - a) * (mean - a);
        return temp / data.length;
    }

    private Double getValue(int i) {
        int valueIndex = autoMode ? (i - originalCount) / extraColumnsPerAggregate : refColumns[i],
                typeIndex = autoMode ? (i - originalCount) % extraColumnsPerAggregate : i;
        if (isNulls[valueIndex])
            return null;
        switch (extraColumnsTypes[typeIndex]) {
            case LOWER_BOUND:
                return intervals[valueIndex].start;
            case UPPER_BOUND:
                return intervals[valueIndex].end;
            case ABSOLUTE_ERROR:
                return errors[valueIndex];
            case RELATIVE_ERROR:
                return relativeErrors[valueIndex];
            case VARIANCE:
                return variances[valueIndex];
            default:
                return null;
        }
    }

    public int getTotalCount() {
        return autoMode ? getOriginalCount() + getCount() : getOriginalCount();
    }

    public int getCount() {
        return autoMode ? aggregatesCount * extraColumnsPerAggregate : q.getExtraColumns().size();
    }

    public int getOriginalCount() {
        return originalCount;
    }

    public int getDisplaySize(int column) {
        return 22;
    }

    public String getLabel(int column) throws SQLException {
        if (!isExtra(column))
            return this.originalMetaData.getColumnLabel(column);
        int i = column - 1;
        if(!autoMode && !this.originalAliases[i].isEmpty())
            return this.originalAliases[i];
        int valueIndex = autoMode ? (i - originalCount) / extraColumnsPerAggregate : refColumns[i],
                typeIndex = autoMode ? (i - originalCount) % extraColumnsPerAggregate : i;
        String aggregateLabel = originalMetaData.getColumnLabel(q.getAggregates().get(valueIndex).getColumn());
        switch (extraColumnsTypes[typeIndex]) {
            case LOWER_BOUND:
                return "low_bnd__" + aggregateLabel;
            case UPPER_BOUND:
                return "up_bnd__" + aggregateLabel;
            case ABSOLUTE_ERROR:
                return "abs_err__" + aggregateLabel;
            case RELATIVE_ERROR:
                return "rel_err__" + aggregateLabel;
            case VARIANCE:
                return "var__" + aggregateLabel;
            default:
                return null;
        }
    }

    public int getPrecision(int column) {
        //TODO: implement
        return 0;
    }

    public int getScale(int column) {
        //TODO: implement
        return 0;
    }

    public int getType(int column) {
        //TODO: complete
        return Types.DOUBLE;
    }

    public String getTypeName(int column) {
        //TODO: complete
        return "DOUBLE";
    }

    public String getClassName(int column) {
        //TODO: complete
        return Double.class.getName();
    }

    public Double get(int column) throws SQLException {
        //TODO: complete
        if (!areValuesValid)
            updateValues();
        return getValue(column - 1);
    }

    public double getDouble(int column) throws SQLException {
        Double val = get(column);
        return val == null ? 0.0 : val;
    }

    public void invalidateValues() {
        areValuesValid = false;
    }

    public int findColumn(String columnLabel) {
        return columnLabels.getOrDefault(columnLabel, -1);
    }

    public boolean isExtra(int columnIndex) {
        if (autoMode)
            return columnIndex > originalCount;
        return isExtras[columnIndex - 1];
    }
}


class ConfidenceInterval {
    double start, end;

    ConfidenceInterval(double estimatedAnswer, double bound) {
        start = estimatedAnswer - Math.abs(estimatedAnswer - bound);
        end = estimatedAnswer + Math.abs(estimatedAnswer - bound);
    }

    public String toString() {
        return "[" + start + ", " + end + "]";
    }
}
