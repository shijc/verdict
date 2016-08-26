package edu.umich.verdict.transformation;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.InvalidSyntaxException;
import edu.umich.verdict.connectors.MetaDataManager;
import edu.umich.verdict.models.Sample;
import edu.umich.verdict.models.StratifiedSample;
import edu.umich.verdict.parser.TsqlBaseVisitor;
import edu.umich.verdict.parser.TsqlParser;
import edu.umich.verdict.processing.SelectStatement;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.TokenStreamRewriter;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class QueryTransformer {
    protected static final List<String> supportedAggregates = Arrays.asList("avg sum count".split(" "));
    protected static final List<String> supportedExtraColumns = Arrays.asList("absolute_error relative_error lower_bound upper_bound".split(" "));
    protected final MetaDataManager metaDataManager;
    protected final TokenStreamRewriter rewriter;
    private Sample sample = null;
    protected SelectStatement q;
    protected TransformedQuery transformed;
    protected int bootstrapTrials;
    protected final double confidence;
    protected final String sampleType;
    protected final double preferredSample;
    protected final boolean showErrors;
    protected final boolean autoMode;

    protected TsqlParser.Select_listContext selectList = null;
    ArrayList<SelectListItem> selectItems = new ArrayList<>();
    //    ArrayList<SelectListItem> groupBys = new ArrayList<>();
    protected String sampleAlias;
    private TsqlParser.Confidence_clauseContext confidenceClause;
    private TsqlParser.Trials_clauseContext trialsClause;
    private TsqlParser.Table_name_with_sampleContext sampleSizeClause;

    public static QueryTransformer forConfig(Configuration conf, MetaDataManager metaDataManager, SelectStatement q) {
        switch (conf.get("bootstrap.method")) {
            case "uda":
                return new UdaTransformer(conf, metaDataManager, q);
            case "udf":
                return new UdfTransformer(conf, metaDataManager, q);
            case "stored":
                return new StoredTransformer(conf, metaDataManager, q);
            default:
                return new IdenticalTransformer(conf, metaDataManager, q);
        }
    }

    public QueryTransformer(Configuration conf, MetaDataManager metaDataManager, SelectStatement q) {
        this.q = q;
        this.metaDataManager = metaDataManager;
        rewriter = q.getRewriter();
        autoMode = conf.get("approximation").toLowerCase().equals("auto");
        confidence = this.getConfidence(conf);
        bootstrapTrials = this.getTrials(conf);
        preferredSample = this.getSampleSize(conf);
        showErrors = !conf.get("error_columns").isEmpty();
        sampleType = conf.get("sample_type").toLowerCase();
        transformed = new TransformedQuery(q, bootstrapTrials, confidence, conf.get("bootstrap.method").toLowerCase());
        if (conf.get("fixed_sample") != null)
            sample = metaDataManager.getSampleByName(conf.get("fixed_sample"));
    }

    public TransformedQuery transform() throws SQLException, InvalidSyntaxException {
        if (!autoMode && sampleSizeClause == null && confidenceClause == null && trialsClause == null) {
            System.out.println("Passing query. No inline indications found.");
            return transformed;
        }
        if (!findAggregates()) {
            System.out.println("No supported aggregate function found.");
            return transformed;
        }
        if (!replaceTableNames()) {
            System.out.println("WARNING: no sample found for this query.");
            return transformed;
        }

        replaceAggregates();
        if (showErrors)
            addBootstrapTrials();

        return transformed;
    }

    protected boolean findAggregates() throws InvalidSyntaxException {
        findSelectList();
        if (selectList == null)
            return false;
        for (TsqlParser.Select_list_elemContext item : selectList.select_list_elem()) {
            SelectListItem itemInfo;
            try {
                itemInfo = new SelectListItem(selectItems.size() + 1, item);
            } catch (Exception e) {
                transformed.getAggregates().clear();
                break;
            }
            selectItems.add(itemInfo);
            if (itemInfo.isSupportedAggregate())
                transformed.addAggregate(itemInfo.getAggregateType(), itemInfo.getInnerExpression(), itemInfo.getIndex());
        }
        outer:
        for (SelectListItem item : selectItems) {
            if (item.isExtraColumn()) {
                for (SelectListItem agg : selectItems) {
                    if (agg.isSupportedAggregate() && agg.getOriginalAlias().equals(item.getAggtegateRef())) {
                        transformed.addExtraColumn(item.getExtraColumnType(), agg.getIndex(), item.getIndex(), item.getOriginalAlias());
                        continue outer;
                    }
                }
                throw new InvalidSyntaxException("Reference '" + item.getAggtegateRef() + "' not found.");
            }
        }
        return !transformed.getAggregates().isEmpty();
    }

    protected void findSelectList() {
        q.getParseTree().accept(new TsqlBaseVisitor<Void>() {
            public Void visitSelect_list(TsqlParser.Select_listContext list) {
                if (selectList != null)
                    // already found
                    return null;
                selectList = list;
                return null;
            }
        });
        if (selectList == null)
            return;
        transformed.setOriginalCols(selectList.select_list_elem().size());
    }

    protected boolean replaceTableNames() {
        TableReplacer replacer = new TableReplacer();
        q.getParseTree().accept(replacer);
        return replacer.replace();
    }

    protected Sample getSample(String tableName) {
        if (sample != null)
            return sample.getTableName().equals(tableName) || sample.getTableName().equals(metaDataManager.getTableNameWithSchema(tableName)) ? sample : null;
        Sample best = null;
        for (Sample s : metaDataManager.getTableSamples(tableName)) {
            if ((s instanceof StratifiedSample && sampleType.equals("uniform")) || (!(s instanceof StratifiedSample) && sampleType.equals("stratified")))
                continue;
            if (best == null)
                best = s;
            else
                best = getPreferred(best, s);
        }
        return best;
    }

    protected Sample getPreferred(Sample sample1, Sample sample2) {
        double diff1 = Math.max(sample1.getCompRatio() / preferredSample, preferredSample / sample1.getCompRatio());
        double diff2 = Math.max(sample2.getCompRatio() / preferredSample, preferredSample / sample2.getCompRatio());
        if (diff1 > 1.2 && diff2 < diff1)
            return sample2;
        if (diff2 > 1.2 && diff1 <= diff2)
            return sample1;
        return sample2.getPoissonColumns() > sample1.getPoissonColumns() ? sample1 : sample2;
    }

//    protected void transformUniform() {
//
//    }

    protected void replaceAggregates() {
        for (SelectListItem item : selectItems)
            if (item.isSupportedAggregate()) {
                if (stratifiedSample())
                    replaceStratifiedAggregate(item);
                else
                    replaceUniformAggregate(item);
            }else if(item.isExtraColumn()){
                replaceExtraColumn(item);
            }
    }

    protected boolean stratifiedSample() {
        return transformed.getSample() instanceof StratifiedSample;
    }

    protected void replaceExtraColumn(SelectListItem item) {
        rewriter.replace(item.ctx.start, item.ctx.stop, "0");
    }

    protected void replaceUniformAggregate(SelectListItem item) {
        double scale = item.getScale();
        if (scale == 1)
            return;
        rewriter.replace(item.ctx.start, item.ctx.stop, scale + "*" + item.expr + " AS " + item.getOuterAlias());
    }

    protected void replaceStratifiedAggregate(SelectListItem item) {
        String expr = item.getInnerExpression(), weightColumn = metaDataManager.getWeightColumn();
        switch (item.aggregateType) {
            case COUNT:
                expr = "sum(case when (" + expr + ") is null then 0 else " + sampleAlias + "." + weightColumn + " end)";
                break;
            case SUM:
                expr = "sum((" + expr + ")*" + sampleAlias + "." + weightColumn + ")";
                break;
            case AVG:
                expr = "sum((" + expr + ")*" + sampleAlias + "." + weightColumn + ")/sum(" + sampleAlias + "." + weightColumn + ")";
                break;
        }

        rewriter.replace(item.ctx.start, item.ctx.stop, expr + " AS " + item.getOuterAlias());
    }

    protected void addBootstrapTrials() {
        boolean stratified = stratifiedSample();
        StringBuilder buf = new StringBuilder();
        for (SelectListItem item : selectItems) {
            if (item.isSupportedAggregate()) {
                if (stratified)
                    buf.append(getStratifiedBootstrapTrials(item));
                else
                    buf.append(getUniformBootstrapTrials(item));
            }
        }
        buf.append(" ");
        rewriter.insertAfter(selectList.stop, buf.toString());
    }

    protected String getUniformBootstrapTrials(SelectListItem item) {
        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < bootstrapTrials; i++)
            buf.append(", ").append(item.getScale()).append("*(").append(getUniformTrialExpression(item, i + 1)).append(")");
        return buf.toString();
    }

    protected String getStratifiedBootstrapTrials(SelectListItem item) {
        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < bootstrapTrials; i++)
            buf.append(", ").append(getStratifiedTrialExpression(item, i + 1));
        return buf.toString();
    }
//
//    protected void transformStratified() throws SQLException {
////        analyzeGroupBy();
////        addInnerStratifiedBootstrapTrials();
////        joinWithWeightTable();
////        addGroupByStrataColumns();
//    }

//    protected void analyzeGroupBy() throws SQLException {
//        TsqlParser.Query_specificationContext selectCtx = (TsqlParser.Query_specificationContext) selectList.getParent();
//        if (selectCtx.GROUP() == null)
//            return;
//        outer:
//        for (TsqlParser.Group_by_itemContext ctx : selectCtx.group_by_item()) {
//            String expr = transformed.getOriginalText(ctx.expression());
//            for (SelectListItem item : selectItems)
//                if (item.isForExpression(expr)) {
//                    groupBys.add(item);
//                    continue outer;
//                }
//            throw new SQLException("GROUP BY item '" + expr + "' should be present in the SELECT list.");
//        }
//    }

//    protected void addInnerStratifiedBootstrapTrials() {
//        StringBuilder buf = new StringBuilder();
//        for (SelectListItem item : selectItems) {
//            item.addInnerAlias();
//            if (item.isSupportedAggregate()) {
//                buf.append(", ").append(getInnerStratifiedBootstrapTrials(item));
//            }
//        }
//        buf.append(" ");
//        rewriter.insertAfter(selectList.stop, buf.toString());
//    }

//    protected String getInnerStratifiedBootstrapTrials(SelectListItem item) {
//        StringBuilder buf = new StringBuilder();
//        String aliasPrefix = item.getInnerAlias() + "_";
//        for (int i = 0; i < bootstrapTrials; i++)
//            buf.append(getUniformTrialExpression(item, i + 1)).append(" AS ").append(aliasPrefix).append(i).append(", ");
//        buf.replace(buf.length() - 2, buf.length(), "");
//        return buf.toString();
//    }

    protected abstract String getUniformTrialExpression(SelectListItem item, int trial);

    protected abstract String getStratifiedTrialExpression(SelectListItem item, int trial);

    public double getConfidence(Configuration conf) {
        q.getParseTree().accept(new TsqlBaseVisitor<Void>() {
            public Void visitConfidence_clause(TsqlParser.Confidence_clauseContext cls) {
                if (confidenceClause != null)
                    // already found
                    return null;
                confidenceClause = cls;
                return null;
            }
        });
        if (confidenceClause == null)
            return conf.getPercent("confidence");
        rewriter.delete(confidenceClause.start, confidenceClause.stop);
        return confidenceClause.percent != null ? Float.parseFloat(confidenceClause.confidence.getText()) / 100 : Float.parseFloat(confidenceClause.confidence.getText());
    }

    public int getTrials(Configuration conf) {
        q.getParseTree().accept(new TsqlBaseVisitor<Void>() {
            public Void visitTrials_clause(TsqlParser.Trials_clauseContext cls) {
                if (trialsClause != null)
                    // already found
                    return null;
                trialsClause = cls;
                return null;
            }
        });
        if (trialsClause == null)
            return conf.getInt("bootstrap.trials");
        rewriter.delete(trialsClause.start, trialsClause.stop);
        return Integer.parseInt(trialsClause.trials.getText());
    }

    public double getSampleSize(Configuration conf) {
        q.getParseTree().accept(new TsqlBaseVisitor<Void>() {
            public Void visitTable_name_with_sample(TsqlParser.Table_name_with_sampleContext cls) {
                if (sampleSizeClause != null)
                    // already found
                    return null;
                sampleSizeClause = cls;
                return null;
            }
        });
        if (sampleSizeClause == null)
            return conf.getPercent("sample_size");
        return sampleSizeClause.percent != null ? Double.parseDouble(sampleSizeClause.size.getText()) / 100 : Double.parseDouble(sampleSizeClause.size.getText());
    }

//    private void joinWithWeightTable() {
//        StratifiedSample sample = (StratifiedSample) transformed.getSample();
//        StringBuilder buf1 = new StringBuilder("select ");
//        for (SelectListItem item : selectItems)
//            if (item.isSupportedAggregate()) {
//                buf1.append("sum(").append("v__w.").append(item.getWeightColumn()).append("*v__sr.").append(item.getInnerAlias()).append(") AS ").append(item.getOuterAlias()).append(", ");
//            } else {
//                buf1.append("v__sr.").append(item.getInnerAlias()).append(" AS ").append(item.getOuterAlias());
//            }
//        buf1.replace(buf1.length() - 2, buf1.length(), "");
//        for (SelectListItem item : selectItems)
//            if (item.isSupportedAggregate()) {
//                buf1.append(getOuterStratifiedBootstrapTrials(item));
//            }
//        buf1.append(" from (");
//        rewriter.insertBefore(selectList.getParent().start, buf1.toString());
//
//        StringBuilder buf2 = new StringBuilder(") as v__sr JOIN ");
////        buf2.append(metaDataManager.getWeightsTable(sample))
////                .append(" AS v__w ON ");
//        for (String col : sample.getStrataColumns())
//            buf2.append("v__sr.v__sc_").append(col).append("=").append("v__w.").append(col).append(" and ");
//        buf2.replace(buf2.length() - 4, buf2.length(), "");
//        if (!groupBys.isEmpty()) {
//            buf2.append(" group by ");
//            for (SelectListItem item : groupBys)
//                buf2.append(item.getInnerAlias()).append(", ");
//            buf2.replace(buf2.length() - 2, buf2.length(), "");
//        }
//        rewriter.insertAfter(selectList.getParent().stop, buf2.toString());
//    }

//    protected String getOuterStratifiedBootstrapTrials(SelectListItem item) {
//        StringBuilder buf = new StringBuilder();
//        String aliasPrefix = item.getInnerAlias() + "_";
//        for (int i = 0; i < bootstrapTrials; i++)
//            buf.append(", ").append("sum(v__w.").append(item.getWeightColumn()).append("*").append(aliasPrefix).append(i).append(")");
//        return buf.toString();
//    }

//    private void addGroupByStrataColumns() {
//        //Add columns into the select list
//        //TODO: handle if strataCols already exist
//        String[] cols = ((StratifiedSample) transformed.getSample()).getStrataColumns();
//        StringBuilder buf1 = new StringBuilder();
//        for (String col : cols)
//            buf1.append(", ").append(sampleAlias).append(".").append(col).append(" AS ").append("v__sc_").append(col);
//        rewriter.insertAfter(selectList.stop, buf1.toString());
//
//        //Add columns into the GROUP BY clause
//        TsqlParser.Query_specificationContext selectCtx = (TsqlParser.Query_specificationContext) selectList.getParent();
//        Token location;
//        StringBuilder buf = new StringBuilder();
//        if (groupBys.isEmpty()) {
//            buf.append(" group by ");
//            if (selectCtx.where != null) {
//                location = selectCtx.where.stop;
//            } else {
//                location = selectCtx.table_source(selectCtx.table_source().size() - 1).stop;
//            }
//        } else {
//            location = selectCtx.group_by_item(groupBys.size() - 1).stop;
//            buf.append(", ");
//        }
//        for (String col : cols)
//            buf.append("v__sc_").append(col).append(", ");
//        buf.replace(buf.length() - 2, buf.length(), "");
//        rewriter.insertAfter(location, buf.toString());
//    }

    protected class SelectListItem {
        private int index;
        private String expr;
        private String innerExpr = null;
        private String aggr = null;
        private TransformedQuery.AggregateType aggregateType = TransformedQuery.AggregateType.NONE;
        private String alias = "";
        private boolean isSupportedAggregate = false;
        private TsqlParser.Select_list_elemContext ctx;
        private String aggregateColumnRef;
        private boolean isExtraColumn = false;
        private TransformedQuery.ExtraColumnType extraColumnType = null;

        public SelectListItem(int index, TsqlParser.Select_list_elemContext ctx) throws Exception {
            this.ctx = ctx;
            if (ctx.expression() == null)
                // probably item is *
                //TODO: better exception
                throw new Exception("In appropriate expression.");
            this.index = index;
            this.expr = transformed.getOriginalText(ctx.expression());
            if (ctx.column_alias() != null)
                alias = ctx.column_alias().getText();
            TsqlParser.ExpressionContext exprCtx = ctx.expression();
            if (exprCtx instanceof TsqlParser.Function_call_expressionContext) {
                // it's a function call
                TsqlParser.Aggregate_windowed_functionContext aggCtx = ((TsqlParser.Function_call_expressionContext) exprCtx).function_call().aggregate_windowed_function();
                if (aggCtx != null) {
                    // its aggregate function
                    if (aggCtx.over_clause() != null)
                        return;
                    if (aggCtx.all_distinct_expression() == null) {
                        // count(*)
                        innerExpr = "1";
                    } else {
                        if (aggCtx.all_distinct_expression().DISTINCT() != null)
                            return;
                        innerExpr = transformed.getOriginalText(aggCtx.all_distinct_expression().expression());
                    }
                    aggr = aggCtx.getChild(0).getText();
                    if (supportedAggregates.contains(aggr.toLowerCase())) {
                        isSupportedAggregate = true;
                        aggregateType = TransformedQuery.AggregateType.valueOf(aggr.toUpperCase());
                    } else
                        aggregateType = TransformedQuery.AggregateType.OTHER;
                } else if (((TsqlParser.Function_call_expressionContext) exprCtx).function_call().scalar_function_name() != null &&
                        ((TsqlParser.Function_call_expressionContext) exprCtx).function_call().scalar_function_name().func_proc_name() != null) {
                    String functionName = ((TsqlParser.Function_call_expressionContext) exprCtx).function_call().scalar_function_name().func_proc_name().getText();
                    if (supportedExtraColumns.contains(functionName.toLowerCase())) {
                        if (((TsqlParser.Function_call_expressionContext) exprCtx).function_call().expression_list() == null)
                            throw new Exception("Invalid error column definition.");
                        aggregateColumnRef = ((TsqlParser.Function_call_expressionContext) exprCtx).function_call().expression_list().getText();
                        isExtraColumn = true;
                        extraColumnType = TransformedQuery.ExtraColumnType.valueOf(functionName.toUpperCase());
                    }
                }
            }
        }

        protected double getScale() {
            if (stratifiedSample())
                return 1;
            switch (getAggregateType()) {
                case SUM:
                case COUNT:
                    return 1 / transformed.getSample().getCompRatio();
                default:
                    return 1;
            }
        }

        public String getOriginalAlias() {
            return this.alias;
        }

        public int getIndex() {
            return index;
        }

        public String getInnerExpression() {
            return innerExpr;
        }

        public TransformedQuery.AggregateType getAggregateType() {
            return aggregateType;
        }

        public boolean isSupportedAggregate() {
            return isSupportedAggregate;
        }

        public void addInnerAlias() {
            rewriter.replace(ctx.start, ctx.stop, expr + " AS " + getInnerAlias());
        }

        public String getInnerAlias() {
            return "v__c" + index;
        }

        public String getOuterAlias() {
            return getOriginalAlias().isEmpty() ? metaDataManager.getIdentifierWrappingChar() + expr + metaDataManager.getIdentifierWrappingChar() : getOriginalAlias();
        }

        public String getWeightColumn() {
            return aggregateType == TransformedQuery.AggregateType.AVG ? "weight" : "ratio";
        }

        public boolean isForExpression(String expr) {
            return this.expr.equals(expr) || this.getOriginalAlias().equals(expr);
        }

        public boolean isExtraColumn() {
            return isExtraColumn;
        }

        public TransformedQuery.ExtraColumnType getExtraColumnType() {
            return extraColumnType;
        }

        public String getAggtegateRef() {
            return aggregateColumnRef;
        }
    }

    private class TableReplacer extends TsqlBaseVisitor<Void> {
        private TsqlParser.Table_source_itemContext sourceTableCtx;

        public Void visitTable_source_item(TsqlParser.Table_source_itemContext ctx) {
            if (ctx.table_name_with_hint() == null && ctx.table_name_with_sample() == null)
                // table_source_item is a sub-query or something else (not a table reference)
                return null;
            Sample sample = (ctx.table_name_with_sample() != null) ?
                    getSample(ctx.table_name_with_sample().table_name().getText()) :
                    getSample(ctx.table_name_with_hint().table_name().getText());
            if (sample != null) {
                if (transformed.getSample() != null && transformed.getSample().getTableSize() >= sample.getTableSize())
                    // already found a sample for a bigger table
                    return null;
                sourceTableCtx = ctx;
                transformed.setSample(sample);
            }
            return null;
        }

        public boolean replace() {
            Sample sample = transformed.getSample();
            if (sample != null) {
                TsqlParser.Table_nameContext nameCtx = (sourceTableCtx.table_name_with_sample() != null) ?
                        sourceTableCtx.table_name_with_sample().table_name() : sourceTableCtx.table_name_with_hint().table_name();
                ParserRuleContext tableContext = (sourceTableCtx.table_name_with_sample() != null) ?
                        sourceTableCtx.table_name_with_sample() : sourceTableCtx.table_name_with_hint().table_name();
                if (sourceTableCtx.as_table_alias() == null) {
                    // if there is no alias, we add an alias equal to the original table name to eliminate side-effects of this change in other parts of the query
                    rewriter.replace(tableContext.start, tableContext.stop, metaDataManager.getSampleFullName(sample) + " AS " + nameCtx.table.getText());
                    sampleAlias = nameCtx.table.getText();
                } else {
                    rewriter.replace(tableContext.start, tableContext.stop, metaDataManager.getSampleFullName(sample));
                    sampleAlias = sourceTableCtx.as_table_alias().table_alias().getText();
                }
                return true;
            }
            return false;
        }
    }
}