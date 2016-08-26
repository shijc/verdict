package edu.umich.verdict.processing;

import edu.umich.verdict.Configuration;
import edu.umich.verdict.InvalidConfigurationException;
import edu.umich.verdict.InvalidSyntaxException;
import edu.umich.verdict.connectors.DbConnector;
import edu.umich.verdict.jdbc.VResultSet;
import edu.umich.verdict.jdbc.VStatement;
import edu.umich.verdict.models.StratifiedSample;
import edu.umich.verdict.transformation.QueryTransformer;
import edu.umich.verdict.transformation.TransformedQuery;
import org.antlr.v4.runtime.TokenStreamRewriter;
import org.antlr.v4.runtime.tree.ParseTree;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SelectStatement extends ParsedStatement {
    protected TokenStreamRewriter rewriter;

    public SelectStatement(String str, ParseTree tree, TokenStreamRewriter rewriter) {
        super(str, tree);
        this.rewriter = rewriter;
    }

    @Override
    public ResultSet run(Configuration conf, DbConnector connector) throws SQLException, InvalidConfigurationException, InvalidSyntaxException {
        ResultSet rs;
        TransformedQuery transformed = QueryTransformer.forConfig(conf, connector.getMetaDataManager(), this).transform();
        String q = transformed.toString();
        if (transformed.isChanged()) {
//            info("New Query:");
//            info(q);
//            info("\n");
            info("Using Sample: " + transformed.getSample().getName() + " Size: " + (transformed.getSample().getCompRatio() *
                    100) + "%" + " Type: " + (transformed.getSample() instanceof StratifiedSample ? "Stratified" : "Uniform"));
//            info("Bootstrap Trials: " + transformed.getBootstrapTrials());
//            info("Method: " + transformed.getMethod());
            rs = connector.executeQuery(q);
            rs = new VResultSet(rs, transformed, conf);
        } else {
            info("Running the original query...");
            rs = connector.executeQuery(q);
        }
        return rs;
    }

    public TokenStreamRewriter getRewriter() {
        return rewriter;
    }

}
