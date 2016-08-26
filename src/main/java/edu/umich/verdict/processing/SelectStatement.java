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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SelectStatement extends ParsedStatement {
    protected TokenStreamRewriter rewriter;

    public SelectStatement(String str, ParseTree tree, TokenStreamRewriter rewriter) {
        super(str, tree);
        this.rewriter = rewriter;
    }

    // used for performance test
    protected String getPerformanceStatement(String table) {
        String sql = "select\n" +
                "sum(extendedprice) as sum_price,\n" +
                "avg(extendedprice) as avg_price\n" +
                "from\n" +
                table + "\n" +
                "where\n" +
                "shipdate <= '1996-01-01'\n" +
                "and linestatus = 'F'";
        return sql;
    }

    // used for running system call (e.g. to clean cache)
    protected String executeCommand(String command) {

        StringBuffer output = new StringBuffer();

        Process p;
        try {
            p = Runtime.getRuntime().exec(command);
            p.waitFor();
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));

            String line = "";
            while ((line = reader.readLine())!= null) {
                output.append(line + "\n");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return output.toString();

    }

    @Override
    public ResultSet run(Configuration conf, DbConnector connector) throws SQLException, InvalidConfigurationException, InvalidSyntaxException {
        ResultSet rs = connector.executeQuery("select 1");
        TransformedQuery transformed = QueryTransformer.forConfig(conf, connector.getMetaDataManager(), this).transform();

        String tablename = "expr_performance.lineitem_1";
        String q = getPerformanceStatement(tablename);
        if (transformed.isChanged()) {
            info("Query:");
            info(q);
            info("\n");
            System.out.println("Method: " + transformed.getMethod() + "Table: " + transformed.getSample());
            rs = connector.executeQuery("uncache table " + tablename);

            for (int i = 0; i < 11; i++) {
                //executeCommand("/home/shijc/code/emptyCache/restartDB");

                long startTime = System.nanoTime();
                rs = connector.executeQuery(q);
                long estimatedTime = System.nanoTime() - startTime;
                System.out.println(estimatedTime / 1000000000.0);
                rs = connector.executeQuery("cache table " + tablename);
            }
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
