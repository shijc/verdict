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

    protected String getRescalingStatement(Configuration conf, String tablename) {
        int poisson = Integer.parseInt(conf.get("bootstrap.trials"));
        String q = "select sum((extendedprice)*lineitem40.v__weight) AS sum_base_price, sum((extendedprice)*lineitem40.v__weight)/sum(lineitem40.v__weight) AS avg_price";

        q = q + ", sum((t.AVG_Y2 + POW(t.SUM_P1*(1 - 1/lineitem40.v__weight)/(t.COUNT_ALL - 1),0.5)*(extendedprice - t.AVG_Y2)) *(lineitem40.v__weight * t.COUNT_ALL / t.SUM_P1)*v__p1)";

        for (int i = 2; i <= poisson; i++) {
            q = q + ", sum((t.AVG_Y2 + POW(t.SUM_P" + i + "*(1 - 1/lineitem40.v__weight)/(t.COUNT_ALL - 1),0.5)*(extendedprice - t.AVG_Y2)) *(lineitem40.v__weight * t.COUNT_ALL / t.SUM_P" + i + ")*v__p";
            q = q + i;
            q = q + ")";
        }

        q = q + ", sum((t.AVG_Y2 + POW(t.SUM_P1*(1 - 1/lineitem40.v__weight)/(t.COUNT_ALL - 1),0.5)*(extendedprice - t.AVG_Y2)) *(lineitem40.v__weight * t.COUNT_ALL / t.SUM_P1)*v__p1) / sum((lineitem40.v__weight * t.COUNT_ALL / t.SUM_P1)*v__p1)";

        for (int i = 2; i <= poisson; i++) {
            q = q + ", sum((t.AVG_Y2 + POW(t.SUM_P" + i + "*(1 - 1/lineitem40.v__weight)/(t.COUNT_ALL - 1),0.5)*(extendedprice - t.AVG_Y2)) *(lineitem40.v__weight * t.COUNT_ALL / t.SUM_P" + i + ")*v__p" + i + ")/ sum((lineitem40.v__weight * t.COUNT_ALL / t.SUM_P" + i + ")*v__p";
            q = q + i;
            q = q + ")";
        }

        q = q + " from " + tablename + " as lineitem40, (SELECT v__weight, AVG(extendedprice) AS AVG_Y2, COUNT(*) AS COUNT_ALL";

        for (int i = 1; i <= poisson; i++) {
            q = q + ", SUM(V__P" + i + ") AS SUM_P" + i;
        }

        q = q + " from " + tablename  + " as lineitem40 where shipdate <= '1996-01-01' and linestatus = 'F' group by v__weight) as t";

        q = q + " where shipdate <= '1996-01-01' and linestatus = 'F' and t.v__weight = lineitem40.v__weight";

        return q;

    }

    protected void runRescaling(Configuration conf, DbConnector connector, String tablesize, String percentage) throws SQLException, InvalidConfigurationException {
        String tablename = "expr_performance.s_s" + tablesize + "_" + percentage;
        String q;

        System.out.println("------Rescaling " + tablesize + "G " + percentage + "%------");
        q = getRescalingStatement(conf,tablename);
        ResultSet rs = connector.executeQuery("uncache table `" + tablename + "`");
        for (int i = 0; i < 6; i++) {
            //executeCommand("/home/shijc/code/emptyCache/restartDB");
            long startTime = System.nanoTime();
            rs = connector.executeQuery(q);
            long estimatedTime = System.nanoTime() - startTime;
            System.out.println(estimatedTime / 1000000000.0);
            rs = connector.executeQuery("cache table `" + tablename + "`");
        }
    }

    protected void runTable(DbConnector connector, String tablename) throws SQLException, InvalidConfigurationException {

        ResultSet rs = connector.executeQuery("uncache table `" + tablename + "`");

        String q = "select sum(extendedprice) as sum_price, "
                + "avg(extendedprice) as avg_price from "
                + tablename
                +" where shipdate <= '1996-01-01'\n"
                + "and linestatus = 'F'";

        for (int i = 0; i < 6; i++) {
            long startTime = System.nanoTime();
            rs = connector.executeQuery(q);
            long estimatedTime = System.nanoTime() - startTime;
            System.out.println(estimatedTime / 1000000000.0);
            rs = connector.executeQuery("cache table `"
                    + tablename + "`");
        }
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

        String tablename = "expr_performance.s_" + transformed.getSample().getName() + "";
        String q = transformed.toString();
        //String tablename = "default.lineitem40";
        //String q = getPerformanceStatement(tablename);
        if (transformed.isChanged()) {

            runRescaling(conf,connector,"1","01");
            runRescaling(conf,connector,"10","01");
            runRescaling(conf,connector,"30","01");

            System.out.println("--- Run 0.1% ---");
            runTable(connector, "expr_performance.l1_01");
            runTable(connector, "expr_performance.l10_01");
            runTable(connector, "expr_performance.l30_01");

            System.out.println("--- Run 1% ---");
            runTable(connector, "expr_performance.l1_1");
            runTable(connector, "expr_performance.l10_1");
            runTable(connector, "expr_performance.l30_1");

            System.out.println("--- Run 2% ---");
            runTable(connector, "expr_performance.l1_2");
            runTable(connector, "expr_performance.l10_2");
            runTable(connector, "expr_performance.l30_2");

            System.out.println("--- Run 5% ---");
            runTable(connector, "expr_performance.l1_5");
            runTable(connector, "expr_performance.l10_5");
            runTable(connector, "expr_performance.l30_5");

            System.out.println("--- Run original datasets ---");
            runTable(connector, "expr_performance.lineitem_1");
            runTable(connector, "expr_performance.lineitem_10");
            runTable(connector, "default.lineitem40");










            runRescaling(conf,connector,"1","1");
            runRescaling(conf,connector,"10","1");
            runRescaling(conf,connector,"30","1");

            runRescaling(conf,connector,"1","2");
            runRescaling(conf,connector,"10","2");
            runRescaling(conf,connector,"30","2");

            runRescaling(conf,connector,"1","5");
            runRescaling(conf,connector,"10","5");
            runRescaling(conf,connector,"30","5");


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
