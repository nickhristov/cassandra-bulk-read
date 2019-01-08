package com.nhhtools.cassandrabulkread;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;

@Component
public class DumpTableDataFromCassandra implements CommandLineRunner {

  private final Session session;
  private final ConfigurableApplicationContext context;

  private static final Options OPTIONS = new Options();

  private static final Option TABLE = new Option("t","table", true, "table to read");
  private static final Option KEYSPACE = new Option("k","k", true, "keyspace to set");
  private static final Option RETRY = new Option("r","retries", true, "maximum retries when failures occur");

  private static final int PAGE_SIZE = 500;

  static {
    TABLE.setRequired(true);

    KEYSPACE.setRequired(true);
    RETRY.setRequired(true);

    OPTIONS.addOption(TABLE);
    OPTIONS.addOption(KEYSPACE);
    OPTIONS.addOption(RETRY);
  }

  public DumpTableDataFromCassandra(
      Session session,
      ConfigurableApplicationContext context) {
    this.session = session;
    this.context = context;
  }

  @Override
  public void run(String... args) throws Exception {

    CommandLineParser parser = new DefaultParser();

    CommandLine commandLine = parser.parse(OPTIONS, args);

    String table = commandLine.getOptionValue("t");
    String keyspace = commandLine.getOptionValue("k");
    String retries = commandLine.getOptionValue("r");

    executeStatement(table, keyspace, Integer.parseInt(retries));
  }

  private void executeStatement(String table, String keyspace, int retries) {

    KeyspaceMetadata keyspaceMetadata = session.getCluster().getMetadata().getKeyspace(keyspace);
    if (keyspaceMetadata == null) {
      throw new IllegalArgumentException("No such keyspace " + keyspace);
    }
    TableMetadata tableMetadata = keyspaceMetadata.getTable(table);
    if (tableMetadata == null) {
      throw new IllegalArgumentException("No such table " + table);
    }

    List<ColumnMetadata> columns = tableMetadata.getColumns();

    String selectBlock = columns.stream()
        .map(ColumnMetadata::getName)
        .collect(Collectors.joining(","));

    boolean done = false;
    PagingState pagingState = null;

    StringBuilder sink = new StringBuilder();
    dumpColumns(columns, sink);
    do {
      try {
        Statement statement = new SimpleStatement("SELECT " + selectBlock + " FROM " + keyspace + "." + table);
        statement.setFetchSize(PAGE_SIZE);

        if (pagingState != null) {
          statement.setPagingState(pagingState);
        }

        ResultSet rs = session.execute(statement);
        for (Row row : rs) {
          pagingState = rs.getExecutionInfo().getPagingState();
          if (rs.getAvailableWithoutFetching() == (PAGE_SIZE/2) && !rs.isFullyFetched()) {
            rs.fetchMoreResults();
          }
          dumpRow(row, columns, sink);
          retries = 0;
        }
        done = true;
      } catch ( NoHostAvailableException | QueryExecutionException exception) {
        System.err.println("ERROR: " + exception.getMessage());
        retries++;
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          // interrupted. abort processing
          return;
        }
      }
    } while (!done);

    context.close();
    context.stop();
  }

  private void dumpColumns(List<ColumnMetadata> columns, StringBuilder sink) {
    for(ColumnMetadata column: columns) {
      sink.append(column.getName());
      sink.append(',');
    }
    sink.deleteCharAt(sink.length()-1);
    System.out.println(sink.toString());
    sink.setLength(0);
  }

  private void dumpRow(Row row, List<ColumnMetadata> columns, StringBuilder builder) {
    int numColumns = columns.size();
    for(int i = 0; i < numColumns; i++) {
      builder.append(row.getObject(i));
      builder.append(',');
    }
    builder.deleteCharAt(builder.length()-1);
    System.out.println(builder.toString());
    builder.setLength(0);
  }
}
