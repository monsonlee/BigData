import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.sql.*;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static org.apache.avro.TypeEnum.c;
import static org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.Scope.row;

/**
 * 将Mysql某一数据库中的所有表及视图迁移至HBase
 * <p>
 * 从Mysql导入到HBase中，可以实现一对一，也可以实现多对一
 * 多对一的情况出现在关联查询非常多的情况下
 * <p>
 * 实现多对一的方法：
 * 在HBase中，1、一个Mysql表对应一个列族；2、所有表的字段都作为列族的列存在
 * 在Mysql中，建立一个视图，转化为一对一的实现方式
 */
public class Mysql2HBase {
    public static void main(String[] args) throws Exception {
        //1、获取数据库连接，通过DataBaseMetaData获取数据库中的所有表的名称、结构
        Connection connection = JdbcUtil.getConnection();
        //获取数据库中表及视图的名称
        ArrayList<String> tableNames = JdbcUtil.getTableNames(connection);
        //2、在HBase中创建对应的表
        org.apache.hadoop.hbase.client.Connection hbaseConn = HBaseUtil.getConnection();
        String familyName = "mx_fn"; //列族名
        String nameSpace = "mx_ns:"; //命名空间

        for (String tableName : tableNames) {
            HBaseUtil.createTable(hbaseConn, nameSpace + tableName, familyName);
        }
        //3、从Mysql中读取数据
        for (String tableName : tableNames) {
            Statement statement = connection.createStatement();
            String sql = "select * from " + tableName;
            ResultSet resultSet = statement.executeQuery(sql);
            //获取列名
            List<String> columnNames = getColumnName(resultSet);
            //读取MySQL某张表的数据
            List<ArrayList<String>> allRows = getAllRow(resultSet, columnNames);
            //构造Put对象
            List<Put> puts = createPutList(familyName, columnNames, allRows);
            //插入到HBase中
            insertData(hbaseConn, nameSpace + tableName, puts);
        }
        //4、关闭连接
        connection.close();
        hbaseConn.close();
    }

    /**
     * 插入数据至HBase中
     *
     * @param hbaseConn
     * @param s
     * @param puts
     */
    private static void insertData(org.apache.hadoop.hbase.client.Connection hbaseConn, String s, List<Put> puts) throws IOException {
        Table table = hbaseConn.getTable(TableName.valueOf(s));
        table.put(puts);
        table.close();
    }

    /**
     * 创建Put列表
     *
     * @param familyName
     * @param columnNames
     * @param allRows
     * @return
     */
    private static List<Put> createPutList(String familyName, List<String> columnNames, List<ArrayList<String>> allRows) {
        ArrayList<Put> puts = new ArrayList<>();
        for (ArrayList<String> row : allRows) {
            int size = row.size();
            String rowKeyString = row.get(0);
            for (int i = 1; i < size; i++) {
                String columnName = columnNames.get(i);
                String columnValue = row.get(i);
                if (columnValue != null) {
                    Put put = HBaseUtil.createPut(rowKeyString, familyName.getBytes(), columnName, columnValue);
                    puts.add(put);
                }
            }
        }
        return puts;
    }

    /**
     * 读取MySQL某张表的数据
     *
     * @param resultSet
     * @param columnNames
     * @return
     * @throws SQLException
     */
    private static List<ArrayList<String>> getAllRow(ResultSet resultSet, List<String> columnNames) throws SQLException {
        ArrayList<ArrayList<String>> allRows = new ArrayList<>();
        while (resultSet.next()) {
            ArrayList<String> row = new ArrayList<>();
            for (String columnName : columnNames) {
                Object value = resultSet.getObject(columnName);
                row.add(value == null ? null : value.toString());
            }
            allRows.add(row);
        }
        return allRows;
    }

    /**
     * 获取表的列名
     *
     * @param resultSet
     * @return
     */
    private static List<String> getColumnName(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        List<String> columnNames = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
            columnNames.add(metaData.getColumnName(i));
        }
        return columnNames;
    }

}
