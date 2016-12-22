import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import static org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.Scope.col;

/**
 * HBase工具类
 */
public class HBaseUtil {
    /**
     * 获得连接
     *
     * @return
     * @throws IOException
     */
    public static Connection getConnection() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "crxy107");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        return ConnectionFactory.createConnection(conf);
    }

    /**
     * 创建表
     *
     * @param admin
     * @param tableNameString
     * @param columnFamily
     * @throws IOException
     */
    public static void createTable(Connection connection, String tableNameString, String columnFamily) throws IOException {
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf(tableNameString); //d2h (data to HBase)
        HTableDescriptor table = new HTableDescriptor(tableName);
        HColumnDescriptor family = new HColumnDescriptor(columnFamily);
        table.addFamily(family);
        //判断表是否已经存在
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        admin.createTable(table);
    }

    /**
     * 获取插入HBase的操作put
     *
     * @param rowKeyString
     * @param familyName
     * @param columnName
     * @param columnValue
     * @return
     */
    public static Put createPut(String rowKeyString, byte[] familyName, String columnName, String columnValue) {
        byte[] rowKey = rowKeyString.getBytes();
        Put put = new Put(rowKey);
        put.addColumn(familyName, columnName.getBytes(), columnValue.getBytes());
        return put;
    }

    /**
     * 打印HBase查询结果
     *
     * @param result
     */
    public static void print(Result result) {
        //result是个四元组<行键，列族，列(标记符)，值>
        byte[] row = result.getRow(); //行键
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();
        for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyEntry : map.entrySet()) {
            byte[] familyBytes = familyEntry.getKey(); //列族
            for (Map.Entry<byte[], NavigableMap<Long, byte[]>> entry : familyEntry.getValue().entrySet()) {
                byte[] column = entry.getKey(); //列
                for (Map.Entry<Long, byte[]> longEntry : entry.getValue().entrySet()) {
                    Long time = longEntry.getKey(); //时间戳
                    byte[] value = longEntry.getValue(); //值
                    System.out.println(String.format("行键rowKey=%s,列族columnFamily=%s,列column=%s,时间戳timestamp=%d,值value=%s", new String(row), new String(familyBytes), new String(column), time, new String(value)));
                }
            }
        }

    }
}
