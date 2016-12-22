import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

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
        conf.set("hbase.zookeeper.quorum", "slave1,slave2,slave3");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        ConnectionFactory.createConnection(conf);
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
}
