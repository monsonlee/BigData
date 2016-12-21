import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.ArrayList;

import static com.sun.xml.internal.fastinfoset.alphabet.BuiltInRestrictedAlphabets.table;

/**
 * 大量数据导入HBase，
 * 三种方式比较及优化
 * 第一种，单条导入，10W条记录耗时2-3分钟，单线程
 * 第二种，批量导入，10w条记录耗时3s，100w条记录耗时25s，单线程(1亿条记录利用多线程)
 * 第三种，利用BufferedMutator批量导入，10w条记录耗时2s，100w条记录耗时22s，单线程(1亿条记录利用多线程)
 */
public class Data2HBase1 {
    public static void main(String[] args) throws Exception {
        //获得连接
        Connection connection = HBaseUtil.getConnection();
//        singleRowImport(connection);//单条导入
//        batchRowImport(connection);//批量导入
//        bmImport(connection); //利用BufferedMutator批量导入
        //关闭连接
        connection.close();
    }

    /**
     * 单条数据导入
     *
     * @param connection
     * @return
     * @throws IOException
     */
    private static void singleRowImport(Connection connection) throws IOException {
        Table table = connection.getTable(TableName.valueOf("t3"));
        byte[] columnFamily = "f1".getBytes();

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 99999; i++) {
            table.put(HBaseUtil.createPut(i + "", columnFamily, "c1", i + ""));
        }
        table.close();
        System.out.println("共耗时：" + (System.currentTimeMillis() - startTime) + "ms");
    }

    /**
     * 批量导入
     *
     * @param connection
     * @throws IOException
     */
    private static void batchRowImport(Connection connection) throws IOException {
        Table table = connection.getTable(TableName.valueOf("t3"));
        byte[] columnFamily = "f1".getBytes();

        long startTime = System.currentTimeMillis();
        ArrayList<Put> puts = new ArrayList<Put>();
        for (int i = 0; i < 99999; i++) {
            puts.add(HBaseUtil.createPut(i + "", columnFamily, "c1", i + ""));
            //每10000条导入一次
            if (i % 10000 == 0) {
                table.put(puts);
                puts.clear();
            }
        }
        table.put(puts);
        table.close();
        System.out.println("共耗时：" + (System.currentTimeMillis() - startTime) + "ms");
    }

    /**
     * 利用BufferedMutator批量导入
     *
     * @param connection
     * @throws IOException
     */
    private static void bmImport(Connection connection) throws IOException {
        BufferedMutator bufferedMutator = connection.getBufferedMutator(TableName.valueOf("t3"));
        byte[] columnFamily = "f1".getBytes();

        long startTime = System.currentTimeMillis();
        ArrayList<Put> puts = new ArrayList<Put>();
        for (int i = 0; i < 999999; i++) {
            puts.add(HBaseUtil.createPut(i + "", columnFamily, "c1", i + ""));
            //每10000条导入一次
            if (i % 10000 == 0) {
                bufferedMutator.mutate(puts);
                puts.clear();
            }
        }
        //批量调用
        bufferedMutator.mutate(puts);
        bufferedMutator.close();
        System.out.println("共耗时：" + (System.currentTimeMillis() - startTime) + "ms");
    }
}
