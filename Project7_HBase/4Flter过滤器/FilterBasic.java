import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import static com.sun.xml.internal.fastinfoset.alphabet.BuiltInRestrictedAlphabets.table;

/**
 * 问题：Filter过滤器基本操作
 * HBase中有张members表，内容如下：
 * 2000|cc|上海|2013-04-11|2014-11-18
 * 2001|Franky|北京|2007-04-11|2010-03-30
 * 2002|陈慧|上海|2009-04-11|2016-04-30
 * 2003|Linda7|深圳|2003-04-11|2004-04-25
 * 2004|Liz|上海|2013-10-11|2015-06-12
 * 2005|bibalily|广州|2002-04-11|2004-04-25
 * 2006|加斐|深圳|2012-04-11|2016-05-03
 * 2007|蒋艳铮|上海|2005-04-11|2007-04-02
 * 2008|张渠|北京|2000-04-11|2004-04-25
 * 2009|骆嫣|上海|2006-04-11|2007-04-25
 * <p>
 * 提供以下查询功能：
 * query(id, name, area, startRegDate, endRegDate, lastDaysLogin)
 * 其中，编号精准匹配、姓名模糊匹配、地区精准匹配、注册时间范围匹配、最近多少天登录。查询条件都不是必填信息。
 */
public class FilterBasic {
    public static void main(String[] args) throws Exception {
        query("2004", null, null, null, null, null);
    }

    /**
     * 设置过滤条件
     * FilterList.Operator.MUST_PASS_ALL，必须全部满足
     * FilterList.Operator.MUST_PASS_ONE，满足任一
     * 默认全部
     *
     * @param id
     * @param name
     * @param area
     * @param startRegDate
     * @param endRegDate
     * @param lastDaysLogin
     * @throws Exception
     */
    private static void query(String id, String name, String area, String startRegDate, String endRegDate, Integer lastDaysLogin) throws Exception {
        //1、获取HBase连接
        Connection connection = HBaseUtil.getConnection();
        //2、获取表
        Table table = connection.getTable(TableName.valueOf("members"));
        //3、设置过滤器
        Scan scan = new Scan(); //客户端缓存
        FilterList filterList = new FilterList();
        //判断id，编号精准匹配
        if (StringUtils.isNotBlank(id)) {
            filterList.addFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(id.getBytes())));
        }
        //判断姓名，姓名模糊匹配
        if (StringUtils.isNotBlank(name)) {
            filterList.addFilter(new SingleColumnValueFilter("f1".getBytes(), "name".getBytes(), CompareFilter.CompareOp.EQUAL, new SubstringComparator(name)));
        }
        //判断地区，地区精准匹配
        if (StringUtils.isNotBlank(area)) {
            filterList.addFilter(new SingleColumnValueFilter("f1".getBytes(), "area".getBytes(), CompareFilter.CompareOp.EQUAL, new BinaryComparator(area.getBytes())));
        }
        //开始注册时间，注册时间范围匹配
        if (StringUtils.isNotBlank(startRegDate)) {
            filterList.addFilter(new SingleColumnValueFilter("f1".getBytes(), "startRegDate".getBytes(), CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryComparator(startRegDate.getBytes())));
        }
        //结束注册时间，注册时间范围匹配
        if (StringUtils.isNotBlank(endRegDate)) {
            filterList.addFilter(new SingleColumnValueFilter("f1".getBytes(), "endDate".getBytes(), CompareFilter.CompareOp.LESS, new BinaryComparator(endRegDate.getBytes())));
        }
        //最近多少天登录
        if (lastDaysLogin != null) {
            Calendar instance = Calendar.getInstance();
            instance.add(Calendar.DAY_OF_MONTH, 0 - lastDaysLogin);
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            String format = simpleDateFormat.format(instance.getTime());
            filterList.addFilter(new SingleColumnValueFilter("f1".getBytes(), "lastdate".getBytes(), CompareFilter.CompareOp.EQUAL, new BinaryComparator(format.getBytes())));
        }
        scan.setFilter(filterList);
        ResultScanner scanner = table.getScanner(scan);
        //4、解析ResultScanner
        for (Result result : scanner) {
            HBaseUtil.print(result); //解析并打印result信息
        }
        //5、关闭
        table.close();
        connection.close();
    }
}
