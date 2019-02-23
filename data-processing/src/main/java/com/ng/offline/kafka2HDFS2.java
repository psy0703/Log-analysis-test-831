package com.ng.offline;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.*;


public class kafka2HDFS2 {

    //相应的属性
    private static FileSystem fs = null;
    private static FSDataOutputStream outputStream = null;
    private static String hdfsPath = "hdfs://psy831:9000";
    private static String user = "psy831";

    public static void main(String[] args) throws IOException {

        //创建配置信息
        Properties props = new Properties();

        //添加配置
        props.put("zookeeper.connect", "psy831:2181");
        props.put("group.id", "g1");
        props.put("zookeeper.session.timeout.ms", "500");
        props.put("zookeeper.sync.time.ms", "250");
        props.put("auto.commit.interval.ms", "1000");

        //创建kafka消费者连接器
        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));

        //创建一个topicCountMap（主题&消费该主题的线程数）
        HashMap<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put("log-analysis", 1);

        //获取消息流
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumerConnector.createMessageStreams(topicCountMap);

        //根据主题以及线程顺序获取数据流
        KafkaStream<byte[], byte[]> stream = messageStreams.get("log-analysis").get(0);

        //
        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();

        //获取当前时间
        long ts = System.currentTimeMillis();

        //创建一个时间格式化对象
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM-dd HH:mm");

        //获取数据存入的路径（yyyyMM/dd/HHmm）
        String totalPath = getPath(ts, sdf);

        try {
            //获取HDFS的文件系统
            fs = FileSystem.get(new URI(hdfsPath), new Configuration(), user);

            //获取输出流
            if (fs.exists(new Path(totalPath))) {
                outputStream = fs.append(new Path(totalPath));
            } else {
                outputStream = fs.create(new Path(totalPath));
            }
        } catch (IOException | InterruptedException | URISyntaxException e) {
            e.printStackTrace();
        }

        while (iterator.hasNext()) {


            //获取一行数据
            String log = new String(iterator.next().message());


            //获取当前时间
            long currentTS = System.currentTimeMillis();

            if (currentTS - ts >= 60000) {
                //重新获取文件路径
                String currentPath = getPath(currentTS, sdf);

                outputStream.close();

                outputStream = fs.create(new Path(currentPath));

                //更新ts
                ts = currentTS;
            }

            System.out.println(log);

            //写入HDFS(按分钟存一个文件)
            outputStream.write((log + "\r\n").getBytes());
            outputStream.hsync();
        }
    }

    private static String getPath(long ts, SimpleDateFormat sdf) {

        //"yyyyMM-dd HH:mm"
        String formatTime = sdf.format(new Date(ts));

        //（yyyyMM/dd/HHmm）
        String[] splits = formatTime.split(" ");

        //取出相应字段
        String yearMonthDay = splits[0];
        String hourMini = splits[1];

        String parentName = yearMonthDay.replaceAll("-", "/");
        String fileName = hourMini.replaceAll(":", "");

        return hdfsPath + "/logData/" + parentName + "/" + fileName;
    }

}
