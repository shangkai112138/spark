package com.lxg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class HDFSTest {

    private static String clusterName = "bigdata-om";
    private static String nn1Addr = "10.48.183.134:8020";
    private static String nn2Addr = "10.48.183.135:8020";
    private static String yarnResourceManager1 = "10.48.183.136:8088";
    private static String yarnResourceManager2 = "10.48.183.137:8088";

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String hdfsAddr =  "hdfs://" + clusterName;

       /* conf.set("fs.defaultFS", hdfsAddr);
        conf.set("dfs.nameservices", clusterName);
        conf.set("dfs.ha.namenodes." + clusterName, "nn1,nn2");
        conf.set("dfs.namenode.rpc-address." + clusterName+".nn1", nn1Addr);
        conf.set("dfs.namenode.rpc-address." + clusterName+".nn2", nn2Addr);
        conf.set("dfs.client.failover.proxy.provider." + clusterName,
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        conf.set("dfs.permissions","false");

        conf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab("dfgx_test001/bdoc@FJBDKDC","/root/dfgx_test001.keytab");*/


        String master = args[0];
        SparkSession sparkSession = null;
        if(master.equals("1")){
            sparkSession = SparkSession.builder()
                    .master("local[2]")
                    .appName("xquery" + Math.random())
                    .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
                    .config("spark.speculation", "false")
                    .config("spark.hadoop.mapreduce.map.speculative", "false")
                    .config("spark.hadoop.mapreduce.reduce.speculative", "false")
                    .getOrCreate();
            sparkSession.sparkContext().hadoopConfiguration().set("fs.defaultFS", hdfsAddr);
            sparkSession.sparkContext().hadoopConfiguration().set("dfs.nameservices", clusterName);
            sparkSession.sparkContext().hadoopConfiguration().set("dfs.ha.namenodes." + clusterName, "nn1,nn2");
            sparkSession.sparkContext().hadoopConfiguration().set("dfs.namenode.rpc-address." + clusterName+".nn1", nn1Addr);
            sparkSession.sparkContext().hadoopConfiguration().set("dfs.namenode.rpc-address." + clusterName+".nn2", nn2Addr);
            sparkSession.sparkContext().hadoopConfiguration().set("dfs.client.failover.proxy.provider." + clusterName,
                    "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
            sparkSession.sparkContext().hadoopConfiguration().set("dfs.permissions","false");
            sparkSession.sparkContext().hadoopConfiguration().set("hadoop.security.authentication", "Kerberos");

            UserGroupInformation.setConfiguration(sparkSession.sparkContext().hadoopConfiguration());
            UserGroupInformation.loginUserFromKeytab("dfgx_test001/bdoc@FJBDKDC","/root/dfgx_test001.keytab");
        }else{
            sparkSession = SparkSession.builder()
                    .master("yarn-client")
                    .appName("xquery" + Math.random())
                    .config("spark.yarn.keytab","/root/dfgx_test001.keytab")
                    .config("spark.yarn.principal","dfgx_test001/bdoc@FJBDKDC")
                    .config("spark.hadoop.yarn.resourcemanager.address", yarnResourceManager1)
                    .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
                    .config("spark.speculation", "false")
                    .config("spark.hadoop.mapreduce.map.speculative", "false")
                    .config("spark.hadoop.mapreduce.reduce.speculative", "false")
                    .getOrCreate();
            sparkSession.sparkContext().hadoopConfiguration().set("fs.defaultFS", hdfsAddr);
            sparkSession.sparkContext().hadoopConfiguration().set("dfs.nameservices", clusterName);
            sparkSession.sparkContext().hadoopConfiguration().set("dfs.ha.namenodes." + clusterName, "nn1,nn2");
            sparkSession.sparkContext().hadoopConfiguration().set("dfs.namenode.rpc-address." + clusterName+".nn1", nn1Addr);
            sparkSession.sparkContext().hadoopConfiguration().set("dfs.namenode.rpc-address." + clusterName+".nn2", nn2Addr);
            sparkSession.sparkContext().hadoopConfiguration().set("dfs.client.failover.proxy.provider." + clusterName,
                    "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
            sparkSession.sparkContext().hadoopConfiguration().set("dfs.permissions","false");
            sparkSession.sparkContext().hadoopConfiguration().set("hadoop.security.authentication", "Kerberos");

            UserGroupInformation.setConfiguration(sparkSession.sparkContext().hadoopConfiguration());
            UserGroupInformation.loginUserFromKeytab("dfgx_test001/bdoc@FJBDKDC","/root/dfgx_test001.keytab");
        }


        System.out.println("开始读取本地文件！");
        Dataset<Row> df = sparkSession.read().option("header",true).csv("/root/data.csv");
        System.out.println("本地文件读取完毕！");
        df.show();
        System.out.println("开始写入hdfs");
        String path = hdfsAddr+"/user/data";
        df.coalesce(1).write().option("header",false).mode(SaveMode.Overwrite).csv(path);
        System.out.println("写文件通过，写出路径为:"+path);
        System.out.println("将写入的文件通过spark读取并显示");
        sparkSession.read().option("header",false).csv(path).show();
        sparkSession.stop();

    }
}
