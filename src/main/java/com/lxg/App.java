package com.lxg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class App {
    public static void main(String[] args){
        String path1 = "/user/bcloud/notebook/googleplaystore.csv";
        String path2 = "/user/bcloud/test/lxg_test3.csv";
        hdfsTest(path1,path2);
        //copyFromLocalFile("/root/fitest/lxg_test.csv",path2);

    }

    public static void hdfsTest(String srcPath,String tarPath) {
        copyTolocal(srcPath,"/root/fitest/lxg_test.csv");
        copyFromLocalFile("/root/fitest/lxg_test.csv",tarPath);
       /* FileSystem fs_fi = null;
        FileSystem fs_bonc = null;
        Map<String,Object> map1 = getFiHdfsAddrAndConf();
        Map<String,Object> map2 = getCommonHdfsAddrAndConf();
        try{
            fs_fi = FileSystem.get(new URI(String.valueOf(map1.get("hdfsAddr"))), (Configuration)map1.get("conf"),"bcloud");
            fs_bonc = FileSystem.get(new URI(String.valueOf(map2.get("hdfsAddr"))), (Configuration)map2.get("conf"),"hadoop");

            FSDataInputStream inputStream = fs_fi.open(new Path(srcPath));
            FSDataOutputStream outputStream = fs_bonc.create(new Path(tarPath), true); // 输出流到HDFS
            IOUtils.copyBytes(inputStream,outputStream,1024);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            closeFileSystem(fs_bonc);
            closeFileSystem(fs_fi);
        }*/
    }

    public static void copyTolocal(String srcPath,String decPath) {
        Map<String,Object> map2 = getFiHdfsAddrAndConf();
        System.setProperty("HADOOP_USER_NAME", "bcloud");
        FileSystem fs = null;
        try {
            fs = FileSystem.get(new URI(String.valueOf(map2.get("hdfsAddr"))), (Configuration)map2.get("conf"));
            System.out.println("华为fi"+fs);
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            fs.copyToLocalFile(new Path(srcPath),new Path(decPath));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeFileSystem(fs);
            fs = null;
            System.out.println("关闭后华为fi"+fs);
        }
    }


    public static void copyFromLocalFile(String srcPath,String localPath) {
        System.out.println("系统当前HADOOP_USER_NAME："+System.getProperty("HADOOP_USER_NAME"));
        System.setProperty("HADOOP_USER_NAME", "abc");
        System.out.println("切换后，系统HADOOP_USER_NAME："+System.getProperty("HADOOP_USER_NAME"));
        Map<String,Object> map2 = getCommonHdfsAddrAndConf();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(new URI(String.valueOf(map2.get("hdfsAddr"))), (Configuration)map2.get("conf"));
            System.out.println("常规1："+fs);
            fs = FileSystem.get(new URI(String.valueOf(map2.get("hdfsAddr"))), (Configuration)map2.get("conf"),"hadoop");
            System.out.println("常规2："+fs);
           /* fs = FileSystem.newInstance(new URI(String.valueOf(map2.get("hdfsAddr"))), (Configuration)map2.get("conf"));
            System.out.println("常规3："+fs);
            fs = FileSystem.newInstance(new URI(String.valueOf(map2.get("hdfsAddr"))), (Configuration)map2.get("conf"),"hadoop");
            System.out.println("常规4："+fs);*/
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            fs.copyFromLocalFile(new Path(srcPath),new Path(localPath));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeFileSystem(fs);
            System.out.println("关闭后常规："+fs);
        }
    }

    //常规hdfs连接
    public static Map<String,Object> getCommonHdfsAddrAndConf(){
        Map<String,Object> map = new HashMap<String,Object>();
        Configuration conf = new Configuration();
        String clusterName = "beh001";
        String nameNodeAddresses = "10.130.2.22:8020,10.130.2.23:8020";
        String hdfsAddress = "hdfs://" + clusterName;
        String[] addresses = nameNodeAddresses.split(",");

        String sysPath = System.getProperty("user.dir");
        //conf.addResource(new Path(sysPath + File.separator +"common" + File.separator + "hdfs-site.xml"));
        //conf.addResource(new Path(sysPath + File.separator +"common" + File.separator + "core-site.xml"));

        conf.set("fs.defaultFS", hdfsAddress);
        conf.set("dfs.nameservices", clusterName);
        conf.set("dfs.ha.namenodes." + clusterName, "nn1, nn2");
        conf.set("dfs.permissions", "false");

        //
        conf.set("ipc.client.fallback-to-simple-auth-allowed", "true");

        conf.set("hadoop.security.authentication", "simple");
        conf.set("dfs.permissions.enabled", "false");
        conf.set("hadoop.security.authorization", "false");

        for (int i = 1; i <= addresses.length; i++) {
            conf.set("dfs.namenode.rpc-address." + clusterName + ".nn" + i, addresses[i - 1]);
        }
        conf.set("dfs.client.failover.proxy.provider." + clusterName,
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");

        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("fs.hdfs.impl.disable.cache", "true");

        map.put("hdfsAddr",hdfsAddress);
        map.put("conf",conf);
        return map;
    }

    //华为fi  HDFS
    public static Map<String,Object> getFiHdfsAddrAndConf(){
        //System.setProperty("USERDNSDOMAIN", "bcloud");
        System.out.println( System.getProperty("USERDNSDOMAIN"));
        Map<String,Object> map = new HashMap<String,Object>();
        String sysPath = System.getProperty("user.dir");
        System.out.println("sysPath:"+sysPath);
        Configuration conf = new Configuration();
        conf.addResource(new Path(sysPath + File.separator + "hdfs-site.xml"));
        conf.addResource(new Path(sysPath+ File.separator + "core-site.xml"));
        System.setProperty("java.security.krb5.conf",sysPath + File.separator + "krb5.conf");
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("PRINCIPAL", "bcloud@HADOOP.COM");
        conf.set("KEYTAB", sysPath + File.separator + "user.keytab");
        System.out.println("1系统当前HADOOP_USER_NAME："+System.getProperty("HADOOP_USER_NAME"));
        UserGroupInformation.setConfiguration(conf);
        System.out.println("2系统当前HADOOP_USER_NAME："+System.getProperty("HADOOP_USER_NAME"));
        try {
            System.out.println("-----------------------华为FI开始认证用户登录");
            UserGroupInformation.loginUserFromKeytab(conf.get("PRINCIPAL"), conf.get("KEYTAB"));
            System.out.println("-----------------------华为FI认证成功");
            System.out.println("3系统当前HADOOP_USER_NAME："+System.getProperty("HADOOP_USER_NAME"));
        } catch (IOException e) {
            System.out.println("-----------------------华为FI认证失败");
            e.printStackTrace();
        }
        map.put("hdfsAddr","hdfs://hacluster");
        map.put("conf",conf);
        return map;
    }

    private static void closeFileSystem(FileSystem fs) {
        if (fs != null) {
            try {
                fs.close();
//                ((DistributedFileSystem)fs).removeCachePool();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
