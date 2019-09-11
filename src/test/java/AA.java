import com.lxg.App;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

public class AA {
    public static void main(String[] args) throws Exception {

        Map<String,Object> map2 = App.getCommonHdfsAddrAndConf();
        System.setProperty("HADOOP_USER_NAME", "bcloud");
//        URI uri = new URI(String.valueOf(map2.get("hdfsAddr")));
        FileSystem fs = FileSystem.get(new URI(String.valueOf(map2.get("hdfsAddr"))), (Configuration) map2.get("conf"));

//        String scheme = uri.getScheme();
//        String authority = uri.getAuthority();
        /*if (conf.getBoolean(disableCacheName, false)) {
            return createFileSystem(uri, conf);
        }*/
    }
}
