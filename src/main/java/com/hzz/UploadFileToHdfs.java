package com.hzz;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RemoteException;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

public class UploadFileToHdfs {

    /**
     * java -cp .:hadoop-common-2.7.1.jar:commons-logging-1.1.3.jar:guava-11.0.2.jar:commons-collections-3.2.1.jar:commons-configuration-1.6.jar:commons-lang-2.4.jar:hadoop-auth-2.7.1.jar:slf4j-api-1.7.10.jar:slf4j-log4j12-1.7.10.jar:log4j-1.2.12.jar:hadoop-hdfs-2.7.1.jar:htrace-core-3.1.0-incubating.jar:servlet-api-2.5.jar:commons-cli-1.2.jar:protobuf-java-2.5.0.jar:commons-io-2.4.jar UploadFileToHdfs abc /user/pcsjob  xxx
     *
     * @param filePath
     * @param dst
     * @throws IOException
     */
    public static void uploadFile(Configuration conf, String filePath, String dst) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path srcPath = new Path(filePath);
        Path dstPath = new Path(dst);
        fs.copyFromLocalFile(false, srcPath, dstPath);
        fs.close();
    }

    public static void main(String[] args) throws IOException {
        File nn = new File(args[0]);
        if(!nn.exists()) {
            System.out.println("file nn not exist");
            return;
        }
        String[] nns = FileUtils.readFileToString(nn).split(",");
        String activeNN = nns[0];
        Configuration conf = new Configuration(true);
        for(String n : nns) {
            try {
                conf.set("fs.default.name", "hdfs://" + n + ":8020");
                conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
                System.out.println("try upload " + args[1] + " to hdfs " + n + " " + args[2]);
                uploadFile(conf, args[1], args[2]);
                System.out.println("upload success");
                activeNN = n;
                break;
            } catch (RemoteException e) {
                if("org.apache.hadoop.ipc.StandbyException".equals(e.getClassName())) {
                    continue;
                }
            }
        }
        System.out.println("active node is: " + activeNN);
        if(!activeNN.equals(nns[0])) {// activeNamenode 发生了变动
            PrintWriter pw = new PrintWriter(nn);
            pw.print(activeNN);
            for(String n : nns) {
                if(!activeNN.equals(n)) {
                    pw.print(',' + n);
                }
            }
            pw.flush();
            pw.close();
        }
    }
}
