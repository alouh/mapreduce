package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.net.URI;

/**
 * @Author: HanJiafeng
 * @Date: 11:25 2018/3/26
 * @Desc: HDFS命令
 */
public class HdfsCommand {

    private static final String HDFS = "hdfs://Master:9000/";
    /**
     * hdfs路径
     */
    private String hdfsPath;
    /**
     * hadoop系统配置
     */
    private Configuration conf;

    public HdfsCommand(Configuration conf) {
        this(HDFS, conf);
    }

    public HdfsCommand(String hdfs, Configuration conf) {
        this.hdfsPath = hdfs;
        this.conf = conf;
    }

    public static void main(String[] args) throws IOException {
        JobConf conf = config();
        HdfsCommand hdfs = new HdfsCommand(conf);
        hdfs.copyFile("datafile/item.csv", "/tmp/new");
        hdfs.ls("/tmp/new");
    }

    public static JobConf config() {
        JobConf conf = new JobConf(HdfsCommand.class);
        conf.setJobName("HdfsDAO");
        return conf;
    }

    public void mkdirs(String folder) throws IOException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        if (!fs.exists(path)) {
            fs.mkdirs(path);
            System.out.println("Create: " + folder);
        }
        fs.close();
    }

    /**
     * 删除文件夹
     *
     * @param folder 文件夹路径
     * @throws IOException IO异常
     */
    public void rmr(String folder) throws IOException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.deleteOnExit(path);
        System.out.println("Delete: " + folder);
        fs.close();
    }

    public void ls(String folder) throws IOException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        FileStatus[] list = fs.listStatus(path);
        System.out.println("ls: " + folder);
        System.out.println("==========================================================");
        for (FileStatus f : list) {
            System.out.printf("name: %s, folder: %s, size: %d\n", f.getPath(), f.isDirectory(), f.getLen());
        }
        System.out.println("==========================================================");
        fs.close();
    }

    public void createFile(String file, String content) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        byte[] buff = content.getBytes();
        try (FSDataOutputStream os = fs.create(new Path(file))) {
            os.write(buff, 0, buff.length);
            System.out.println("Create: " + file);
        }
        fs.close();
    }

    public void copyFile(String local, String remote) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.copyFromLocalFile(new Path(local), new Path(remote));
        System.out.println("copy from: " + local + " to " + remote);
        fs.close();
    }

    public void download(String remote, String local) throws IOException {
        Path path = new Path(remote);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.copyToLocalFile(path, new Path(local));
        System.out.println("download: from" + remote + " to " + local);
        fs.close();
    }

    public void cat(String remoteFile) throws IOException {
        Path path = new Path(remoteFile);
        FSDataInputStream fsdis = null;
        System.out.println("cat: " + remoteFile);
        try (FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf)) {
            fsdis = fs.open(path);
            IOUtils.copyBytes(fsdis, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(fsdis);
        }
    }
}
