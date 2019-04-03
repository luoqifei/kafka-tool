package shell.cmd;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;
public class SSHTest{
    public Boolean login(String ip,String userName,String userPwd) {
        boolean flg = false;
        try {
            Connection conn = new Connection(ip);
            conn.connect();// 连接
            //判断身份是否已经认证
            if (!conn.isAuthenticationComplete()) {
                //加锁，防止多线程调用时线程间判断不一致，导致出现重复认证
                synchronized (this) {
                    if (!conn.isAuthenticationComplete()) {
                        //进行身份认证
                        flg = conn.authenticateWithPassword(userName, userPwd);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (IllegalStateException e) {
            e.printStackTrace();
        }
        return flg;
    }

    /**
     *
     * @param userName
     * @param operation Write/Read
     * @param shellBaseDir
     * @param topicName
     * @return
     */
    public boolean alcsAuthorizer(String userName,String operation,String shellBaseDir,String topicName){
        String zkUrl = "localhost:2181/sasl";
        String cmd = shellBaseDir+"/bin/kafka-acls.sh --authorizer kafka.security.auth.SimpleAclAuthorizer " +
                  "--authorizer-properties zookeeper.connect="+zkUrl+
                  " --add --allow-principal User:"+userName+
                  " --operation "+ operation+
                  " --topic "+topicName;
        return true;
    }
    public boolean queryAcls(String shellBaseDir){
        String zkUrl = "localhost:2181/sasl";
        String cmd = "./bin/kafka-acls.sh --authorizer-properties zookeeper.connect="+zkUrl+" --list";
        return true;
    }

    /**
     * @param userName
     * @param clientId
     * @param quotaName producer_byte_rate/consumer_byte_rate
     * @param quataBytes
     * @return
     */
    public boolean setQuotas(String shellBaseDir,String userName,String clientId,String quotaName,long quataBytes){
        String zkUrl = "localhost:2181/sasl";
        String cmd = shellBaseDir+"/bin/kafka-configs.sh --zookeeper "+zkUrl+
                " --alter --add-config '"+quotaName+"="+quataBytes+"' " +
                " --entity-type users --entity-name " + userName +
                " --entity-type clients --entity-name " + clientId;
        return true;
    }
    public static boolean exeCmd(String cmd,Session sess){
        try {
            //执行具体命令
            sess.execCommand(cmd);
            //获取返回输出
            InputStream stdout = new StreamGobbler(sess.getStdout());
            //返回错误输出
            InputStream stderr = new StreamGobbler(sess.getStderr());
            BufferedReader stdoutReader = new BufferedReader(
                    new InputStreamReader(stdout));
            BufferedReader stderrReader = new BufferedReader(
                    new InputStreamReader(stderr));
            System.out.println("Here is the output from stdout:");
            while (true) {
                String line = stdoutReader.readLine();
                if (line == null)
                    break;
                System.out.println(line);
            }
            System.out.println("Here is the output from stderr:");
            while (true) {
                String line = stderrReader.readLine();
                if (line == null)
                    break;
                System.out.println(line);
            }
            //等待,除非 1.连接关闭；2.输出数据传送完毕；3.进程状态为退出；4.超时
            sess.waitForCondition(3,1);
            int status = sess.getExitStatus();
            System.out.println("exist code = "+sess.getExitStatus());
            //关闭Session
            sess.close();
            if (status == 0){
                return true;
            }else {
                return false;
            }
        } catch (IOException e) {
            e.printStackTrace(System.err);
            return false;
            //System.exit(2);
        }finally {
            if(sess !=null) {
                sess.close();
            }
        }
    }
    public static void main(String[] args) {
        String hostname = "192.168.238.128";
        String username = "luoqifei";
        String password = "1234";
        int port = 22;
        Connection conn = null;
        Session sess = null;
        try {
            conn = new Connection(hostname,port);
            conn.connect();
            //进行身份认证
            boolean isAuthenticated = conn.authenticateWithPassword(
                    username,password);
            if (isAuthenticated == false)
                throw new IOException("Authentication failed.");

            String cmd = "/home/luoqifei/kafka/bin/kafka-configs.sh --zookeeper localhost:2181/sasl --describe --entity-type users --entity-name alice --entity-type clients --entity-name alice-001-producer-100a";
            //开启一个Session
            sess = conn.openSession();
            //执行具体命令
            System.out.println(exeCmd(cmd,sess));
            System.out.println(exeCmd("cat /home/luoqifei/kafka/1.sh",conn.openSession()));
            System.out.println(exeCmd("sh /home/luoqifei/kafka/1.sh",conn.openSession()));


            //关闭Connection
            conn.close();
        } catch (IOException e) {
            e.printStackTrace(System.err);
            //System.exit(2);
        }finally {
            if(sess !=null) {
                sess.close();
            }
            if(conn != null){
                conn.close();
            }
        }
    }
}
