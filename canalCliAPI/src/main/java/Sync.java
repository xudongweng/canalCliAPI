
import com.canal.helper.MailHelper;
import com.canal.mysql.CanalClient;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Locale;
import java.util.ResourceBundle;
import org.apache.log4j.Logger;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author sheriff
 */
public class Sync {
    public static void main(String[] args){
        Locale myLocale = Locale.getDefault(Locale.Category.FORMAT);
        ResourceBundle rb= ResourceBundle.getBundle("config",myLocale);
        CanalClient cc=new CanalClient();
        cc.setconnect(rb.getString("source.canal.server"), Integer.parseInt(rb.getString("source.canal.port")), rb.getString("source.canal.instance"));
        int totalEmptyCount = 120;
        int emptyCount = 0;
        while (emptyCount < totalEmptyCount) {
            cc.tansferEntry();
            if(!cc.getIsExecute()){//如果有执行错误，则退出
                break;
            }
            if(cc.getIsEmpty()){
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                }
            }else{
                emptyCount = 0;
            }
            System.out.println("empty count : " + (++emptyCount));
        }
        cc.disconnect();
        
        Logger log=Logger.getLogger(Sync.class);
        try{
            InetAddress addr = InetAddress.getLocalHost();
            String ip=addr.getHostAddress();
        
            MailHelper mh=new MailHelper("163.com","sheriff_weng","zxcv1234");
            mh.sendEmail("sheriff.weng@mobizone.com", ip + "synchronization", ip + "synchronization has stoped.");
        }catch(UnknownHostException e){
            log.error(e.toString());
        }
    }
}
