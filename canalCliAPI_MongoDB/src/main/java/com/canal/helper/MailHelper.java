/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.canal.helper;

import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;
import org.apache.log4j.Logger;

/**
 *
 * @author sheriff
 */
public class MailHelper {
    
    private String prefix="smtp";
    private String postfix="163.com";
    private String charset="utf-8";
    private String user="";
    private String password="";
    private String hostname="";
    private String form="";
    private Logger log=Logger.getLogger(MailHelper.class);
    
    public MailHelper(String prefix,String postfix,String charset,String user,String password){
        this.prefix=prefix;
        this.postfix=postfix;
        this.charset=charset;
        this.user=user;
        this.password=password;
        this.setHostName();
    }
    
    public MailHelper(String postfix,String user,String password){
        this.postfix=postfix;
        this.user=user;
        this.password=password;
        this.setHostName();
        this.setForm();
    }
    
    public MailHelper(String postfix){
        this.postfix=postfix;
        this.setHostName();
    }
    
    public void setAuth(String user,String password){
        this.user=user;
        this.password=password;
        this.setForm();
    }
    
    private void setHostName(){
        this.hostname=this.prefix+"."+this.postfix;
    }
    
    private void setForm(){
        this.form=this.user+"@"+this.postfix;
    }
    
    public boolean sendEmail(String to,String subject,String msg){
        HtmlEmail email = new HtmlEmail();//创建一个HtmlEmail实例对象
        email.setHostName(hostname);//邮箱的SMTP服务器，一般123邮箱的是smtp.123.com,qq邮箱为smtp.qq.com
        email.setCharset(this.charset);//设置发送的字符类型        
        
        try{
            email.addTo(to);//设置收件人
            email.setFrom(this.form, this.user);//发送人的邮箱为自己的，用户名可以随便填
            email.setAuthentication(this.form, this.password);//设置发送人到的邮箱和用户名和授权码(授权码是自己设置的)
            email.setSubject(subject);//设置发送主题
            email.setMsg(msg);//设置发送内容
            //由于邮件滥发等原因阿里云服务器禁用了25端口，所以这里得使用ssl加密传输（这样使用的端口号是465）
            email.setSSLOnConnect(true);
            email.send(); //发送邮件
            return true;
        }catch(EmailException e){
            this.log.error(e.toString()+" [to:]"+to+",[subject:]"+subject+",[msg:]"+msg);
        }
        return false;
    }
}
