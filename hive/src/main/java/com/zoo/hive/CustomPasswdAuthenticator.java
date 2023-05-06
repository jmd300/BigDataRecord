package com.zoo.hive;

/**
 * @Author: JMD
 * @Date: 5/5/2023
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import javax.security.sasl.AuthenticationException;

public class CustomPasswdAuthenticator implements org.apache.hive.service.auth.PasswdAuthenticationProvider{

    private Logger LOG = org.slf4j.LoggerFactory.getLogger(CustomPasswdAuthenticator.class);
    private static final String HIVE_JDBC_PASSWD_AUTH_PREFIX="hive.jdbc_passwd.auth.%s";
    private Configuration conf=null;

    @Override
    public void Authenticate(String userName, String passwd) throws AuthenticationException {
        LOG.info("Hive2 login info... user: "+ userName +" is trying login.");
        String passwdConf = getConf().get(String.format(HIVE_JDBC_PASSWD_AUTH_PREFIX, userName));
        if(passwdConf==null){
            String message = "Hive2 login error! Configration null. user:"+userName;
            LOG.info(message);
            throw new AuthenticationException(message);
        }
        if(!passwd.equals(passwdConf)){
            String message = "Hive2 login error! userName or password is error. user:"+userName;
            throw new AuthenticationException(message);
        }
    }

    public Configuration getConf() {
        if(conf==null){
            this.conf=new Configuration(new HiveConf());
        }
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf=conf;
    }
}
