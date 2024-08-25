package xogta.tech;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import java.io.IOException;

public class KerberosAuthenticator {
    public static void authenticate() throws IOException {
        System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab("hive/fully.qualified.domain.name@REALM", "/etc/security/keytabs/hive.service.keytab");
    }
}