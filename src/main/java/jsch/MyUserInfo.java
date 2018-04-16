package jsch;

import com.jcraft.jsch.UserInfo;
import org.junit.Test;

/**
 * @Author: KeHongwei
 * @Description: This class provide interface to feedback information to the user.
 * @Date: Created in 11:54 2018/4/12
 * @Modified By:
 */
public class MyUserInfo implements UserInfo {
    private String password;

    private String passphrase;

    @Override
    public String getPassphrase() {
//        System.out.println("MyUserInfo.getPassphrase()");
        return null;
    }

    @Override
    public String getPassword() {
//        System.out.println("MyUserInfo.getPassword()");
        return null;
    }

    @Override
    public boolean promptPassphrase(final String arg0) {
//        System.out.println("MyUserInfo.promptPassphrase()");
//        System.out.println(arg0);
        return false;
    }

    @Override
    public boolean promptPassword(final String arg0) {
//        System.out.println("MyUserInfo.promptPassword()");
//        System.out.println(arg0);
        return false;
    }

    @Override
    public boolean promptYesNo(final String arg0) {
//        System.out.println("MyUserInfo.promptYesNo()");
//        System.out.println(arg0);
        if (arg0.contains("The authenticity of host")) {
            return true;
        }
        return false;
    }

    @Override
    public void showMessage(final String arg0) {
//        System.out.println("MyUserInfo.showMessage()");
    }

}
