package com.timliu.debugtool;

import org.junit.Test;

import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.Socket;

/**
 * 运行时工具类
 */
public class RTUtil {

    private static String SEPERATOR = "\t";

    /**
     * 主机名
     */
    public static String getHostname() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "unknown";
    }

    /**
     * 进程PID
     */
    public static String getPID() {
        try {
            return ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "unknown";
    }

    /**
     * 线程TID
     */
    public static String getTID() {
        try {
            return Thread.currentThread().getName();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "unknown";
    }

    /**
     * Bean信息App@1234:()
     */
    public static String getBeanInfo(Object obj, String method, String args) {
        try {
            String cname = obj.getClass().getSimpleName();
            int hash = obj.hashCode();
            return getHostname() + SEPERATOR
                    + getPID() + SEPERATOR
                    + getTID() + SEPERATOR
                    + cname + SEPERATOR
                    + hash + SEPERATOR
                    + method + "(" + args + ")";
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "unknown";
    }

    /**
     * Bean信息
     */
    public static String getBeanInfo(Object obj, String method) {
        return getBeanInfo(obj, method, "");
    }

    public static void sendInfo(String ip, int port, Object obj, String method, String args) {
        try {
            Socket s = new Socket(ip, port);
            OutputStream out = s.getOutputStream();
            out.write(getBeanInfo(obj, method, args).getBytes());
            out.write(("\r\n").getBytes());
            out.flush();
            out.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test1() {
        System.out.println(getBeanInfo(this, "test1", "2"));
        sendInfo("localhost", 8888, this, "test1", "2");
    }
}
