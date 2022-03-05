package Util;

import java.sql.*;

public class MysqlUtil {
    private static final String IP = "localhost";
    private static final String DB_NAME = "luohao";
    private static final String USER_NAME = "root";
    private static final String USER_PWD = "root";

    public MysqlUtil() {
    }

    public static Connection getConnection() {
        return getConnection("localhost", "luohao", "root", "root");
    }

    public static Connection getConnection(String ip, String dbName, String user_name, String pwd) {
        return getConnection(ip, 3306, dbName, user_name, pwd);
    }

    public static Connection getConnection(String ip, int port, String dbName, String user_name, String pwd) {
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            String url = String.format("jdbc:mysql://%s:%s/%s?useUnicode=true&characterEncoding=utf-8&useSSL=false", ip, port, dbName);
            url = url + "&useServerPrepStmts=false&rewriteBatchedStatements=true";
            Connection conn = DriverManager.getConnection(url, user_name, pwd);
            return conn;
        } catch (InstantiationException var7) {
            var7.printStackTrace();
        } catch (IllegalAccessException var8) {
            var8.printStackTrace();
        } catch (ClassNotFoundException var9) {
            var9.printStackTrace();
        } catch (SQLException var10) {
            var10.printStackTrace();
        }

        return null;
    }

    public static void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException var2) {
                var2.printStackTrace();
            }
        }

    }

    public static void closeStatement(Statement state) {
        if (state != null) {
            try {
                state.close();
            } catch (SQLException var2) {
                var2.printStackTrace();
            }
        }

    }

    public static void closeResultSet(ResultSet set) {
        if (set != null) {
            try {
                set.close();
            } catch (SQLException var2) {
                var2.printStackTrace();
            }
        }

    }

    public static ResultSet getResultFromMysql(Connection conn, String sql) {
        PreparedStatement states = null;
        ResultSet rs = null;

        try {
            states = conn.prepareStatement(sql, 1003, 1007);
            states.setFetchSize(-2147483648);
            rs = states.executeQuery();
        } catch (SQLException var5) {
            var5.printStackTrace();
        }

        return rs;
    }
}
