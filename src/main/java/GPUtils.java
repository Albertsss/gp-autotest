import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @Author: KeHongwei
 * @Description:
 * @Date: Created in 14:08 2018/4/9
 * @Modified By:
 */
public class GPUtils {

    public static void printText(String text){
        try {
            int a=text.getBytes("gbk").length;
            String words="++                              ++";
            StringBuilder sb=new StringBuilder(words);
            sb.replace((34-a)/2,(34+a)/2,text);
            System.out.println("++++++++++++++++++++++++++++++++++");
            System.out.println(sb);
            System.out.println("++++++++++++++++++++++++++++++++++");
        }catch (UnsupportedEncodingException e){
            e.printStackTrace();
        }

    }

    public static void selectEmployees(String sql, Connection con, PreparedStatement ps, ResultSet rs){
        try {
            ps=con.prepareStatement(sql);
            rs=ps.executeQuery();
            while(rs.next()){
                System.out.println(rs.getString("id")+"--"+rs.getString("name")+"--"+
                        rs.getString("age")+"--"+rs.getString("address")+"--"+rs.getString("salary"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            GPConnectionUtils.close(rs,ps);
        }
    }

    public static void printJoin(String sql, Connection con, PreparedStatement ps, ResultSet rs){
        try {
            ps=con.prepareStatement(sql);
            rs=ps.executeQuery();
            while(rs.next()){
                System.out.println(rs.getString("id")+"--"+rs.getString("name")+"--"+
                rs.getString("dept"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            GPConnectionUtils.close(rs,ps);
        }
    }

    /*public static void main(String[] args) {
        printText("测试创建数据库");
    }*/
}
