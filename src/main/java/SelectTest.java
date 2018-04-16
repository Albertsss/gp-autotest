import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @Author: KeHongwei
 * @Description:
 * @Date: Created in 11:18 2018/4/9
 * @Modified By:
 */
public class SelectTest {
    private PreparedStatement ps=null;
    private ResultSet rs=null;

    @Test
    public void sel(){
        String sql="select * from minielectric.ebike_pass_record limit 10";
        Connection con=GPConnectionUtils.getConn();
        try {
            ps=con.prepareStatement(sql);
            rs=ps.executeQuery();
            while (rs.next()){
                System.out.println(rs.getString("id")+"---"+rs.getString("plate_no"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            GPConnectionUtils.close(rs,ps);
        }
    }

    public static void main(String[] args) {
        new SelectTest().sel();
    }
}
