import org.junit.Test;

import java.sql.*;

/**
 * @Author: KeHongwei
 * @Description:
 * @Date: Created in 14:05 2018/4/9
 * @Modified By:
 */
public class GPTest {
//    private PreparedStatement ps=null;
//    private ResultSet rs=null;
    private static final String t="psql---->>";

    public static void main(String[] args) {
        GPTest test=new GPTest();
        test.createDB();            //创建数据库进行测试
        test.createSchema();        //创建架构
        test.createTable();         //创建表
        test.tableTest();           //测试表约束、数据类型
        test.curdTest();            //对标进行增删改查
        test.dropTable();           //删除表
        test.orderByTest();         //测试order by
        test.groupByTest();         //测试group by
        test.havingTest();          //测试having子句
        test.andTest();             //测试and条件
        test.orTest();              //测试or条件
        test.notTest();             //测试not条件
        test.likeTest();            //测试like条件
        test.inTest();              //测试in条件
        test.notInTest();           //测试not in条件
        test.betweenTest();         //测试between条件
        test.updateData();          //更新数据
        test.innerJoinTest();       //测试内连接
        test.leftJoinTest();        //测试左外连接
        test.rightJoinTest();       //测试右外连接
        test.fullJoinTest();        //测试全外连接
        test.crossJoinTest();       //测试跨连接
        test.viewTest();            //测试视图
        test.functionTest();        //测试函数
        test.triggerSequenceTest(); //测试触发器和序列
        test.maxAsTest();           //测试max和as别名
        test.limitTest();           //测试limit和offset
        test.indexTest();           //测试索引
        test.dateTest();            //测试日期和时间函数
        test.collectionTest();      //集合测试（union、intersect、except）
        test.alterTest();           //测试alert修改表
        test.subQueryTest();        //测试子查询
        test.serialTest();          //测试serial自增长
        test.authorityTest();       //测试权限控制
//        test.triggerTestDemo();
        GPServer gpServer=new GPServer();   //测试gp服务器相关信息
        GPService gpService=new GPService();    //测试gp相关服务
        test.dropSchema();//删除架构
        test.dropDB();//测试完删除数据库

    }

    /**
     * 测试创建修改数据库
     */
    @Test
    public void createDB(){
        GPUtils.printText("测试创建修改数据库");
        String sql="create database testdd";
        System.out.println(t+sql);
        Connection con=GPConnectionUtils.getConn();
        PreparedStatement ps=null;
        try {
            ps=con.prepareStatement(sql);
            boolean ifsuc=ps.execute();
            if(!ifsuc){
//                System.out.println("创建数据库testdd成功");
                sql="alter database testdd rename to testdb";
                System.out.println(t+sql);
                ps=con.prepareStatement(sql);
                ps.execute();
                GPConnectionUtils.url="jdbc:pivotal:greenplum://192.168.18.211:5432;DatabaseName=testdb";
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            GPConnectionUtils.close(null,ps);
            GPConnectionUtils.conn=null;
        }
    }

    /**
     * 测试删除数据库
     */
    @Test
    public void dropDB(){
        if(GPConnectionUtils.conn!=null){
            try {
                GPConnectionUtils.conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        GPConnectionUtils.conn=null;
        GPConnectionUtils.url="jdbc:pivotal:greenplum://192.168.18.211:5432;DatabaseName=postgres";
        GPUtils.printText("测试删除数据库");
        String sql="drop database testdb";
        System.out.println(t+sql);
        Connection con=GPConnectionUtils.getConn();
        PreparedStatement ps=null;
        try {
            ps=con.prepareStatement(sql);
            boolean ifsuc=ps.execute();
            if(!ifsuc){
                System.out.println("删除数据库testdb成功");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            GPConnectionUtils.close(null,ps);
        }
    }

    /**
     * 测试创建修改架构
     */
    @Test
    public void createSchema(){
        GPUtils.printText("测试创建修改架构");
        String sql="create schema demo";
        System.out.println(t+sql);
        Connection con=GPConnectionUtils.getConn();
        PreparedStatement ps=null;
        try {
            ps=con.prepareStatement(sql);
            ps.execute();
            sql="alter schema demo rename to demolw";
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            GPConnectionUtils.close(null,ps);
        }
    }

    /**
     * 测试删除架构
     */
    @Test
    public void dropSchema(){
        GPUtils.printText("测试删除架构");
        String sql="drop schema demolw";
        System.out.println(t+sql);
        Connection con=GPConnectionUtils.getConn();
        PreparedStatement ps=null;
        try {
            ps=con.prepareStatement(sql);
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            GPConnectionUtils.close(null,ps);
        }
    }

    /**
     * 测试建表
     */
    @Test
    public void createTable(){
        GPUtils.printText("测试建表");
        String sql="create table emp(eno int primary key not null,ename varchar(30),eage int)";
        System.out.println(t+sql);
        Connection con=GPConnectionUtils.getConn();
        PreparedStatement ps=null;
        try {
            ps=con.prepareStatement(sql);
            ps.execute();
            sql="create table employees(id int primary key not null,name text not null,age int not null,address " +
                    "varchar(50),salary real)";
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            GPConnectionUtils.close(null,ps);
        }
    }

    /**
     * 测试表约束、数据类型
     */
    @Test
    public void tableTest(){
        GPUtils.printText("测试表约束、数据类型");
        String sql="create table test(" +
                "a smallint not null," +
                "b integer unique," +
                "c bigint check(c>0), " +
                "d decimal(15,8)," +
                "e numeric(15,8)," +
                "f real," +
                "g text," +
                "h timestamp," +
                "i interval," +
                "j date," +
                "k time," +
                "l boolean," +
                "eno int references emp(eno)" +
                ");";
        System.out.println(t+sql);
        Connection con=GPConnectionUtils.getConn();
        PreparedStatement ps=null;
        try {
            ps=con.prepareStatement(sql);
            boolean a=ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            GPConnectionUtils.close(null,ps);
        }
    }

    /**
     * 测试删表
     */
    @Test
    public void dropTable(){
        GPUtils.printText("测试删表");
        String sql="drop table test";
        System.out.println(t+sql);
        Connection con=GPConnectionUtils.getConn();
        PreparedStatement ps=null;
        try {
            ps=con.prepareStatement(sql);
            ps.execute();
            sql="drop table emp";
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            GPConnectionUtils.close(null,ps);
        }
    }

    /**
     * 测试增删改查
     */
    @Test
    public void curdTest(){
        GPUtils.printText("测试增删改查");
        Connection con=GPConnectionUtils.getConn();
        String sql="insert into employees(id,name,age,address,salary) values (1, 'Maxsu', 25, '海口市人民大道2880号', 109990.00 )," +
                "(2, 'minsu', 25, '广州中山大道', 125000.00 ),(3, '李洋', 21, '北京市朝阳区', 185000.00)," +
                "(4, 'Manisha', 24, 'Mumbai', 65000.00),(5, 'Larry', 21, 'Paris', 85000.00)";
        System.out.println(t+sql);
        PreparedStatement ps=null;
        ResultSet rs=null;
        boolean ifsuc;
        try {
            ps=con.prepareStatement(sql);
            ifsuc=ps.execute();
            if(!ifsuc){
                System.out.println("新增成功");
            }
            sql="select * from employees order by id";
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            rs=ps.executeQuery();
//            System.out.println("查询结果:");
            while (rs.next()){
                System.out.println(rs.getString("id")+"--"+rs.getString("name")+"--"+rs.getString("age")+"--"+
                        rs.getString("address")+"--"+rs.getString("salary"));
            }
            sql="update employees set age=29,salary=9800 where id=1";
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            if(ps.executeUpdate()>0){
                System.out.println("更新成功");
            }
            sql="select * from employees order by id";
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            rs=ps.executeQuery();
            while (rs.next()){
                System.out.println(rs.getString("id")+"--"+rs.getString("name")+"--"+rs.getString("age")+"--"+
                        rs.getString("address")+"--"+rs.getString("salary"));
            }
            sql="delete from employees where id=1";
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            ifsuc=ps.execute();
            if(!ifsuc){
                System.out.println("删除成功");
            }
            sql="select * from employees order by id";
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            rs=ps.executeQuery();
            while (rs.next()){
                System.out.println(rs.getString("id")+"--"+rs.getString("name")+"--"+rs.getString("age")+"--"+
                        rs.getString("address")+"--"+rs.getString("salary"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            GPConnectionUtils.close(rs,ps);
        }
    }

    /**
     * 测试order by
     */
    @Test
    public void orderByTest(){
        GPUtils.printText("测试order by");
        String sql="select * from employees order by name desc";
        System.out.println(t+sql);
        Connection con=GPConnectionUtils.getConn();
        PreparedStatement ps=null;
        ResultSet rs=null;
        GPUtils.selectEmployees(sql,con,ps,rs);
        /*try {
            ps=con.prepareStatement(sql);
            rs=ps.executeQuery();
            while(rs.next()){
                System.out.println(rs.getString("id")+"--"+rs.getString("name")+"--"+rs.getString("age")+"--"+
                        rs.getString("address")+"--"+rs.getString("salary"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            GPConnectionUtils.close(rs,ps);
        }*/

    }

    /**
     * 测试group by
     */
    @Test
    public void groupByTest(){
        GPUtils.printText("测试group by");
        String sql="select name,sum(salary) sum from employees group by name";
        System.out.println(t+sql);
        Connection con=GPConnectionUtils.getConn();
        PreparedStatement ps=null;
        ResultSet rs=null;
        try {
            ps=con.prepareStatement(sql);
            rs=ps.executeQuery();
            while(rs.next()){
                System.out.println(rs.getString("name")+"--"+rs.getString("sum"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            GPConnectionUtils.close(rs,ps);
        }

    }

    /**
     * 测试having子句
     */
    @Test
    public void havingTest(){
        GPUtils.printText("测试having子句");
        String sql="select name from employees group by name having count(name)<2";
        System.out.println(t+sql);
        Connection con=GPConnectionUtils.getConn();
        PreparedStatement ps=null;
        ResultSet rs=null;
        try {
            ps=con.prepareStatement(sql);
            rs=ps.executeQuery();
            while(rs.next()){
                System.out.println(rs.getString("name"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            GPConnectionUtils.close(rs,ps);
        }

    }

    /**
     * 测试and
     */
    @Test
    public void andTest(){
        GPUtils.printText("测试and");
        String sql="select * from employees where salary>120000 and id<4";
        System.out.println(t+sql);
        Connection con=GPConnectionUtils.getConn();
        PreparedStatement ps=null;
        ResultSet rs=null;
        GPUtils.selectEmployees(sql,con,ps,rs);
    }

    /**
     * 测试or
     */
    @Test
    public void orTest(){
        GPUtils.printText("测试or");
        String sql="select * from employees where name='李洋' or address='Mumbai'";
        System.out.println(t+sql);
        Connection con=GPConnectionUtils.getConn();
        PreparedStatement ps=null;
        ResultSet rs=null;
        GPUtils.selectEmployees(sql,con,ps,rs);
    }

    /**
     * 测试not条件
     */
    @Test
    public void notTest(){
        GPUtils.printText("测试not条件");
        String sql="select * from employees where address is not null";
        System.out.println(t+sql);
        Connection con=GPConnectionUtils.getConn();
        PreparedStatement ps=null;
        ResultSet rs=null;
        GPUtils.selectEmployees(sql,con,ps,rs);
    }

    /**
     * 测试like条件
     */
    @Test
    public void likeTest(){
        GPUtils.printText("测试like条件");
        String sql="select * from employees where name like 'Ma%'";
        System.out.println(t+sql);
        Connection con=GPConnectionUtils.getConn();
        PreparedStatement ps=null;
        ResultSet rs=null;
        GPUtils.selectEmployees(sql,con,ps,rs);
    }

    /**
     * 测试in条件
     */
    @Test
    public void inTest(){
        GPUtils.printText("测试in条件");
        String sql="select * from employees where age in (21,24)";
        System.out.println(t+sql);
        Connection con=GPConnectionUtils.getConn();
        PreparedStatement ps=null;
        ResultSet rs=null;
        GPUtils.selectEmployees(sql,con,ps,rs);
    }

    /**
     * 测试not in条件
     */
    @Test
    public void notInTest(){
        GPUtils.printText("测试not in条件");
        String sql="select * from employees where age not in (21,24)";
        System.out.println(t+sql);
        Connection con=GPConnectionUtils.getConn();
        PreparedStatement ps=null;
        ResultSet rs=null;
        GPUtils.selectEmployees(sql,con,ps,rs);
    }

    /**
     * 测试between条件
     */
    @Test
    public void betweenTest(){
        GPUtils.printText("测试between条件");
        String sql="select * from employees where age between 23 and 26";
        System.out.println(t+sql);
        Connection con=GPConnectionUtils.getConn();
        PreparedStatement ps=null;
        ResultSet rs=null;
        GPUtils.selectEmployees(sql,con,ps,rs);
    }

    /**
     * 更新数据
     */
    @Test
    public void updateData(){
        GPUtils.printText("更新数据");
        String sql="truncate employees";
        System.out.println(t+sql);
        Connection con=GPConnectionUtils.getConn();
        PreparedStatement ps=null;
        try {
            ps=con.prepareStatement(sql);
            ps.execute();
            sql="insert into employees(id,name,age,address,salary) values (1,'Maxsu',25,'海口市人民大道2880号',109990)," +
                    "(2,'Minsu',25,'广州中山大道',125000),(3,'李洋',21,'北京市朝阳区',185000)," +
                    "(4,'Manisha',24,'Mumbai',65000),(5,'Larry',21,'Paris',85000)," +
                    "(7,'Minsu',21,'Delhi',135000),(8,'Manisha',19,'Noida',125000)";
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            ps.execute();
            sql="create table department(id int,dept text,fac_id int)";
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            ps.execute();
            sql="insert into department values(1,'IT', 1),(2,'Engineering', 2),(3,'HR', 7)";
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            GPConnectionUtils.close(null,ps);
        }
    }

    /**
     * 测试内连接
     */
    @Test
    public void innerJoinTest(){
        GPUtils.printText("测试内连接");
        String sql="select employees.id,employees.name,department.dept from employees inner join department " +
                "on employees.id=department.id";
        System.out.println(t+sql);
        Connection con=GPConnectionUtils.getConn();
        PreparedStatement ps=null;
        ResultSet rs=null;
        GPUtils.printJoin(sql,con,ps,rs);
    }

    /**
     * 测试左外连接
     */
    @Test
    public void leftJoinTest(){
        GPUtils.printText("测试左外连接");
        String sql="select employees.id,employees.name,department.dept from employees left outer join department " +
                "on employees.id=department.id";
        System.out.println(t+sql);
        Connection con=GPConnectionUtils.getConn();
        PreparedStatement ps=null;
        ResultSet rs=null;
        GPUtils.printJoin(sql,con,ps,rs);
    }

    /**
     * 测试右外连接
     */
    @Test
    public void rightJoinTest(){
        GPUtils.printText("测试右外连接");
        String sql="select employees.id,employees.name,department.dept from employees right outer join department " +
                "on employees.id=department.id";
        System.out.println(t+sql);
        Connection con=GPConnectionUtils.getConn();
        PreparedStatement ps=null;
        ResultSet rs=null;
        GPUtils.printJoin(sql,con,ps,rs);
    }

    /**
     * 测试全外连接
     */
    @Test
    public void fullJoinTest(){
        GPUtils.printText("测试全外连接");
        String sql="select employees.id,employees.name,department.dept from employees full outer join department " +
                "on employees.id=department.id";
        System.out.println(t+sql);
        Connection con=GPConnectionUtils.getConn();
        PreparedStatement ps=null;
        ResultSet rs=null;
        GPUtils.printJoin(sql,con,ps,rs);
    }

    /**
     * 测试跨连接
     */
    @Test
    public void crossJoinTest(){
        GPUtils.printText("测试跨连接");
        String sql="select name,dept from employees cross join department";
        System.out.println(t+sql);
        Connection con=GPConnectionUtils.getConn();
        PreparedStatement ps=null;
        ResultSet rs=null;
        try {
            ps=con.prepareStatement(sql);
            rs=ps.executeQuery();
            while(rs.next()){
                System.out.println(rs.getString("name")+"--"+rs.getString("dept"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            GPConnectionUtils.close(rs,ps);
        }
    }


    /**
     * 测试视图
     */
    @Test
    public void viewTest(){
        GPUtils.printText("测试视图");
        String sql="create view current_employees as select name,id,salary from employees";
        System.out.println(t+sql);
        Connection con=GPConnectionUtils.getConn();
        PreparedStatement ps=null;
        ResultSet rs=null;
        try {
            ps=con.prepareStatement(sql);
            ps.execute();
            sql="select * from current_employees";
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            rs=ps.executeQuery();
            while(rs.next()){
                System.out.println(rs.getString("name")+"--"+rs.getString("id")+"--"+
                rs.getString("salary"));
            }
            sql="drop view current_employees";
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            GPConnectionUtils.close(rs,ps);
        }
    }

    /**
     * 测试函数
     */
    @Test
    public void functionTest(){
        GPUtils.printText("测试函数");
        String sql="create or replace function totalRecords() " +
                "returns integer as $total$ " +
                "declare " +
                "   total integer;" +
                "begin " +
                "   select count(*) into total from employees;" +
                "   return total;" +
                "end;" +
                "$total$ language plpgsql;";
        System.out.println(t+sql);
        Connection con=GPConnectionUtils.getConn();
        PreparedStatement ps=null;
        ResultSet rs=null;
        try {
            ps=con.prepareStatement(sql);
            ps.execute();
            sql="select totalRecords()";
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            rs=ps.executeQuery();
            while (rs.next()){
                System.out.println(rs.getString(1));
            }
            sql="drop function totalrecords()";
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            GPConnectionUtils.close(rs,ps);
        }
    }

    /**
     * 测试触发器,有bug
     */
    @Test
    public void triggerTestDemo(){
        GPUtils.printText("测试触发器");
        String sql="create table student(s_id int primary key,s_name varchar(64),age int)";
        Connection con=GPConnectionUtils.getConn();
        PreparedStatement ps=null;
        ResultSet rs=null;
        try {
            ps=con.prepareStatement(sql);
            ps.execute();
            sql="create table score(s_id int,score int,test_data date)";
            ps=con.prepareStatement(sql);
            ps.execute();
            sql="create or replace function student_delete_function() " +
                    "returns trigger as $$ " +
                    "begin " +
                    "   delete from score where s_id=NEW.s_id;" +
                    "   return NEW;" +
                    "end;" +
                    "$$ " +
                    "language plpgsql volatile";
            ps=con.prepareStatement(sql);
            ps.execute();
            sql="create trigger delete_student_trigger after delete on student for each row execute procedure student_delete_function();";
            ps=con.prepareStatement(sql);
            ps.execute();
            sql="insert into student values(1,'张三',14),(2,'李四',14),(3,'王五',13)";
            ps=con.prepareStatement(sql);
            ps.execute();
            sql="insert into score values(1,78,'2016-12-30'),(1,79,'2016-12-31'),(2,66,'2016-12-30')," +
                    "(2,90,'2016-12-31'),(3,87,'2016-12-30'),(3,79,'2016-12-31')";
            ps=con.prepareStatement(sql);
            ps.execute();
            sql="select * from student";
            ps=con.prepareStatement(sql);
            rs=ps.executeQuery();
            while(rs.next()){
                System.out.println(rs.getString(1)+"--"+rs.getString(2)+"--"+rs.getString(3));
            }
            System.out.println("----------------------------------");
            sql="select * from score";
            ps=con.prepareStatement(sql);
            rs=ps.executeQuery();
            while (rs.next()){
                System.out.println(rs.getString(1)+"--"+rs.getString(2)+"--"+rs.getString(3));
            }
            sql="delete from student where s_id=3";
            ps=con.prepareStatement(sql);
            ps.execute();
            System.out.println("----------------------------------");
            sql="select * from score;";
            ps=con.prepareStatement(sql);
            rs=ps.executeQuery();
            while (rs.next()){
                System.out.println(rs.getString(1)+"--"+rs.getString(2)+"--"+rs.getString(3));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 测试触发器和序列
     */
    @Test
    public void triggerSequenceTest(){
        GPUtils.printText("测试触发器和序列");
        String sql="create sequence shipments_ship_id_seq minvalue 1";
        System.out.println(t+sql);
        Connection con=GPConnectionUtils.getConn();
        PreparedStatement ps=null;
        ResultSet rs=null;
        try {
            ps=con.prepareStatement(sql);
            ps.execute();
            sql="alter sequence shipments_ship_id_seq restart with 1";
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            ps.execute();
            sql="create table shipments(id integer NOT NULL PRIMARY KEY,customer_id integer, isbn text, ship_date timestamp)";
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            ps.execute();
            sql="create or replace function insert_id() returns trigger as $$ " +
                    "declare " +
                    "   seq_id integer; " + //声明一个变量，存储新的序列值
                    "begin " +
                    "   select into seq_id nextval('shipments_ship_id_seq');" + //获取新序列值
                    "   NEW.id=seq_id;" +   //赋值给记录
                    "   return NEW;" +  //返回修改后的记录
                    "end;" +
                    "$$ LANGUAGE plpgsql volatile"; //指定使用 PL/PGSQL 作为脚本语言
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            ps.execute();
            sql="create trigger insert_ship_id before insert on shipments " +
                    "for each row execute procedure insert_id()";
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            ps.execute();
            sql="insert into shipments (customer_id, isbn, ship_date) values (221, '0394800753', 'now'),(321, '0394800555', 'now')";
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            ps.execute();
            sql="select * from shipments";
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            rs=ps.executeQuery();
            while (rs.next()){
                System.out.println(rs.getString(1)+"--"+rs.getString(2)+"--"+rs.getString(3));
            }
            sql="drop trigger insert_ship_id on shipments"; //测试删除触发器
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            ps.execute();
            sql="drop sequence shipments_ship_id_seq";  //测试删除序列
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            GPConnectionUtils.close(rs,ps);
        }
    }

    /**
     * 测试max、min、sum、avg和as别名
     */
    @Test
    public void maxAsTest(){
        GPUtils.printText("测试max和as别名");
        String sql="select name,max(salary) as package from employees group by name";
        System.out.println(t+sql);
        Connection con=GPConnectionUtils.getConn();
        PreparedStatement ps=null;
        ResultSet rs=null;
        try {
            ps=con.prepareStatement(sql);
            rs=ps.executeQuery();
            while (rs.next()){
                System.out.println(rs.getString(1)+"--"+rs.getString(2));
            }
            sql="select min(salary) from employees";
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            rs=ps.executeQuery();
            while(rs.next()){
                System.out.println(rs.getString(1));
            }
            sql="select sum(salary) from employees";
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            rs=ps.executeQuery();
            while(rs.next()){
                System.out.println(rs.getString(1));
            }
            sql="select avg(salary) from employees";
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            rs=ps.executeQuery();
            while(rs.next()){
                System.out.println(rs.getString(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            GPConnectionUtils.close(rs,ps);
        }
    }

    /**
     * 测试limit和offset
     */
    @Test
    public void limitTest(){
        GPUtils.printText("测试limit和offset");
        String sql="select * from employees order by id limit 3 offset 2";
        System.out.println(t+sql);
        Connection con=GPConnectionUtils.getConn();
        PreparedStatement ps=null;
        ResultSet rs=null;
        try {
            ps=con.prepareStatement(sql);
            rs=ps.executeQuery();
            while(rs.next()){
                System.out.println(rs.getString(1)+"--"+rs.getString(2)+"--"+
                        rs.getString(3)+"--"+rs.getString(4)+"--"+
                        rs.getString(5));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            GPConnectionUtils.close(rs,ps);
        }
    }

    /**
     * 测试索引
     */
    @Test
    public void indexTest(){
        GPUtils.printText("测试索引");
        String sql="create index employees_id_idx on employees(id)";
        Connection con=GPConnectionUtils.getConn();
        PreparedStatement ps=null;
        ResultSet rs=null;
        try {
            ps=con.prepareStatement(sql);
            ps.execute();
            sql="select * from employees where id=4";
            ps=con.prepareStatement(sql);
            rs=ps.executeQuery();
            while (rs.next()){
                System.out.println(rs.getString(1)+"--"+rs.getString(2)+"--"+
                        rs.getString(3)+"--"+rs.getString(4));
            }
            sql="drop index employees_id_idx";
            ps=con.prepareStatement(sql);
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            GPConnectionUtils.close(rs,ps);
        }
    }

    /**
     * 测试日期和时间函数
     */
    @Test
    public void dateTest(){
        GPUtils.printText("测试日期和时间函数");
        String sql="select age(timestamp '2017-01-26', timestamp '1951-08-15')";
        System.out.println(t+sql);
        Connection con=GPConnectionUtils.getConn();
        PreparedStatement ps=null;
        ResultSet rs=null;
        try {
            ps=con.prepareStatement(sql);
            rs=ps.executeQuery();
            while (rs.next()){
                System.out.println(rs.getString(1));
            }
            sql="SELECT CURRENT_TIME";
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            rs=ps.executeQuery();
            while (rs.next()){
                System.out.println(rs.getString(1));
            }
            sql="SELECT CURRENT_DATE";
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            rs=ps.executeQuery();
            while (rs.next()){
                System.out.println(rs.getString(1));
            }
            sql="SELECT CURRENT_TIMESTAMP";
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            rs=ps.executeQuery();
            while (rs.next()){
                System.out.println(rs.getString(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            GPConnectionUtils.close(rs,ps);
        }
    }

    /**
     * 测试集合(union、intersect、except)
     */
    @Test
    public void collectionTest(){
        GPUtils.printText("测试集合(union、intersect、except)");
        String sql="create table tbl_test1(a int,b varchar(10),c varchar(5))";
        System.out.println(t+sql);
        Connection con=GPConnectionUtils.getConn();
        PreparedStatement ps=null;
        ResultSet rs=null;
        try {
            ps=con.prepareStatement(sql);
            ps.execute();
            sql="create table tbl_test2(e int,f varchar(10),g varchar(32))";
            ps=con.prepareStatement(sql);
            ps.execute();
            sql="insert into tbl_test1(a,b,c) values (1,'HA','12'),(2,'ha','543')";
            ps=con.prepareStatement(sql);
            ps.execute();
            sql="insert into tbl_test2(e,f,g) values (1,'HA','dh'),(3,'hk','76sskjhk')";
            ps=con.prepareStatement(sql);
            ps.execute();
            sql="select a,b from tbl_test1 union select e,f from tbl_test2";
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            rs=ps.executeQuery();
            while (rs.next()){
                System.out.println(rs.getString(1)+"--"+rs.getString(2));
            }
            sql="select a,b from tbl_test1 intersect select e,f from tbl_test2";
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            rs=ps.executeQuery();
            while (rs.next()){
                System.out.println(rs.getString(1)+"--"+rs.getString(2));
            }
            sql="select a,b from tbl_test1 except select e,f from tbl_test2";
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            rs=ps.executeQuery();
            while (rs.next()){
                System.out.println(rs.getString(1)+"--"+rs.getString(2));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            GPConnectionUtils.close(rs,ps);
        }
    }

    /**
     * 测试修改表
     */
    @Test
    public void alterTest(){
        GPUtils.printText("测试修改表");
        String sql="select * from employees";
        System.out.println(t+sql);
        Connection con=GPConnectionUtils.getConn();
        PreparedStatement ps=null;
        ResultSet rs=null;
        try {
            ps=con.prepareStatement(sql);
            rs=ps.executeQuery();
            while (rs.next()){
                System.out.println(rs.getString(1)+"--"+rs.getString(2)+"--"+rs.getString(3)+"--"+
                        rs.getString(4)+"--"+rs.getString(5));
            }
            sql="alter table employees add gender char(1) default '男'";
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            ps.execute();
            sql="select * from employees";
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            rs=ps.executeQuery();
            while (rs.next()){
                System.out.println(rs.getString(1)+"--"+rs.getString(2)+"--"+rs.getString(3)+"--"+
                        rs.getString(4)+"--"+rs.getString(5)+"--"+rs.getString(6));
            }
            sql="alter table employees drop gender";
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            GPConnectionUtils.close(rs,ps);
        }
    }

    /**
     * 测试事务
     */
    @Test
    public void transactionTest(){
        GPUtils.printText("测试事务");
        String sql="update employees set age=30 where id=1";
        System.out.println(t+sql);
        Connection con=GPConnectionUtils.getConn();
        PreparedStatement ps=null;
        ResultSet rs=null;
        try {
            con.setAutoCommit(false);
            ps=con.prepareStatement(sql);
            ps.execute();
            Savepoint point=con.setSavepoint("upd");
            con.rollback();
            System.out.println("select * from employees");
            GPUtils.selectEmployees("select * from employees",con,ps,rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            GPConnectionUtils.close(rs,ps);
        }
    }

    /**
     * 测试子查询
     */
    @Test
    public void subQueryTest(){
        GPUtils.printText("测试子查询");
        String sql="select * from employees where id in (select id from department)";
        System.out.println(t+sql);
        Connection con=GPConnectionUtils.getConn();
        PreparedStatement ps=null;
        ResultSet rs=null;
        GPUtils.selectEmployees(sql,con,ps,rs);
    }

    /**
     * 测试serial自动增长
     */
    @Test
    public void serialTest(){
        GPUtils.printText("测试real自动增长");
        String sql="create table tinfo(id serial primary key,name varchar(20))";
        System.out.println(t+sql);
        Connection con=GPConnectionUtils.getConn();
        PreparedStatement ps=null;
        ResultSet rs=null;
        try {
            ps=con.prepareStatement(sql);
            ps.execute();
            sql="insert into tinfo(name) values('t1'),('t2'),('t3')";
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            ps.execute();
            sql="select * from tinfo";
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            rs=ps.executeQuery();
            while (rs.next()){
                System.out.println(rs.getString(1)+"--"+rs.getString(2));
            }
            sql="drop table tinfo";
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            GPConnectionUtils.close(rs,ps);
        }
    }

    /**
     * 测试权限控制
     */
    @Test
    public void authorityTest(){
        GPUtils.printText("测试权限控制");
        String sql="create user initest with password 'initest'";
        System.out.println(t+sql);
        Connection con=GPConnectionUtils.getConn();
        PreparedStatement ps=null;
        try {
            ps=con.prepareStatement(sql);
            ps.execute();
            sql="grant all on employees to initest";
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            ps.execute();
            sql="revoke all on employees from initest";
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            ps.execute();
            sql="drop user initest";
            System.out.println(t+sql);
            ps=con.prepareStatement(sql);
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            GPConnectionUtils.close(null,ps);
        }
    }

}
