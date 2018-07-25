package wang.ismy.db;

import com.mysql.cj.xdevapi.JsonArray;
import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

public class Main {
    public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
        //读取文件并转为json对象
        JSONObject object=new JSONObject( FileUtils.readFileToString(new File("config.json"),"UTF8"));
        //从JSON中获取这几个数据
        String driver=object.getString("driver");
        String db1=object.getString("db1");
        String db2=object.getString("db2");
        String url=object.getString("url");
        String user=object.getString("user");
        String password=object.getString("password");
        //获取操作
        JSONArray tableOperations=object.getJSONArray("operations");


        //加载驱动
        Class.forName(driver);

        //获取两个数据库连接
        Connection connection1=DriverManager.getConnection(url+db1+"?serverTimezone=UTC",user,password);

        Connection connection2=DriverManager.getConnection(url+db2+"?serverTimezone=UTC",user,password);


        for(int i=0;i<tableOperations.length();i++){
            JSONObject table=tableOperations.getJSONObject(i);
            String table1=table.getString("table1");
            String table2=table.getString("table2");
            JSONArray rows=table.getJSONArray("rows");
            Map<String,String> rowMapper=new HashMap<String, String>();
            for(int j=0;j<rows.length();j++){
                String row1=rows.getJSONObject(j).getString("row1");
                String row2=rows.getJSONObject(j).getString("row2");
                rowMapper.put(row1,row2);
            }

            ResultSet set=connection1.prepareStatement("select * from "+table1+";").executeQuery();
            String sql="insert into "+table2+"(";
            String values="values(";
            TreeSet<String> rowSet=new TreeSet<String>(rowMapper.keySet());
            for(String str:rowSet){
                sql+=rowMapper.get(str)+",";
                values+="?,";
            }
            sql=sql.substring(0,sql.length()-1)+") ";
            values=values.substring(0,values.length()-1) +");";
            sql=sql+values;
            System.out.println(sql);
            while(set.next()){
                PreparedStatement preparedStatement=connection2.prepareStatement(sql);
                int count=1;
                for(String str:rowSet){
                    preparedStatement.setObject(count,set.getObject(str));
                    count++;
                }
                System.out.println(preparedStatement.executeUpdate());
            }
        }
    }
}
