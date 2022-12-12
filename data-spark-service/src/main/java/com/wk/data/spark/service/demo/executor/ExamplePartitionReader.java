//package com.wk.data.spark.service.demo.executor;
//
//import com.wk.data.spark.infrastructure.config.ShardingsphereDatasource;
//import lombok.SneakyThrows;
//import org.apache.spark.sql.catalyst.InternalRow;
//import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
//import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
//
//import java.io.IOException;
//import java.sql.Connection;
//import java.sql.PreparedStatement;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import java.util.ArrayList;
//import java.util.List;
//
///**
// * @author smash_hq
// */
//public class ExamplePartitionReader implements InputPartitionReader<InternalRow> {
//
//    public ExamplePartitionReader(String db, String table) {
//        data(db, table);
//    }
//
//    /**
//     * 构建一个数据结构
//     */
//    private static class Person {
//        private int id;
//        private int age;
//
//        public Person(int id, int age) {
//            this.id = id;
//            this.age = age;
//        }
//
//        public int getAge() {
//            return age;
//        }
//
//    }
//
//    private static List<Person> people = new ArrayList<>();
//
//    private void data(String db, String table) {
//        Connection connection;
//        PreparedStatement statement;
//        {
//            try {
//                String sql = "select id,age from " + table;
//                ShardingsphereDatasource datasource = new ShardingsphereDatasource();
//                connection = datasource.dataSource(table, "id").getConnection();
//                statement = connection.prepareStatement(sql);
//                ResultSet set = statement.executeQuery();
//                while (set.next()) {
//                    people.add(new Person(set.getInt("id"), set.getInt("age")));
//                }
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//        }
//    }
//
//    private int index = 0;
//
//    @SneakyThrows
//    @Override
//    public boolean next() {
//        return index < people.size();
//    }
//
//
//    @SneakyThrows
//    @Override
//    public InternalRow get() {
//
//        GenericInternalRow genericInternalRow;
//        // 进行connection的操作
//        genericInternalRow = new GenericInternalRow(new Object[]{people.get(index).id, people.get(index).age});
//        index++;
//        return genericInternalRow;
//    }
//
//    @Override
//    public void close() throws IOException {
//
//    }
//}
