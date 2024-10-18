import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Hadoop {

    // MapPhase: Xử lý dữ liệu đầu vào và xác định partition dựa trên giá trị
    public static class MapPhase extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            try {
                String line = value.toString();
                int each_line_data = Integer.parseInt(line.trim());
                Text each_line_key = new Text(determineKey(each_line_data));
                outputCollector.collect(each_line_key, new IntWritable(each_line_data));
            } catch (NumberFormatException e) {
                System.err.println("Lỗi định dạng số: " + value.toString());
            }
        }

        private static String determineKey(int value) {
            if (value < Constant.MAX_PARTITION1) {
                return Constant.KEY1;
            } else if (value < Constant.MAX_PARTITION2) {
                return Constant.KEY2;
            } else if (value < Constant.MAX_PARTITION3) {
                return Constant.KEY3;
            } else if (value < Constant.MAX_PARTITION4) {
                return Constant.KEY4;
            } else {
                return Constant.KEY5;
            }
        }
    }

    // ReducePhase: Xây dựng B+ Tree và ghi ra HDFS
    public static class ReducePhase extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {

        @Override
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            // Khởi tạo danh sách để chứa các giá trị
            List<Integer> valueList = new ArrayList<>();

            // Thêm các giá trị vào danh sách
            while (values.hasNext()) {
                int value = values.next().get();
                valueList.add(value);
            }

            // Kiểm tra nếu danh sách trống, không cần tiếp tục xử lý
            if (valueList.isEmpty()) {
                System.err.println("Không có giá trị nào cho khóa: " + key.toString());
                return;
            }

            // Xây dựng B+ Tree từ danh sách giá trị
            BPlusTree bPlusTree = new BPlusTree(4); // Order của B+ Tree
            bPlusTree.buildBottomUp(valueList);

            // Phát ra thông tin sau khi hoàn thành ghi B+ Tree
            output.collect(key, new Text("Chiều cao cây " + key.toString() + " " + bPlusTree.getHeight()));
        }
    }

    // Main: Cấu hình và chạy job Hadoop
    public static void main(String[] args) throws Exception {
        long totalStartTime = System.currentTimeMillis();

        JobConf conf = new JobConf(Hadoop.class);
        conf.setJobName("BPlusTree");
        conf.setNumMapTasks(3);
        conf.setNumReduceTasks(5);

        // Cấu hình mapper và reducer
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setMapperClass(MapPhase.class);
        conf.setReducerClass(ReducePhase.class);

        // Đường dẫn vào và ra
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        try {
            long startTimeJob = System.currentTimeMillis(); // Thời gian bắt đầu job
            JobClient.runJob(conf);
            long endTimeJob = System.currentTimeMillis(); // Thời gian kết thúc job
            System.out.println("Thời gian thực thi job Hadoop: " + (endTimeJob - startTimeJob) + "ms");

            // Đọc kết quả từ thư mục đầu ra
            FileSystem fs = FileSystem.get(conf);
            Path outputPath = new Path(args[1]);
            // Duyệt qua các file trong thư mục đầu ra
            for (FileStatus status : fs.listStatus(outputPath)) {
                if (status.isFile()) {
                    try (FSDataInputStream in = fs.open(status.getPath());
                         BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
                        String line;
                        while ((line = reader.readLine()) != null) {
                            System.out.println(line);
                        }
                    }
                }
            }

        } catch (Exception e) {
            System.err.println("Đã xảy ra lỗi khi chạy job Hadoop: " + e.getMessage());
            e.printStackTrace();
        }

        long totalEndTime = System.currentTimeMillis(); // Thời gian kết thúc toàn bộ quy trình
        System.out.println("Tổng thời gian thực thi: " + (totalEndTime - totalStartTime) + "ms");
    }

}
