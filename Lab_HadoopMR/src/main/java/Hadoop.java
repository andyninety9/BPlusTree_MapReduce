import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

// Định nghĩa lớp Constant để lưu các giá trị MAX_PARTITION


public class Hadoop {

    // MapPhase: Xử lý dữ liệu đầu vào và xác định partition dựa trên giá trị
    public static class MapPhase extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String line = value.toString();
                int each_line_data = Integer.parseInt(line.trim());
                Text each_line_key = new Text(determineKey(each_line_data));
                context.write(each_line_key, new IntWritable(each_line_data));
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
    public static class ReducePhase extends Reducer<Text, IntWritable, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // Khởi tạo danh sách để chứa các giá trị
            List<Integer> valueList = new ArrayList<>();

            // Thêm các giá trị vào danh sách
            for (IntWritable value : values) {
                valueList.add(value.get());
            }

            if (valueList.isEmpty()) {
                System.err.println("Không có giá trị nào cho khóa: " + key.toString());
                return;
            }

            BPlusTree bPlusTree = new BPlusTree(10); // Order của B+ Tree
            bPlusTree.buildBottomUp(valueList);


            // Phát ra thông tin sau khi hoàn thành ghi B+ Tree
            context.write(new Text("Chiều cao cây " + key.toString() + ": "), new Text(String.valueOf(bPlusTree.getHeight())));
        }
    }

    // Main: Cấu hình và chạy job Hadoop
    public static void main(String[] args) throws Exception {
        long totalStartTime = System.currentTimeMillis();

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "BPlusTree");
        job.setJarByClass(Hadoop.class);

        // Cấu hình mapper và reducer
        job.setMapperClass(MapPhase.class);
        job.setReducerClass(ReducePhase.class);

        job.setOutputKeyClass(Text.class); // Key là Text
        job.setOutputValueClass(IntWritable.class); // Value là IntWritable

        // Đường dẫn vào và ra
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        try {
            long startTimeJob = System.currentTimeMillis(); // Thời gian bắt đầu job
            boolean success = job.waitForCompletion(true);
            long endTimeJob = System.currentTimeMillis(); // Thời gian kết thúc job
            System.out.println("Thời gian thực thi job Hadoop: " + (endTimeJob - startTimeJob) + "ms");

            if (success) {
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
            } else {
                System.err.println("Job Hadoop thất bại.");
            }
        } catch (Exception e) {
            System.err.println("Đã xảy ra lỗi khi chạy job Hadoop: " + e.getMessage());
            e.printStackTrace();
        }

        long totalEndTime = System.currentTimeMillis(); // Thời gian kết thúc toàn bộ quy trình
        System.out.println("Tổng thời gian thực thi: " + (totalEndTime - totalStartTime) + "ms");
    }
}