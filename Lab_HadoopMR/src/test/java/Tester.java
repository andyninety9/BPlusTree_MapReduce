import java.io.IOException;

public class Tester {
    public static void main(String[] args) {
        BPlusTree b = new BPlusTree(4);
        b.insert(2);
        b.insert(1);
        b.insert(3);
        b.insert(5);
        b.insert(6);
        b.insert(7);
        b.insert(8);
        b.insert(9);
        b.insert(10);
        b.insert(11);
        try {
            byte[] serializeBPlusTree = HDFS.serializeBPlusTree(b);
            b.printTree();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
