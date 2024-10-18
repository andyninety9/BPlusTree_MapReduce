import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class HDFS {
    public static byte[] serializeBPlusTree(BPlusTree bPlusTree) throws IOException {
        try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
             ObjectOutputStream objectStream = new ObjectOutputStream(byteStream)) {
            objectStream.writeObject(bPlusTree);
            return byteStream.toByteArray();
        }
    }
}
