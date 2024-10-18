/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *
 * @author andymai
 */
public class BPlusTree implements Serializable {
    private static final long serialVersionUID = 1L;

    // Root node of the tree
    private Node root;

    // Maximum number of keys per node
    private final int order;

    // Constructor to initialize the B+ Tree
    public BPlusTree(int order) {
        if (order < 3) {
            throw new IllegalArgumentException("Order must be at least 3");
        }
        this.root = new Node(true);
        this.order = order;
    }

    public Node getRoot() {
        return root;
    }

    public void setRoot(Node root) {
        this.root = root;
    }

    // Find the appropriate leaf node for insertion
    private Node findLeaf(int key) {
        Node node = root;
        while (!node.isLeaf) {
            int i = 0;
            while (i < node.keys.size() && key >= node.keys.get(i)) {
                i++;
            }
            node = node.children.get(i);
        }
        return node;
    }

    // Insert a key into the B+ Tree
    public void insert(int key) {
        Node leaf = findLeaf(key);
        insertIntoLeaf(leaf, key);

        // Split the leaf node if it exceeds the order
        if (leaf.keys.size() > order - 1) {
            splitLeaf(leaf);
        }
    }

    // Insert into the leaf node
    private void insertIntoLeaf(Node leaf, int key) {
        int pos = Collections.binarySearch(leaf.keys, key);
        if (pos < 0) {
            pos = -(pos + 1);
        }
        leaf.keys.add(pos, key);
    }

    // Split a leaf node and update parent nodes
    private void splitLeaf(Node leaf) {
        int mid = (order + 1) / 2;
        Node newLeaf = new Node(true);

        // Move half the keys to the new leaf node
        if (mid <= leaf.keys.size()) {
            newLeaf.keys.addAll(leaf.keys.subList(mid, leaf.keys.size()));
            leaf.keys.subList(mid, leaf.keys.size()).clear();
        }

        newLeaf.next = leaf.next;
        leaf.next = newLeaf;

        // If the root splits, create a new root
        if (leaf == root) {
            Node newRoot = new Node(false);
            newRoot.keys.add(newLeaf.keys.get(0));
            newRoot.children.add(leaf);
            newRoot.children.add(newLeaf);
            root = newRoot;
        } else {
            insertIntoParent(leaf, newLeaf, newLeaf.keys.get(0));
        }
    }

    // Insert into the parent node after a leaf split
    private void insertIntoParent(Node left, Node right, int key) {
        Node parent = findParent(root, left);

        if (parent == null) {
            throw new RuntimeException("Parent node not found for insertion");
        }

        int pos = Collections.binarySearch(parent.keys, key);
        if (pos < 0) {
            pos = -(pos + 1);
        }

        parent.keys.add(pos, key);
        parent.children.add(pos + 1, right);

        // Split the internal node if it exceeds the order
        if (parent.keys.size() > order - 1) {
            splitInternal(parent);
        }
    }

    // Split an internal node
    private void splitInternal(Node internal) {
        int mid = (order + 1) / 2;
        Node newInternal = new Node(false);

        // Move half the keys to the new internal node
        if (mid + 1 <= internal.keys.size()) {
            newInternal.keys.addAll(internal.keys.subList(mid + 1, internal.keys.size()));
            internal.keys.subList(mid + 1, internal.keys.size()).clear();
        }

        // Move half the children to the new internal node
        if (mid + 1 <= internal.children.size()) {
            newInternal.children.addAll(internal.children.subList(mid + 1, internal.children.size()));
            internal.children.subList(mid + 1, internal.children.size()).clear();
        }

        // If the root splits, create a new root
        if (internal == root) {
            Node newRoot = new Node(false);
            newRoot.keys.add(internal.keys.get(mid));
            newRoot.children.add(internal);
            newRoot.children.add(newInternal);
            root = newRoot;
        } else {
            insertIntoParent(internal, newInternal, internal.keys.remove(mid));
        }
    }

    // Find the parent node of a given node
    private Node findParent(Node current, Node target) {
        if (current.isLeaf || current.children.isEmpty()) {
            return null;
        }

        for (int i = 0; i < current.children.size(); i++) {
            Node child = current.children.get(i);

            if (child == target) {
                // Parent found
                return current;
            }

            Node possibleParent = findParent(child, target);
            if (possibleParent != null) {
                return possibleParent;
            }
        }

        // Parent not found
        return null;
    }

    // Search for a key in the B+ Tree
    public boolean search(int key) {
        Node node = findLeaf(key);
        int pos = Collections.binarySearch(node.keys, key);
        return pos >= 0;
    }

    // Display the Tree (for debugging purposes)
    public void printTree() {
        printNode(root, 0);
    }

    private void printNode(Node node, int level) {
        System.out.println("Level " + level + ": " + node.keys);
        if (!node.isLeaf) {
            for (Node child : node.children) {
                printNode(child, level + 1);
            }
        }
    }

    // Method để xây dựng B+ Tree từ dưới lên
    public void buildBottomUp(List<Integer> keys) {
        // Kiểm tra xem danh sách keys có rỗng không
        if (keys.isEmpty()) {
            System.out.println("Danh sách khóa rỗng. Không thể xây dựng cây B+.");
            return; // Hoặc xử lý theo cách khác, như ném ngoại lệ
        }

        // Sắp xếp các khóa theo thứ tự tăng dần
        Collections.sort(keys);

        // Bước 1: Tạo các node lá
        List<Node> leaves = new ArrayList<>();
        int i = 0;

        // Lặp qua các khóa và tạo node lá
        while (i < keys.size()) {
            Node leaf = new Node(true);

            // Thêm các khóa vào node lá, không vượt quá order - 1 khóa mỗi node
            int j = 0;
            while (j < order - 1 && i < keys.size()) {
                leaf.keys.add(keys.get(i));
                i++;
                j++;
            }

            // Liên kết với node lá tiếp theo
            if (!leaves.isEmpty()) {
                leaves.get(leaves.size() - 1).next = leaf;
            }
            leaves.add(leaf);
        }

        // Bước 2: Xây dựng các node bên trong từ các node lá
        while (leaves.size() > 1) {
            List<Node> parents = new ArrayList<>();
            int k = 0;

            // Tạo các node bên trong (internal nodes) từ các node lá hoặc node bên trong hiện có
            while (k < leaves.size()) {
                Node parent = new Node(false);
                int m = 0;

                // Thêm con và khóa tương ứng vào node bên trong, không vượt quá order children mỗi node
                while (m < order && k < leaves.size()) {
                    Node child = leaves.get(k);
                    parent.children.add(child);

                    // Khóa đầu tiên của node con sẽ trở thành khóa của node bên trong
                    if (m > 0 && !child.keys.isEmpty()) { // Kiểm tra child.keys có rỗng
                        parent.keys.add(child.keys.get(0));
                    }
                    k++;
                    m++;
                }
                parents.add(parent);
            }

            // Cập nhật lại danh sách leaves bằng danh sách các node cha vừa tạo
            leaves = parents;
        }

        // Node cuối cùng trong danh sách sẽ là root của cây
        root = leaves.get(0);
    }

    public List<Integer> getAllKeys() {
        List<Integer> keys = new ArrayList<>();
        traverseKeys(root, keys);
        return keys;
    }

    private void traverseKeys(Node node, List<Integer> keys) {
        if (node.isLeaf) {
            keys.addAll(node.keys);
        } else {
            for (int i = 0; i < node.children.size(); i++) {
                traverseKeys(node.children.get(i), keys);
                if (i < node.keys.size()) {
                    keys.add(node.keys.get(i)); // Thêm khóa giữa các con
                }
            }
        }
    }

    public int getHeight() {
        return getHeight(root);
    }

    private int getHeight(Node node) {
        if (node.isLeaf) {
            return 1; // Chiều cao của node lá là 1
        }
        return 1 + getHeight(node.children.get(0)); // Thêm 1 cho chiều cao của node hiện tại
    }

    // Phương thức để đếm số lượng nút trong cây
    public int getNodeCount() {
        return getNodeCount(root);
    }

    private int getNodeCount(Node node) {
        int count = 1; // Đếm node hiện tại
        if (!node.isLeaf) {
            for (Node child : node.children) {
                count += getNodeCount(child); // Đếm số lượng nút con
            }
        }
        return count;
    }
}
