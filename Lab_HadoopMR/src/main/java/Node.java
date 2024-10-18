/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author andymai
 */
public class Node implements Serializable {
    // True for leaf nodes, False for internal nodes
    boolean isLeaf;

    // The keys stored in this node
    List<Integer> keys;

    // Children nodes (for internal nodes)
    List<Node> children;

    // Link to the next leaf node
    Node next;

    // Constructor to initialize a node
    public Node(boolean isLeaf) {
        this.isLeaf = isLeaf;
        this.keys = new ArrayList<>();
        this.children = new ArrayList<>();
        this.next = null;
    }
}
