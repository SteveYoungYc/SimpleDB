package simpledb.transaction;

import java.util.concurrent.ConcurrentLinkedQueue;

public class Digraph {
    private final int V;//顶点数目
    private int E;//边的数目
    private final ConcurrentLinkedQueue<Integer>[] adj;//邻接表

    public Digraph(int V) {
        this.V = V;
        this.E = 0;
        adj = new ConcurrentLinkedQueue[V];
        for (int v = 0; v < V; ++v) {
            adj[v] = new ConcurrentLinkedQueue<>();
        }
    }

    public int V() {
        return V;
    }//获取顶点数目

    public int E() {
        return E;
    }//获取边的数目

    //注意，只有这里与无向图不同
    public void addEdge(int v, int w) {
        adj[v].add(w);//将w添加到v的链表中
        E++;
    }

    public Iterable<Integer> adj(int v) {
        return adj[v];
    }

    //获取有向图的取反
    public Digraph reverse() {
        Digraph R = new Digraph(V);
        for (int v = 0; v < V; v++) {
            for (int w : adj(V))
                R.addEdge(w, v);//改变加入的顺序
        }
        return R;
    }
}