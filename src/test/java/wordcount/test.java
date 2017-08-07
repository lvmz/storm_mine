package wordcount;

import java.util.LinkedList;

/**
 * Created by lmz on 2017/7/25.
 */
public class test {
    public static void main(String[] args) {
        int array[]=new int[]{1,2,3};
        LinkedList<Integer> list = new LinkedList<>();
        for (int a:array) {
            a=a+1;
            list.add(a);
        }
        for (int b:list )
            System.out.println(b);
    }
}
