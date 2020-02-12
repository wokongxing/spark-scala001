package main.scala.com.xiaolin.TEST;

public class OOMTest {
    public static void main(String[] args) {
        int i=1;
        for (;;){
            com.xiaolin.CreteLog.AccessLog log = new com.xiaolin.CreteLog.AccessLog();
            System.out.println(i);
            i++;
        }
    }
}
