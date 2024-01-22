package test;

/**
 * @ClassName: Thread
 * @Description: TODO 类描述
 * @Author: Tanjh
 * @Date: 2023/01/08 12:53
 * @Company: Copyright©
 **/

public class ThreadTest {
    public static void main(String[] args) throws InterruptedException {
        Thread t1 = new Thread();
        Thread t2 = new Thread();

        t2.start();
        t1.start();

        t1.sleep(100);
        t2.wait(100);

    }
}
