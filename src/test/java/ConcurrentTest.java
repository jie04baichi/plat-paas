
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
 
/**
  ConcurrentTask[] task = new ConcurrentTask[5];
  for(int i=0;i<task.length;i++){
       task[i] = new ConcurrentTask(){
            public void run() {
                System.out.println("==============");
                 
            }};
  }
  new ConcurrentTest(task);
 * @author pengjie
 *
 */
public class ConcurrentTest {
    private CountDownLatch startSignal = new CountDownLatch(1);//��ʼ����
    private CountDownLatch doneSignal = null;//������
    private CopyOnWriteArrayList<Long> list = new CopyOnWriteArrayList<Long>();
    private AtomicInteger err = new AtomicInteger();//ԭ�ӵ���
    private CommonTask[] task = null;
     
    public ConcurrentTest(CommonTask... task){
        this.task = task;
        if(task == null){
            System.out.println("task can not null");
            System.exit(1);
        }
        doneSignal = new CountDownLatch(task.length);
        start();
        
    }
    /**
     * @param args
     * @throws ClassNotFoundException
     */
    private void start(){
        //�����̣߳����������̵߳ȴ��ڷ��Ŵ�
        createThread();
        //�򿪷���
        startSignal.countDown();//�ݼ�������ļ�����������㣬���ͷ����еȴ���߳�
        try {
            doneSignal.await();//�ȴ������̶߳�ִ�����
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //����ִ��ʱ��
        getExeTime();
    }
    /**
     * ��ʼ�������̣߳����ڷ��Ŵ��ȴ�
     */
    private void createThread() {
        long len = doneSignal.getCount();
        for (int i = 0; i < len; i++) {
            final int j = i;
            new Thread(new Runnable(){
                public void run() {
                    try {
                        startSignal.await();//ʹ��ǰ�߳������������������֮ǰһֱ�ȴ�
                        long start = System.currentTimeMillis();
                        task[j].run();
                        long end = (System.currentTimeMillis() - start);
                        list.add(end);
                    } catch (Exception e) {
                        err.getAndIncrement();//�൱��err++
                    }
                    doneSignal.countDown();
                }
            }).start();
        }
    }
    /**
     * ����ƽ����Ӧʱ��
     */
    private void getExeTime() {
        int size = list.size();
        List<Long> _list = new ArrayList<Long>(size);
        _list.addAll(list);
        Collections.sort(_list);
        long min = _list.get(0);
        long max = _list.get(size-1);
        long sum = 0L;
        for (Long t : _list) {
            sum += t;
        }
        long avg = sum/size;
        System.out.println("min: " + min);
        System.out.println("max: " + max);
        System.out.println("avg: " + avg);
        System.out.println("err: " + err.get());
    } 
}