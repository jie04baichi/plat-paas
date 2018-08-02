/**
 * 
 * @author PJieWin
 * ���Էֲ�ʽ�����񵥣�ͨ��ConcurrentTest����ִ������֤�ֲ�ʽ��
 */
public class CommonTask {

	static
	{
		
	}

    public void run() {
        try {
//            lock = new DistributedLock("192.168.1.158:2181","product-number");
            System.out.println("<<<<<<<<<Thread " + Thread.currentThread().getId() + " running, is time to do something !!!");
            //Thread.sleep(3000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
        }
         
    }

}
