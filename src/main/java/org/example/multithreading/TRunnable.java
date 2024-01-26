package org.example.multithreading;

public class TRunnable implements Runnable {
    private ThreadLocal<Integer> threadLocal = new ThreadLocal<Integer>();
    private Integer localVar = 0;

    public synchronized void setLocalVar(Integer x) {
        localVar = x;
        System.out.println("setting local variable to " + x);
    }

    public Integer getLocalVar() { return localVar; }

    @Override
    public void run() {
        Integer i = (int) (Math.random() * 100D);
        threadLocal.set(i);
        setLocalVar(i);

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
        }
        System.out.println("Thread Name= "+Thread.currentThread().getName()
                +" threadlocal value = "+ threadLocal.get() + " local variable value " + getLocalVar());
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
        }
        System.out.println("Thread Name= "+Thread.currentThread().getName()
                +" threadlocal value = "+ threadLocal.get() + " local variable value " + getLocalVar());
    }
}
