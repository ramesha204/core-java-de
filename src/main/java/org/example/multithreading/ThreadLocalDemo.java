package org.example.multithreading;

public class ThreadLocalDemo {

    public static void main(String[] args) {
        TRunnable sharedRunnableInstance = new TRunnable();

        Thread thread1 = new Thread(sharedRunnableInstance);
        Thread thread2 = new Thread(sharedRunnableInstance);

        thread1.start();
        thread2.start();
    }

}
