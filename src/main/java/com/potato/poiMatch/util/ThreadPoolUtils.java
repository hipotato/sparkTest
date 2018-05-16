package com.potato.poiMatch.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by potato on 2018/5/11.
 */
public class ThreadPoolUtils {
    private ThreadPoolUtils() {
    }

    private static final ExecutorService threadPool = Executors.newCachedThreadPool();

    public static ExecutorService getThreadPool() {
        return threadPool;
    }
}
