package com.mn.cdc.function;

/**
 * @program:cdc-master
 * @description 函数式接口，接收一个参数，仅此而已
 * @author:miaoneng
 * @create:2021-09-09 09:22
 **/
@FunctionalInterface
public interface BlockingConsumer<T> {
    void accept(T t) throws InterruptedException;
}
