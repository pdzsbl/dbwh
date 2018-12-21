from multiprocessing import Process,Pool
import os
import subprocess
import time
import random

#我们比较慢的操作都是cpu密集型的，所以采用多进程，多线程适合IO密集型任务

def foo(a):
    print(a)
    time.sleep(3)
    print(a)
    return a+1

if __name__=='__main__':
    p = Pool(5)
    k = []
    for i in range(5):
        result = p.apply_async(foo, args=(i, ))
        k.append(result)
    # foo和a分别是你的方法和参数，这行可以写多个，执行多个进程，返回不同结果
    p.close()
    p.join()
    for i in range(5):
        print("get",k[i].get())

