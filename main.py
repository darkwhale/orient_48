import threading
from logs import make_log
from m_socket import receive
from monitor import monitor
import time
import sys

from monitor import thread_nums


"""
receive_thread 用于接收客户端传输的文件，并向mask_file中写入记录；
mask_thread 用于监控mask_file文件，并进行数据清洗和数据处理；
"""


if __name__ == '__main__':

    database = sys.argv[1]
    idle_symbol = True

    # 开启线程用于接收文件;
    receive_thread = threading.Thread(target=receive, args=(), name='receive')
    receive_thread.start()
    make_log("INFO", "文件接受程序已开启--------------")

    # 开启线程用于监控mask文件;
    mask_thread = threading.Thread(target=monitor, args=(database, 5,), name='mask')
    mask_thread.start()
    make_log("INFO", "文件处理程序已开启--------------")

    while True:
        time.sleep(5)
        if thread_nums("accept") == 0 and thread_nums("monitor") == 0 and idle_symbol:
            idle_symbol = False
            print("暂未有新数据需要处理")
        else:
            idle_symbol = True


