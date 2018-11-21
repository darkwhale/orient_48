import os
import time
import shutil
import threading
import subprocess
from dataclean.clean import process_dir
from hive import hive

from logs import make_log
from mask import read_mask
from mask import delete_mask
from zip_file import unzip_file
from utils import transfer


# 计算该类的线程数；
def thread_nums(name="monitor"):
    thread_list = [thread for thread in threading.enumerate()
                   if thread.getName().startswith(name)]

    return len(thread_list)


# 清洗入库单个文件；
def monitor_data(file, in_monitor, database):
    try:
        make_log("INFO", "数据清洗：" + file)

        # 第一步，解压数据；
        unzip_dir = unzip_file(file)

        # todo 清洗，入库;待调试；
        merge_dir = process_dir(unzip_dir)

        hive(os.path.abspath(merge_dir), database)

        # make_log("INFO", "清洗完成：" + file)

        delete_mask(file)
        make_log("INFO", "清洗完毕：" + file)

        # 转移文件；
        transfer(merge_dir, os.path.join(os.path.dirname(os.path.dirname(merge_dir)), 'merge'))

        # shutil.rmtree(os.path.dirname(file))

    except subprocess.CalledProcessError:
        make_log("ERROR", "数据入库未完成：" + file)

    except FileNotFoundError:
        make_log("ERROR", "文件不存在" + file)

        delete_mask(file)
    finally:
        in_monitor.remove(file + '\n')


# 监控数据并调用清洗接口；
def monitor(database, thread_num=5):

    # 定义正在处理中的文件；
    in_monitor = []

    while True:
        time.sleep(3)

        mask_str = read_mask()

        # new_monitor代表了所有未进行处理的文件；
        new_monitor = [file for file in mask_str if file not in in_monitor]

        # 计算最大可新建的线程数；
        free_thread_nums = thread_num - thread_nums("monitor")

        max_thread_num = free_thread_nums if free_thread_nums \
                                             < len(new_monitor) else len(new_monitor)

        # 开启max_thread_num个线程用于处理数据；
        for i in range(max_thread_num):
            compress_thread = threading.Thread(target=monitor_data,
                                               args=(new_monitor[i].strip(), in_monitor, database),
                                               name="monitor")

            # 将该文件夹放入in_compress列表中；
            in_monitor.append(new_monitor[i])

            compress_thread.start()


