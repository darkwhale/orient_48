# -*- coding: UTF-8 -*-
# 多线程接收数据；
import socket
import struct
import threading
import os
from logs import make_log

from utils import get_database
from mask import write_mask

# 接收文件接口；
port = 12345


# 获取新文件夹名；
def get_new_dir(file_name, disk_index):
    # return os.path.join('receive', get_database(file_name), file_name[:-4])
    return os.path.join('HDATA', str(disk_index), get_database(file_name), file_name[:-4])


# 多线程，传输完毕可直接进行清洗；
def receive_thread(connection):

    try:
        connection.settimeout(600)

        file_info_size = struct.calcsize('128sl')

        buf = connection.recv(file_info_size)

        if buf:
            file_name, file_size = struct.unpack('128sl', buf)

            file_name = file_name.decode().strip('\00')

            # 查找最小的目录用于存储文件；
            disk_index = file_name[0]
            file_name = file_name[1:]

            # todo 生产时替换；
            # 在receive下用时间戳创建新的文件夹，防止命名冲突；
            file_new_dir = get_new_dir(file_name, disk_index)

            if not os.path.exists(file_new_dir):
                os.makedirs(file_new_dir)

            file_new_name = os.path.join(file_new_dir, file_name)
            file_new_name_size = 0

            if os.path.exists(file_new_name):
                file_new_name_size = os.path.getsize(file_new_name)

            connection.send(str(file_new_name_size).encode())

            received_size = file_new_name_size

            w_file = open(file_new_name, 'ab')

            make_log("INFO", "开始接收文件:" + file_name)

            finished = True

            while not received_size == file_size:
                r_data = connection.recv(10240)
                received_size += len(r_data)

                # 未接收到data表示连接断开；
                if len(r_data) == 0:
                    finished = False
                    break
                w_file.write(r_data)

            w_file.close()

            if finished:
                make_log("INFO", "传输完成： %s" % file_name)

                # 写到记录文件里；

                write_mask(file_new_name)
            else:
                make_log("ERROR", "传输失败： %s" % file_name)

        connection.close()

    except socket.timeout:
        print("连接超时！")
    finally:
        connection.close()


def receive():
    host = socket.gethostname()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((host, port))
    sock.listen(5)

    # print("服务已启动---------------")

    while True:
        connection, address = sock.accept()
        print("接收地址：", address)
        thread = threading.Thread(target=receive_thread, args=(connection, ), name="accept")
        thread.start()
