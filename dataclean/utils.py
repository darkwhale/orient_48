# -*- coding: UTF-8 -*-
import os
import shutil
import time

dest_path = "new_data"
err_path = "err_data"
merge_path = "merge_data"
stable_col_num = 70


# 获取文件夹下所有的文件路径；
def get_file_list(file_dir):
    file_list = []

    for root, dirs, files in os.walk(file_dir):
        for name in files:
            file_list.append(os.path.join(root, name))

    return file_list


# 清洗单个文件；
def clean_file(dest_dir, err_dir, cur_file):
    # 取绝对名字
    cur_file_name = os.path.basename(cur_file)

    date = cur_file_name[:8]

    dest_file_path = os.path.join(dest_dir, cur_file_name[8:])
    err_file_path = os.path.join(err_dir, cur_file_name[8:])

    # todo 用w还是a？
    dest_file = open(dest_file_path, 'w', encoding='utf-8')
    err_file = open(err_file_path, 'w', encoding='utf8')

    with open(cur_file, 'r', encoding='utf-8') as src_file:
        for cur_line in src_file:
            cur_cols = cur_line.split(',')
            col_num = len(cur_cols)

            # 处理运单号；
            waybill_delimiter = cur_line.index('<=>')

            # 未发现该符号；
            if -1 == waybill_delimiter:
                continue

            waybill_index = waybill_delimiter + 3
            cur_line = cur_line[waybill_index:]

            # 添加日期；
            cur_line = date + ',' + cur_line

            if stable_col_num == col_num:
                dest_file.write(cur_line)
            else:
                err_file.write(cur_line)

    dest_file.close()
    err_file.close()


# 清洗文件目录；unzip_dir,返回清洗后的文件夹；
def clean_folder(folder):
    # 创建文件夹；与unzip_dir在同一层目录；

    dest_dir = os.path.join(os.path.dirname(folder), dest_path)
    err_dir = os.path.join(os.path.dirname(folder), err_path)

    if os.path.exists(dest_dir):
        shutil.rmtree(dest_dir)
    os.makedirs(dest_dir)

    if os.path.exists(err_dir):
        shutil.rmtree(err_dir)
    os.makedirs(err_dir)

    # print(folder)
    file_list = get_file_list(folder)
    # print(len(file_list))

    for file in file_list:
        try:
            clean_file(dest_dir, err_dir, file)
        except Exception as e:
            print(e)

    return dest_dir


# 从new_data文件夹开始；
def merge_files(folder, block_size=128):

    # 创建文件夹；与new_data在同一层目录；

    merge_dir = os.path.join(os.path.dirname(folder), merge_path)
    if os.path.exists(merge_dir):
        shutil.rmtree(merge_dir)
    os.makedirs(merge_dir)

    database_name = os.path.basename(os.path.dirname(os.path.dirname(folder)))
    base_file_name = database_name + '_'

    cur_merge_file_size = 0

    for index, file in enumerate(get_file_list(folder)):
        # file已经是绝对路径，不需要join；
        if block_size < cur_merge_file_size or index == 0:
            cur_merge_file_size = 0
            write_file_path = os.path.join(merge_dir, base_file_name + str(time.time()) + '.csv')
            write_file = open(write_file_path, 'w', encoding='utf-8')

        cur_file_size = os.path.getsize(file)/1024/1024

        cur_merge_file_size += cur_file_size

        read_file = open(file, 'r', encoding='utf-8')

        for cur_line in read_file:
            write_file.write(cur_line)

        read_file.close()
    write_file.close()

    return merge_dir

