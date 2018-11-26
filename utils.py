import os
import time


# 解析数据库名；
def get_database(file_name):
    file_name = os.path.basename(file_name)
    return ''.join([z for z in file_name[:-4] if z.isalpha()])


# 获取文件夹下所有文件列表；
def get_file_list(folder):
    file_list = []

    for root, dirs, files in os.walk(folder):
        for name in files:
            file_list.append(os.path.join(root, name))

    file_list.sort()

    return file_list


# 转移文件夹下的文件到另一个目录；
def transfer(src_folder, dst_folder):
    file_list = get_file_list(src_folder)
    for file in file_list:
        try:
            os.renames(file, os.path.join(dst_folder, os.path.basename(file)))
        except Exception as e:
            print(e)
            os.renames(file, os.path.join(dst_folder, os.path.basename(file) + str(time.time())))


