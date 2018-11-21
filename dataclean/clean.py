from dataclean.utils import clean_folder
from dataclean.utils import merge_files


# 处理文件；目录,数据库名,日期
def process_dir(folder):

    # 清洗文件夹，获取清洗后的目录名；
    new_data_dir = clean_folder(folder)

    # 合并小文件；
    merge_data_dir = merge_files(new_data_dir)

    return merge_data_dir

