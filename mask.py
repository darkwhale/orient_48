import os
import threading

file_lock = threading.Lock()

mask_file = 'mask/mask'
if not os.path.exists(os.path.dirname(mask_file)):
    os.mkdir(os.path.dirname(mask_file))

if not os.path.exists(mask_file):
    os.mknod(mask_file)


# 追加记录;
def write_mask(mask):
    # 上锁，多线程同步；
    with file_lock:
        with open(mask_file, 'a') as appender:
            appender.write(mask.strip() + '\n')


# 读记录；
def read_mask():

    with file_lock:

        with open(mask_file, 'r') as reader:
            mask_str = reader.readlines()

    return mask_str


# 删除记录；
def delete_mask(mask):

    with file_lock:

        with open(mask_file, 'r') as reader:
            mask_str = reader.readlines()

        mask_str = [sub_mask for sub_mask in mask_str if sub_mask.strip() != mask.strip()]

        with open(mask_file, 'w') as writer:
            writer.write(''.join(mask_str))



