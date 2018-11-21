"""
生成hive脚本并运行；
"""

import subprocess
import os


# 写sql文件；
def make_sql(sql_path, *sql_list):

    sql_strs = ';\n'.join([sql for sql in sql_list]) + ';'

    with open(sql_path, 'w') as write_sql:
        write_sql.write(sql_strs)


def hive(dir, database):
    # todo 添加sql语言;
    hive_file_path = os.path.join(os.path.dirname(dir), 'hive.sql')

    table_name = os.path.basename(os.path.dirname(os.path.dirname(dir)))

    create_sql = "create table if not exists src_" + table_name + "\n(insert_day string, \n" \
                                                           "orderno string, \n" \
                                                           "biztype string, \n" \
                                                           "transtype string, \n" \
                                                           "payment string, \n" \
                                                           "iscod int, \n" \
                                                           "codcy int, \n" \
                                                           "codvalue float, \n" \
                                                           "isinsure int, \n" \
                                                           "insurevalue float, \n" \
                                                           "isspu int, \n" \
                                                           "isinspect int, \n" \
                                                           "iscustoms int, \n" \
                                                           "specialbiz string, \n" \
                                                           "companycode string, \n" \
                                                           "companyname string, \n" \
                                                           "gname string, \n" \
                                                           "gtype string, \n" \
                                                           "gqty int, \n" \
                                                           "gwt float, \n" \
                                                           "gvol float, \n" \
                                                           "gpkg string, \n" \
                                                           "gsize float, \n" \
                                                           "scountry string, \n" \
                                                           "sprovince string, \n" \
                                                           "scity string, \n" \
                                                           "sdistrict string, \n" \
                                                           "scsrcode string, \n" \
                                                           "stbid string, \n" \
                                                           "sorgan string, \n" \
                                                           "sname string, \n" \
                                                           "sid string, \n" \
                                                           "smobile string, \n" \
                                                           "smobileattr string, \n" \
                                                           "smobiletype string, \n" \
                                                           "stel string, \n" \
                                                           "sadd string, \n" \
                                                           "szip string, \n" \
                                                           "scouriername string, \n" \
                                                           "scouriermobile string, \n" \
                                                           "sbrcode string, \n" \
                                                           "sbrname string, \n" \
                                                           "sbrtel string, \n" \
                                                           "sbradd string, \n" \
                                                           "colltime string, \n" \
                                                           "ordertime string, \n" \
                                                           "delytime string, \n" \
                                                           "eatime string, \n" \
                                                           "sendtime string, \n" \
                                                           "signofftime string, \n" \
                                                           "rcountry string, \n" \
                                                           "rprovince string, \n" \
                                                           "rcity string, \n" \
                                                           "rdistrict string, \n" \
                                                           "rcsrcode string, \n" \
                                                           "rtbid string, \n" \
                                                           "rorgan string, \n" \
                                                           "rname string, \n" \
                                                           "rid string, \n" \
                                                           "rmobile string, \n" \
                                                           "rmobileattr string, \n" \
                                                           "rmobiletype string, \n" \
                                                           "rtel string, \n" \
                                                           "radd string, \n" \
                                                           "rzip string, \n" \
                                                           "rcouriername string, \n" \
                                                           "rcouriermobile string, \n" \
                                                           "rbrcode string, \n" \
                                                           "rbrname string, \n" \
                                                           "rbrtel string, \n" \
                                                           "rbradd string, \n" \
                                                           ")row format delimited \n" \
                                                           "fields terminated by ',' \n" \
                                                           "lines terminated by '\\n' " \
                                                           "stored as textfile "

    load_data_sql = "load data local inpath '" + dir + "' into table src_" + table_name

    make_sql(hive_file_path,
             "set hive.exec.compress.output=false",
             "set hive.exec.compress.intermediate=true",
             "set mapred.max.split.size=1000000000",
             "set mapred.min.split.size.per.node=1000000000",
             "set mapred.min.split.size.per.rack=1000000000",
             "set hive.groupby.skewindata=true",
             "set hive.auto.convert.join=true",
             "set hive.exec.dynamic.partition.mode=nostrick",
             "set hive.exec.dynamic.partition=true",
             "set hive.stats.autogather=false",
             "set hive.stats.reliable=false",
             "use " + database,
             create_sql,
             load_data_sql
             )

    # todo 待验证；
    hive_command = "hive -f " + hive_file_path

    return subprocess.check_output(hive_command,)

