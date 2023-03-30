import argparse
import concurrent.futures
import multiprocessing
import os
import subprocess
import time

import mysql.connector
import yaml
from rich.prompt import Prompt
from tqdm import tqdm

from clogger import clogger
from zip_file import compress_and_delete, decompress


class MysqlBackuper:
    def __init__(self, hostname, username, password, database, port=3306, db_cwd=None, backup_dir=None, ex_opt=None,
                 max_workers=4):
        """
        初始化备份对象属性
        :param hostname: 数据库主机名或 IP 地址
        :param username: 数据库用户名
        :param password: 数据库密码
        :param database: 待备份的数据库名
        :param port: 数据库端口号，默认为 3306
        :param backup_dir: 备份文件存储目录，默认为 './backup'
        """
        self.ex_opt = "" if ex_opt is None else ' '.join(ex_opt)  # 额外的备份命令选项
        self.hostname = hostname
        self.username = username
        self.password = password
        self.database = database
        self.port = port
        self.db_cwd = db_cwd
        self.backup_dir = backup_dir
        self.mysql_exe = 'mysqldump'  # 默认备份命令
        self.max_workers = max_workers  # 默认备份线程数

    def backup_table(self, table):
        # 增加异常处理，处理数据库连接异常
        connected = False
        cnx = None
        err = ""
        while not connected:
            try:
                # 连接数据库，直到连接成功为止
                cnx = mysql.connector.connect(user=self.username, password=self.password, host=self.hostname,
                                              port=self.port, database=self.database)
                connected = True
            except Exception as ec:
                clogger.info(f"数据库连接异常:{ec}")
                time.sleep(2)

        try:
            # 输出备份进度
            # clogger.info(f"正在备份表 {table}...")
            # 构造备份命令
            #  --routines 用于在备份时同时备份存储过程和函数等程序性对象。
            cmd = f"{self.mysql_exe} --default-character-set=utf8mb4 -u {self.username} -p{self.password} " \
                  f"-h {self.hostname} -P {self.port} {self.ex_opt} {self.database} {table}"
            backup_file = os.path.join(self.db_backup_dir, table + '.sql')  # 备份文件名为表名加上后缀 .sql
            cmd = f'{cmd} > {backup_file}'
            result = subprocess.run(cmd, shell=True, cwd=self.db_cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            assert result.stdout.decode('gbk') == ''
            err = result.stderr.decode('gbk')
            assert err == ''
            time.sleep(0.5)
            # 输出备份完成信息
            # clogger.info(f"表 {table} 备份完成")
        except Exception as es:
            # 抛出备份异常信息
            clogger.info(f"表 {table} 备份失败：{es} {err}")
        finally:
            cnx.close()  # 关闭数据库连接

    db_backup_dir = None  # 备份时按照时间命名的实际sql输出工作目录

    def backup_all_tables(self):
        start_time = time.time()
        # 创建以当前时间命名的目录
        self.db_backup_dir = os.path.join(self.backup_dir, time.strftime('%Y%m%d_%H%M%S'))
        # 如果目录不存在，则创建目录
        if not os.path.exists(self.db_backup_dir):
            os.makedirs(self.db_backup_dir)

        clogger.info(f'开始备份数据库{self.database},目录:{self.db_backup_dir}')

        # 获取所有表的名称
        cnx = mysql.connector.connect(user=self.username, password=self.password, host=self.hostname, port=self.port,
                                      database=self.database)
        cursor = cnx.cursor()
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor]
        cursor.close()
        cnx.close()

        # 使用线程池并发备份所有表格
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {}
            # 构造备份每个表格的任务
            for i, table in enumerate(tables):
                future = executor.submit(self.backup_table, table)
                futures[future] = i + 1
            # 遍历已完成的备份任务，更新备份进度信息，并输出备份完成情况
            for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), ncols=80, desc='进度',
                               unit='个表格', unit_scale=True):
                pass
        clogger.info(f"正在备份第 {futures[future]}/{len(tables)} 个表格，已完成 100.00%")

        # 输出备份完成信息
        clogger.info("备份完成！")
        end_time = time.time()
        clogger.info(f"备份总共耗时：{end_time - start_time:.2f}秒")

    def restore_table(self, restore_dir, table_file):
        max_retry = 3  # 最大重试次数
        retry_count = 0  # 已尝试的次数
        err = ""
        while retry_count < max_retry:
            try:
                # 构造还原命令
                backup_file = os.path.join(restore_dir, table_file)  # 备份文件名为表名加上后缀 .sql
                cmd = f"mysql -u {self.username} -p{self.password} " \
                      f"-h {self.hostname} -P {self.port} --default-character-set=utf8mb4 {self.database}"
                cmd = f'{cmd} < {backup_file}'
                result = subprocess.run(cmd, shell=True, cwd=self.db_cwd, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE)
                assert result.stdout.decode('gbk') == ''
                err = result.stderr.decode('gbk')
                assert err == ''
                time.sleep(0.5)
                # 输出还原完成信息
                # clogger.info(f"表 {table} 还原完成")
                break  # 如果还原成功，跳出循环
            except Exception as ee:
                retry_count += 1
                if retry_count >= max_retry:
                    clogger.error(f"表 {table_file} 还原失败：{ee}")
                else:
                    clogger.warning(f"表 {table_file} 还原失败:{err},正在进行第 {retry_count} 次重试...")
                    time.sleep(5)  # 等待一段时间后再次尝试

    def restore_all_tables(self, restore_dir):
        start_time = time.time()
        clogger.info(f'开始还原数据库{self.database},目录:{restore_dir}')
        # 获取所有备份文件的名称
        backup_files = os.listdir(restore_dir)

        # 使用线程池并发还原所有表格
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = {}
            # 构造还原每个表格的任务
            for i, table_file in enumerate(backup_files):
                future = executor.submit(self.restore_table, restore_dir, table_file)
                futures[future] = i + 1
            # 使用 tqdm 输出还原进度
            for _ in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc='进度',
                          unit='个表格', unit_scale=True, ncols=80):
                pass

        # 输出还原完成信息
        clogger.info("还原完成！")
        end_time = time.time()
        clogger.info(f"还原总共耗时：{end_time - start_time:.2f}秒")


def prompt(choices):
    return Prompt().ask(choices=choices)


def backup(backuper: MysqlBackuper):
    backuper.backup_all_tables()


def restore(backuper):
    sub_dirs = [name for name in os.listdir(backuper.backup_dir)
                if os.path.isdir(os.path.join(backuper.backup_dir, name))]
    dir_path = prompt(choices=sub_dirs)
    dir_path = os.path.join(backuper.backup_dir, dir_path)
    backuper.restore_all_tables(dir_path)


def backup_and_compress(backuper):
    backuper.backup_all_tables()
    time.sleep(1)
    compress_and_delete(backuper.db_backup_dir)


def restore_and_decompress(backuper):
    sub_dirs = [name for name in os.listdir(backuper.backup_dir)
                if os.path.isfile(os.path.join(backuper.backup_dir, name)) and name.endswith('7z')]
    file_name = prompt(choices=sub_dirs)
    file_path = os.path.join(backuper.backup_dir, file_name)
    decompress(file_path)
    time.sleep(1)
    file_name = ''.join(file_name.split('.')[:-1])
    file_path = os.path.join(backuper.backup_dir, file_name)
    backuper.restore_all_tables(file_path)


def compress_and_delete_dir(backuper):
    sub_dirs = [name for name in os.listdir(backuper.backup_dir)
                if os.path.isdir(os.path.join(backuper.backup_dir, name))]
    dir_name = prompt(choices=sub_dirs)
    file_path = os.path.join(backuper.backup_dir, dir_name)
    compress_and_delete(file_path)


def decompress_file(backuper):
    sub_dirs = [name for name in os.listdir(backuper.backup_dir)
                if os.path.isfile(os.path.join(backuper.backup_dir, name)) and name.endswith('7z')]
    file_name = prompt(choices=sub_dirs)
    file_path = os.path.join(backuper.backup_dir, file_name)
    decompress(file_path)


def parse():
    parser = argparse.ArgumentParser(description='Backup and Restore Tool')
    parser.add_argument('--backup', '-bk', action='store_true', help='backup all tables')
    parser.add_argument('--restore', '-rs', action='store_true', help='restore all tables')
    parser.add_argument('--backup_compress', '-bkc', action='store_true', help='backup all tables and compress')
    parser.add_argument('--restore_decompress', '-rsd', action='store_true', help='restore all tables and decompress')
    parser.add_argument('--compress_delete_dir', '-cdd', action='store_true', help='compress and delete a directory')
    parser.add_argument('--decompress', '-dc', action='store_true', help='decompress a file')
    return parser


def main():
    with open('dbbp.yaml', 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    args = parse().parse_args()
    backuper_config = config['backuper']
    backuper = MysqlBackuper(
        hostname=backuper_config['hostname'],
        username=backuper_config['username'],
        password=backuper_config['password'],
        database=backuper_config['database'],
        db_cwd=backuper_config['db_cwd'],
        port=backuper_config['port'],
        backup_dir=backuper_config['backup_dir'],
        ex_opt=backuper_config.get('ex_opt')
    )
    # args.backup = True
    # args.restore_dir = r'D:\NEMBackupDataBase\test\20230328_084354'
    if args.backup:
        backup(backuper)
    elif args.restore:
        restore(backuper)
    elif args.backup_compress:
        backup_and_compress(backuper)
    elif args.restore_decompress:
        restore_and_decompress(backuper)
    elif args.compress_delete_dir:
        compress_and_delete_dir(backuper)
    elif args.decompress:
        decompress_file(backuper)
    else:
        clogger.info('Please specify a valid option')


if __name__ == '__main__':
    multiprocessing.freeze_support()
    #  -p clogger.py -p zip_file.py
    try:
        main()
    except Exception as e:
        raise e
    finally:
        clogger.info('输入任何字符关闭')
        input()
