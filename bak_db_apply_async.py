import argparse
import multiprocessing
import os
import subprocess
import time
from queue import Empty

import mysql.connector
import yaml
from rich.prompt import Prompt
from tqdm import tqdm

from clogger import clogger
from zip_file import compress_and_delete, decompress


# from queue import Queue


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

    def backup_table(self, table_name, result_queue):
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
            # clogger.info(f"正在备份表 {table_name}...")
            # 构造备份命令
            #  --routines 用于在备份时同时备份存储过程和函数等程序性对象。
            cmd = f"{self.mysql_exe} -u {self.username} -p{self.password} " \
                  f"-h {self.hostname} -P {self.port} {self.ex_opt} {self.database} {table_name}"
            backup_file = os.path.join(self.db_backup_dir, table_name + '.sql')  # 备份文件名为表名加上后缀 .sql
            cmd = f'{cmd} > {backup_file}'
            result = subprocess.run(cmd, shell=True, cwd=self.db_cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            assert result.stdout.decode('gbk') == ''
            err = result.stderr.decode('gbk')
            assert err == ''
            # 输出备份完成信息
            # clogger.info(f"表 {table_name} bak done")

            # 将备份结果写入共享队列
            result_queue.put((table_name, True, backup_file))
        except Exception as es:
            # 抛出备份异常信息
            clogger.error(f"表 {table_name} 备份失败：{es} {err}")
            result_queue.put((table_name, False, str(e)))
            # raise es
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

        with multiprocessing.Manager() as manager:
            # 创建共享队列
            table_queue = manager.Queue(len(tables))
            # 将任务放入队列
            for table_name in tables:
                table_queue.put(table_name)

            # 创建一个共享队列，用于存储备份结果
            result_queue = multiprocessing.Manager().Queue()

            # 创建进程池，并启动若干个子进程
            pool = multiprocessing.Pool(processes=self.max_workers)
            backup_threads = []
            while not table_queue.empty():
                try:
                    # 从任务队列中取出一个表名
                    table_name = table_queue.get(timeout=1)
                    # 立即启动一个子进程进行备份操作
                    backup_thread = pool.apply_async(self.backup_table, args=(table_name, result_queue))
                    backup_threads.append(backup_thread)
                except Empty:
                    break

            # 等待所有子进程完成备份操作
            for backup_thread in tqdm(backup_threads, desc="备份进度", ncols=80):
                backup_thread.get()

            # 处理备份结果，记录日志
            success_tables = []
            failed_tables = []
            while not result_queue.empty():
                table_name, success, info = result_queue.get()
                if success:
                    success_tables.append(table_name)
                    # clogger.info(f"表 {table_name} 备份成功，备份文件为 {info}")
                else:
                    failed_tables.append(table_name)
                    clogger.error(f"表 {table_name} 备份失败，失败原因：{info}")

            if failed_tables:
                clogger.warning(f"{len(failed_tables)} 张表备份失败：{', '.join(failed_tables)}")
            else:
                clogger.info("所有表备份成功！")

        # 输出备份完成信息
        clogger.info("备份完成！")
        end_time = time.time()
        clogger.info(f"备份总共耗时：{end_time - start_time:.2f}秒")

    def restore_table(self, restore_dir, table_name, result_queue):
        # 将备份文件恢复到数据表
        try:
            backup_path = os.path.join(restore_dir, f"{table_name}.sql")  # 备份文件名为表名加上后缀 .sql
            if not os.path.exists(backup_path):
                raise Exception(f"数据表 {table_name} 的备份文件不存在！")

            # 使用 mysql 命令行工具恢复数据表
            cmd = f"mysql -u {self.username} -p{self.password} " \
                  f"-h {self.hostname} -P {self.port} {self.database}"
            cmd = f'{cmd} < {backup_path}'
            restore_cmd = f"{cmd} < {backup_path}"
            recode = subprocess.call(restore_cmd, shell=True, cwd=self.db_cwd)
            if recode == 0:
                result_queue.put((table_name, True))
                # print(f"数据表 {table_name} 还原成功！")
            else:
                error_msg = f"恢复数据表 {table_name} 失败，返回码为 {recode}"
                raise subprocess.CalledProcessError(recode, restore_cmd, error_msg)
        except Exception as er:
            result_queue.put((table_name, False, str(er)))

    def restore_all_tables(self, restore_dir):
        start_time = time.time()
        # 获取备份文件名列表
        backup_files = [f for f in os.listdir(restore_dir) if
                        os.path.isfile(os.path.join(restore_dir, f)) and f.endswith('.sql')]
        if not backup_files:
            print(f"备份目录 '{restore_dir}' 中未找到任何备份文件！")
            return

        # 初始化一个共享队列和任务队列
        result_queue = multiprocessing.Manager().Queue()
        task_queue = multiprocessing.Manager().Queue()
        for backup_file in backup_files:
            table_name = os.path.splitext(backup_file)[0]
            task_queue.put(table_name)

        # 创建进程池，启动若干个子进程进行还原操作
        pool = multiprocessing.Pool(processes=self.max_workers)
        restore_threads = []
        while not task_queue.empty():
            try:
                table_name = task_queue.get(timeout=1)
                restore_thread = pool.apply_async(self.restore_table, args=(restore_dir, table_name, result_queue))
                restore_threads.append(restore_thread)
            except Exception as ef:
                print(ef)

        # 等待所有子进程完成工作
        for restore_thread in tqdm(restore_threads, desc="还原进度", ncols=80):
            restore_thread.get()

        # 处理还原结果
        success_tables = []
        failed_tables = []
        while not result_queue.empty():
            table_name, success, message = result_queue.get()
            if success:
                success_tables.append(table_name)
            else:
                failed_tables.append(table_name)
                clogger.info(message)

        if failed_tables:
            print(f"还原失败的数据表：{', '.join(failed_tables)}")
        else:
            print("所有数据表都已成功恢复！")
        end_time = time.time()
        clogger.info(f"备份总共耗时：{end_time - start_time:.2f}秒")


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
        max_workers=backuper_config['max_workers'],
        backup_dir=backuper_config['backup_dir'],
        ex_opt=backuper_config.get('ex_opt')
    )
    # args.restore = True
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
    # 将 '2023-04-15' 转换为 datetime.datetime 对象
    try:
        main()
    except Exception as e:
        raise e
    finally:
        clogger.info('输入任何字符关闭')
        input()
