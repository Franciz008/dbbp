import os
import subprocess

from clogger import clogger
import time


def measure_time(func_name):
    def decorator(func):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            clogger.info(f"{func_name}总共耗时：{end_time - start_time:.2f}秒")
            return result

        return wrapper

    return decorator


default_work_dir = 'D:\\WORK\\PYTHON\\my-python-tools\\多线程备份数据库\\resource\\7z2201-extra\\'


def run_command(cmd, cwd):
    """
    运行命令并返回结果

    :param cwd: 工作目录
    :param cmd: 命令字符串
    :return: 执行命令后返回的输出或错误信息字符串
    """

    process = subprocess.Popen(cmd, cwd=cwd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = process.communicate()
    if err:
        raise err.decode('utf-8')
    clogger.info('run_command:done')
    return out.decode('utf-8')


def run_7zip(cmd, work_dir=default_work_dir):
    """
    运行 7-Zip 命令

    :param cmd: 7-Zip 命令及其参数，以列表形式传入
    :param work_dir: 工作目录，字符串类型
    """
    run_command(cmd, work_dir)


@measure_time('压缩并删除')
def compress_and_delete(src_path, dest_path=None):
    """
    压缩并删除源目录或源文件

    :param src_path: 源目录或源文件路径
    :param dest_path: 目标压缩文件路径
    """
    if not dest_path:
        # 自动生成压缩包文件名
        file_name = os.path.splitext(os.path.basename(src_path))[0]
        dest_path = os.path.join(os.path.dirname(src_path), f'{file_name}.7z')
    clogger.info(f'正在压缩{src_path}为{dest_path},然后删除源文件')
    cmd = ['7za.exe', 'a', '-t7z', dest_path, src_path, '-sdel']
    run_7zip(cmd)


@measure_time('解压')
def decompress(src_path, dest_path=None):
    """
    解压缩到输出目录

    :param src_path: 源压缩文件路径
    :param dest_path: 输出目录路径
    """
    if not dest_path:
        # 自动生成目录
        file_name = os.path.splitext(os.path.basename(src_path))[0]
        dest_path = os.path.join(os.path.dirname(src_path), file_name)
    clogger.info(f'正在解压{src_path}到{dest_path}')
    os.makedirs(dest_path, exist_ok=True)  # 创建目录（如果不存在）
    cmd = ['7za.exe', 'x', src_path, f'-o{dest_path}']
    run_7zip(cmd)


# if __name__ == '__main__':
#     # decompress(r"D:\NEMBackupDataBase\test.7z")
#     # compress_and_delete(r"D:\NEMBackupDataBase\test")
#     import subprocess
#
#     result = subprocess.run(['dir'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
#     print(result.returncode)
#     print(result.stdout.decode('gbk'))
#     print(result.stderr.decode('gbk'))
