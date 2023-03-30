# 备份和还原工具--dbbp

该备份和还原工具基于 Python 3 实现，可用于备份和还原 MySQL 数据库。工具使用了异步多进程技术将数据库的每个表在备份和还原工作分配到各个进程从而大幅度提高效率。

## 配置文件说明

备份和还原工具需要一个 YAML 格式的配置文件。示例配置文件如下：

```yaml
backuper:
  hostname: localhost
  port: 3306
  username: root
  password: password
  database: mydb
  db_cwd: /var/lib/mysql
  max_workers: 4
  backup_dir: /var/backups/mysql
  ex_opt:
    - --add-drop-table
    - --single-transaction
```

其中，`backuper` 表示备份器的配置。主要包含以下字段：

* `hostname`: MySQL 服务器的主机名或 IP 地址。
* `port`: MySQL 服务器的端口号。
* `username`: 连接 MySQL 数据库所使用的用户名。
* `password`: 连接 MySQL 数据库所使用的密码。
* `database`: 待备份或待还原的数据库名称。
* `db_cwd`: MySQL 数据库的数据目录。
* `max_workers`: 运行备份或还原任务的最大工作进程数。
* `backup_dir`: 备份文件存放的目录。
* `ex_opt`: 执行备份或还原任务时需要增加额外选项

## 参数说明

本工具支持以下命令行参数：

* `--backup` 或 `-bk`: 备份所有数据表。
* `--restore` 或 `-rs`: 还原所有数据表。
* `--backup_compress` 或 `-bkc`: 备份所有数据表并压缩备份文件。
* `--restore_decompress` 或 `-rsd`: 还原所有数据表并解压备份文件。
* `--compress_delete_dir` 或 `-cdd`: 压缩并删除一个目录。
* `--decompress` 或 `-dc`: 解压一个文件。

## 使用示例

以下是一些使用示例：

### 备份所有数据表

```sh
python bak_db_apply_async.py --backup
```

也可以是,

```shell
dbbp.exe --backup
```

以下所有的示例同理.

### 还原所有数据表

```sh
python bak_db_apply_async.py --restore
```

### 备份所有数据表并压缩备份文件

```sh
python bak_db_apply_async.py --backup_compress
```

### 还原所有数据表并解压备份文件

```sh
python bak_db_apply_async.py --restore_decompress
```

### 压缩并删除一个目录

```sh
python bak_db_apply_async.py --compress_delete_dir
```

### 解压一个文件

```sh
python bak_db_apply_async.py --decompress
```

## 打包命令

```sh
pyinstaller --key 4008820 -n dbbp -F bak_db_apply_async.py --add-data "D:/WORK/PYTHON/my-python-tools/多线程备份数据库/resource;resource" -p clogger.py -p zip_file.py --distpath=E:\WORK\测试工具\多线程备份数据库
```

## 许可证

该备份和还原工具是基于 MIT 许可证发布的开源软件。

---