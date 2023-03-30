[中文文档](https://github.com/Franciz008/dbbp/blob/main/%E4%B8%AD%E6%96%87ReadMe.md)

# Backup and Restore Tool --dbbp

This is a Python 3-based backup and restore tool for MySQL databases.

## Configuration

This backup and restore tool is implemented based on Python 3 and can be used to backup and restore MySQL databases. The
tool uses asynchronous multiprocess technology to assign each table of the database to different processes in the backup
and restore work, thus greatly improving efficiency.

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

Here, `backuper` is the configuration for the backuper. It contains the following fields:

* `hostname`: The hostname or IP address of the MySQL server.
* `port`: The port number of the MySQL server.
* `username`: The username used to connect to the MySQL database.
* `password`: The password used to connect to the MySQL database.
* `database`: The name of the database to be backed up or restored.
* `db_cwd`: The data directory for the MySQL database.
* `max_workers`: The maximum number of worker threads to run for backup or restore tasks.
* `backup_dir`: The directory to store backup files.
* `ex_opt`: The options to exclude when performing backup or restore tasks, such as "events" and "routines".

## Command-line arguments

The tool supports the following command-line arguments:

* `--backup` or `-bk`: Backup all data tables.
* `--restore` or `-rs`: Restore all data tables.
* `--backup_compress` or `-bkc`: Backup all data tables and compress backup files.
* `--restore_decompress` or `-rsd`: Restore all data tables and decompress backup files.
* `--compress_delete_dir` or `-cdd`: Compress and delete a directory.
* `--decompress` or `-dc`: Decompress a file.

## Examples

Here are some examples:

### Backup all data tables

```sh
python bak_db_apply_async.py --backup
```

It can also be:

```shell
dbbp.exe --backup
```

The same applies to all of the examples below.

### Restore all data tables

```sh
python bak_db_apply_async.py --restore
```

### Backup all data tables and compress backup files

```sh
python bak_db_apply_async.py --backup_compress
```

### Restore all data tables and decompress backup files

```sh
python bak_db_apply_async.py --restore_decompress
```

### Compress and delete a directory

```sh
python bak_db_apply_async.py --compress_delete_dir
```

### Decompress a file

```sh
python bak_db_apply_async.py --decompress
```

## License

This backup and restore tool is released under the MIT License.
