# 什么是 PolarDBManager

PolarDBManager 是一种打包工具，可构建 PolarDB for PostgreSQL 开源数据库的镜像，包括引擎镜像和管理镜像。

- 引擎镜像：提供给用户实际访问的镜像，用户一般在此镜像建立出的容器中对数据库进行各种操作，如连接数据库，进行数据插入、查询等。
- 管理镜像：提供给 PolarDB Stack 使用，一般在此镜像建立出的容器中对数据库进行各种管控操作，如升级、重启、备份、HA 等。

# 原理

PolarDBManager 通过 `image.py` 脚本打镜像，该脚本读取 `image.yml` 配置文件，决定要打哪些镜像，是否要 push 等。

`image.yml`配置文件的格式如下：

```yaml
---
images:
  # 共有3个镜像相关的配置，分别为引擎镜像（本地测试用）、引擎镜像（发布用）和管理镜像。请根据打镜像的用途，调整相关参数。

  # 引擎镜像（本地测试用）
  - id: polardb_pg_engine_test
    # id为该镜像的标识。管理镜像根据此id引用引擎镜像。可使用默认值，也可自定义。镜像id不允许重复。
    type: engine
    # 镜像类型，当前取值支持engine和manager，分别对应引擎镜像和管理镜像。引擎镜像是管理镜像的基础镜像。
    build_image_name: polardb_pg/polardb_pg_engine_test
    # build目标镜像全名。可使用默认值，也可自定义。
    build_image_dockerfile: ./docker/Dockerfile.pg.engine.test
    # build目标镜像使用的Dockerfile，包括其文件名以及在本项目的具体路径。
    engine_repo: https://github.com/ApsaraDB/PolarDB-for-PostgreSQL.git
    # PolarDB for PostgreSQL开源数据库的github地址。
    engine_branch: main
    # PolarDB for PostgreSQL开源数据库的github分支。
    engine_config_template: src/backend/utils/misc/postgresql.conf.sample
    # PolarDB for PostgreSQL开源数据库目录中参数模板的位置。
    push: false
    # 打完镜像是否自动push到镜像仓库。如果是发布镜像的话，需要push，本地测试则不需要push。
    enable: true
    # 是否build该镜像

  # 引擎镜像（发布用），参数说明同上
  - id: polardb_pg_engine_release
    type: engine
    build_image_name: polardb_pg/polardb_pg_engine_release
    build_image_dockerfile: ./docker/Dockerfile.pg.engine.release
    engine_repo: https://github.com/ApsaraDB/PolarDB-for-PostgreSQL.git
    engine_branch: main
    engine_config_template: src/backend/utils/misc/postgresql.conf.sample
    push: true
    enable: false

  # 管理镜像
  - id: polardb_pg_manager
    # id为该镜像的标识。管理镜像根据此id引用引擎镜像。可使用默认值，也可自定义。镜像id不允许重复。
    type: manager
    # 镜像类型，当前取值支持engine和manager，分别对应引擎镜像和管理镜像。
    build_image_name: polardb_pg/polardb_pg_manager
    # build目标镜像全名。可使用默认值，也可自定义。
    build_image_dockerfile: ./docker/Dockerfile.manager
    # build目标镜像使用的Dockerfile，包括其文件名以及在本项目的具体路径。
    engine_image_id: polardb_pg_engine_release
    # 该manager镜像需要对应的引擎镜像id。引擎镜像是管理镜像的基础镜像。
    push: false
    # 打完镜像是否自动push到镜像仓库，如果是发布镜像的话，需要push，本地测试则不需要push。
    enable: true
    # 是否build该镜像
```

当前`image.yml`有三个镜像的配置：

1. 引擎镜像（本地测试用），默认 id 为 polardb_pg_engine_test。
2. 引擎镜像（本地测试用），默认 id 为 polardb_pg_engine_release。
3. 管理镜像，默认 id 为 polardb_pg_manager。

`image.py`打镜像的流程和原理如下，因为管理镜像依赖引擎镜像，所以会先打所有引擎镜像，再打管理镜像。

1. 根据`image.yml`中**engine_repo** 和 **engine_branch** 的配置，将 PolarDB for PostgreSQL 内核对应分支的代码 pull 下来。
2. 根据`image.yml`中**engine_config_template**的配置，拷贝 PolarDB for PostgreSQL 内核中的参数模板到本地（rootfs 目录下），作为 postgresql.conf。
3. 执行`docker build`命令开始打镜像，先打 test 或 release 引擎镜像，然后再打管理镜像。
4. 根据`image.yml`中**push**的配置，若为 enable，则将打好的镜像 push 到镜像仓库。

# 本地构建镜像

## 安装依赖

Git 2.1.4 以上版本
Docker 17.0 以上版本

## 构建镜像

如果要构建新的镜像，请按照以下步骤:

1. 下载 PolarDBManager 的代码。

   ```
   git clone https://github.com/ApsaraDB/PolarDBManager.git && cd PolarDBManager
   ```

2. 根据实际镜像构建的需求，修改`image.yml`中的配置，主要为：

   **engine_branch**：使用 PolarDB for PostgreSQL 开源数据库的哪条 github 分支上代码来构建引擎镜像。

   > 如果需要 push 镜像，请先添加镜像仓库的读写权限，然后执行以下命令登录镜像仓库。
   >
   > ```
   > docker login -u ${USER} -p
   > ```

3. 执行如下命令，开始构建镜像。

   ```
   ./image.py -f image.yml
   ```

   构建的结果会放在`image.result`文件中。

# 镜像功能

## 引擎镜像

引擎镜像功能比较简单，主要为初始化并拉起 PolarDB PostgreSQL 进程。内部有一个 supervisor 守护进程，配合管控 PolarDB Stack 进行实例启停操作。

## 管理镜像

管理镜像提供管控功能给 PolarDB Stack 使用，PolarDB Stack 在管理镜像建立出的容器中对数据库进行各种管控操作。

以下为功能原理。

- 创建用户（RW）：通过执行 SQL 创建用户。
- 创建复制关系（RW）：与 RO/Standby 搭建复制关系，其中 RO 为同步复制，Standby 为异步复制。

  1. 修改 postgresql.conf。
  2. 创建 slot。命名方式如下：

     - RO：replica*${custins_id}*${slot_unique_name}
     - Standby：standby*${custins_id}*${slot_unique_name}

- 磁盘满锁定实例（RW, RO, Standby）：磁盘满后，需要将实例锁定为普通用户只能读，不能写，但超级用户可以读写。

  1. 修改 postgresql.conf，设置**polar_force_trans_ro_non_sup=on**。
  2. 执行：

     ```
     pg_ctl -D /data reload
     ```

- 磁盘满解锁实例（RW, RO, Standby）

  1. 修改 postgresql.conf，设置**polar_force_trans_ro_non_sup=off**。
  2. 执行：

     ```
     pg_ctl -D /data reload
     ```

- 过期锁定实例（RW, RO, Standby）：实例过期后，需要完全禁止读写。

  1. 创建锁文件*/data/ins_lock*。
  2. 执行：

     ```
     pg_ctl -D /data stop
     ```

     停止的是管理镜像的容器，引擎镜像容器不停止。

- 过期解锁实例（RW, RO, Standby）：删除锁文件/data/ins_lock，然后 supervisor 会自动拉起实例。
- 磁盘扩容（RW, RO, Standby），通过执行如下 SQL 实现：

  ```
  select polar_vfs_disk_expansion()
  ```

- 参数刷新（RW, RO, Standby）：

  1.  修改 postgresql.conf 文件。
  2.  执行：
      ```
      pg_ctl -D /data reload
      ```

- 启动/停止/重启实例（RW, RO, Standby），通过执行如下命令实现：

  ```
  pg_ctl -D /data start
  pg_ctl -D /data stop
  pg_ctl -D /data restart
  ```

- 健康检查（RW, RO, Standby），通过连接到数据库，然后执行如下命令实现：

  ```
  select 1
  ```

### 线上运维

- 查看管控日志：管控日志记录在`/log/manager.log`文件中，日志按照大小自动轮转，单文件最大 10MB，最多保存 15 个文件。
- 查看引擎日志：引擎日志记录在`/data/pg_log`目录下，`/log/pg_log`是该目录的一个软连接，放到`/log`目录下，用于外部日志采集。
- 连接数据库：执行`pg`命令连接。
