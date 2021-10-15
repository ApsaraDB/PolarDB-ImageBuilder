# What is PolarDBManager
PolarDBManager is a tool for PolarDB for PostgreSQL opensource database to build images (including engine image and manager image).

- Engine Image:  Engine image is provided for users to access. Users can operate the database in the pods created by the engine image, such as connecting to the database, inserting data, querying data, etc.

- Manager Image: Manager image is for PolarDB Stack. PolarDB Stack controls the database in the pods created by the manager image, such as upgrade, restart, backup, HA switch, etc.

# Principles

PolarDBManager builds image by `image.py` script. This script will get the contents in `image.yml` file, which contains information such as whether to build engine/manager image or not, push image or not, etc.

The contents in  `image.yml` file are as follows:

```yaml

---
images:
# The following shows the settings of three images: engine image (for test), engine image (for release), and manager image. Please adjust the settings according to your actual needs.

# Engine image (for test)
  - id: polardb_pg_engine_test
    # The identifier of the engine image. The manager image will find the engine image based on this id. You can use the default id, or customize one. Duplicate ids are not allowed.
    type: engine
    # Image type. Two values available: engine and manager. engine is for engine image while manager is for manager image. 
    build_image_name: polardb_pg/polardb_pg_engine_test
    # The name of the built image. You can use the default name, or customize one. 
    build_image_dockerfile: ./docker/Dockerfile.pg.engine.test
    # The Dockerfile which will be used when building image. You need to specify the file name and path.
    engine_repo: https://github.com/ApsaraDB/PolarDB-for-PostgreSQL.git
    # The github address of PolarDB for PostgreSQL open source database.
    engine_branch: main
    # The github branch of PolarDB for PostgreSQL open source database.
    engine_config_template: src/backend/utils/misc/postgresql.conf.sample
    # The path and file name of the parameter template in the PolarDB for PostgreSQL open source database.
    push: false
    # Whether to push the built image to the docker registry automatically. Set it to true if you need to release the image. If you are testing, set it to false.
    enable: true
    # Build the image or not. Set it to true if you want to build and false if you don't.

# Engine image (for release)
  - id: polardb_pg_engine_release
    type: engine
    build_image_name: polardb_pg/polardb_pg_engine_release
    build_image_dockerfile: ./docker/Dockerfile.pg.engine.release
    engine_repo: https://github.com/ApsaraDB/PolarDB-for-PostgreSQL.git
    engine_branch: main
    engine_config_template: src/backend/utils/misc/postgresql.conf.sample
    push: true
    enable: false

# Manager image
  - id: polardb_pg_manager
    # The identifier of the manager image. You can use the default id, or customize one. Duplicate ids are not allowed.
    type: manager
    # Image type. Two values available: engine and manager. 
    build_image_name: polardb_pg/polardb_pg_manager
    # The name of the built image. You can use the default name, or customize one. 
    build_image_dockerfile: ./docker/Dockerfile.manager
    # The Dockerfile which will be used when building image. You need to specify the file name and path.
    engine_image_id: polardb_pg_engine_release
    # The engine image id. PolarDBManager will build the manager image based on the engine image. So you need to specify the engine image id for the manager image.
    push: false
    # Whether to push the built image to the docker registry automatically. Set it to true if you need to release the image. If you are testing, set it to false.
    enable: true
    # Build the image or not. Set it to true if you want to build and false if you don't.
```

The  `image.yml` file contains the settings of three images: 

1. Engine image for test. Default ID: polardb_pg_engine_test.

2. Engine image for release. Default ID: polardb_pg_engine_release.

3. Manager image. Default ID: polardb_pg_manager.

Manager image depends on the engine image (for test one or for release one). So PolarDBManager will first build the the engine image, and then manager image.   

The process and principles are as follows:

1. First, PolarDBManager will pull the code from the repository of PolarDB PostgreSQL engine, according to the **engine_repo** and **engine_branch** settings in `image.yml`.

2. Then, PolarDBManager will copy the parameter template in the repository of PolarDB for PostgreSQL engine to your PolarDBManager repository (rootfs/) , which will be used as *postgresql.conf*, according to **engine_config_template** in `image.yml`.  

3. Run `docker build` command to start building image. PolarDBManager will first build engine images (test or release) and then manager image.

4. In the `image.yml`, if you set **push** of the images to *true*, PolarDBManager will push the built image to the image repository.

   

# Build Image Locally
## Install Depedency
Run the following commands to install python:

```bash
sudo pip install -r requirements.txt
```

## Build Image

Perform the following steps to build a new image.

1. Run the following commands to download the source code of PolarDBManager: 

   ```
   git clone https://github.com/ApsaraDB/PolarDBManager.git && cd PolarDBManager
   ```

2. Edit the settings in `image.yml` according to your actual needs.

   **engine_branch**: Enter the branch name of PolarDB for PostgreSQL of which you want to build engine image based the source code.

   > Before pushing the image to the docker repository, please make sure you have the read-write access to the registry, and then run the following command to log into the repository.
   > ``` 
   docker login -u ${USER} -p

3. Run the following commands to start building.

   ```
   ./image.py -f image.yml
   ```

   The result will be added to the `image.result` file.



# How to Use Image

## Engine Image

The engine image can initialize and start the process of PolarDB for PostgreSQL. Cooperated with PolarDB Stack, the supervisor daemon process can start or stop the instance.

## Manager Image

Manager image provides functions for PolarDB Stack to control and manage the database, such as upgrade, restart, backup, HA switch, etc. 

The following contents show the supported functions and the principles.

* Creating user on RW node by running SQL statements.

* Creating data replication relationship on RW node: between RW and RO (synchronous replication), and between RW and standby (asynchronous replication).

  1. Editing *postgresql.conf*. 

  2. Creating a slot. Naming rule:

     - RO: replica_${custins_id}_${slot_unique_name}

     - Standby: standby_${custins_id}_${slot_unique_name}

* Locking the instance when the HDD is full (on RW, RO, Standby): When the HDD is full, lock the instance. Once locked, the normal user can read but cannot write, and super user can both read and write.

  1. Editing *postgresql.conf*: Set **polar_force_trans_ro_non_sup=on**.

  2. Running the following command:

     ```
     pg_ctl -D /data reload

* Unlocking the instance when the HDD is full (on RW, RO, Standby)

   1. Editing *postgresql.conf*: Set **polar_force_trans_ro_non_sup=off**.

   2. Running the following command:

      ```
      pg_ctl -D /data reload
      ```

* Locking the instance when the instance is expired (RW, RO, Standby): Forbid all the reading and writing when the instance is expired.

  1. Creating lock file: */data/ins_lock*.
    
    2. Running the following command:
    
       ```
       pg_ctl -D /data stop
       ```
    
       This command stops the pod of manager engine, not the engine pod. 
  
* Unlocking the instance when the instance is expired (RW, RO, Standby): Deleting the lock file */data/ins_lock*, and supervisor will start the instance automatically.

* Disk expansion (RW, RO, Standby) by running the following SQL statements:

  ```
  select polar_vfs_disk_expansion()

* Refreshing parameters (RW, RO, Standby)

  1. Editing *postgresql.conf*.

  2. Running the following command:

     ```
     pg_ctl -D /data reload
     ```

* Starting/stopping/restarting instance (RW, RO, Standby) by running the following commands:

  
        pg_ctl -D /data start
        pg_ctl -D /data stop
        pg_ctl -D /data restart
    
* Health Check (RW, RO, Standby) , connecting to the database and running the following statements:

    ```
    select 1  
    ```

    

# Maintenance

* Check control and management logs:

  The control and management logs are recorded in the `/log/manager.log` files, which are generated according to the file size. The maximum size of each file is 10 MB, and 15 files will be saved at most. 


* Check engine logs:

  The engine logs are recorded in the directory `/data/pg_log`. `/log/pg_log` is a soft link of  `/data/pg_log`  in `/log`, for collecting logs by external services. 


* Connect to database:

  Run `pg` command to connect to the database.



