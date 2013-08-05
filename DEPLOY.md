在成功[安装TFS][1]之后，在你的安装目录（默认为~/tfs_bin)，包含几个子目录bin、conf、include、lib、logs、script等。

*   bin：包含tfs所有的可执行程序文件，如nameserver(NS)、dataserver(DS)、tfstool。
*   conf：包含tfs的配置文件，如NS的配置文件ns.conf，DS的配置文件ds.conf。
*   include：包含TFS客户端库相关的头文件，应用程序使用TFS需要包含这些头文件。
*   lib： 包含TFS客户端的静/动态库，应用程序使用TFS需要连接libtfsclient。
*   logs：用于存储TFS运行过程中的日志。
*   script：包含tfs常用的一些运维脚本，如stfs用于格式化DS， tfs启动/停止NS、DS。

本文介绍如何搭建TFS存储集群，以两台存储节点（4个DS进程）为例，部署拓扑图如下所示：

![enter image description here][2]

配置TFS
-------

~/tfs_bin/conf目录下包含TFS的配置文件模板，在部署前需要根据环境修改配置文件，下面对NS，DS一些关键的配置项进行说明，所有配置项使用key=value的形式进行配置，每行一项，在行首加#可直接注释配置项使之不生效；使用者应该根据实际情况，修改conf/ns.conf、conf/ds.conf这两个配置文件。

    [public] 通用配置
    log_size=1073741824   日志文件大小，当日志大小超过这个配置值时，TFS会进行rotate，产生新的日志文件, 建议不修改
    log_num=64  最多保存的日志文件个数，超出会删除最旧的日志文件，建议不修改
    log_level=info  日志级别（error，warn，info，debug），生产环境建议info级别， 测试环境建议使用debug级别
    task_max_queue_size=10240  server最大的请求队列长度，超出会直接丢掉请求包，可根据实际情况修改
    work_dir=/home/xxxx/tfs  TFS工作目录，强烈建议直接设置为TFS的安装目录，以方便运维管理
    dev_name=bond0  server使用的网卡名，需要修改为你机器的某个网卡名
    ip_addr=10.232.36.201  server服务的ip地址，必须是dev_name网卡上的IP地址，如果NS配置了HA，此处配置vip
    port=8100  server服务的端口号，根据实际环境修改, 该配置项建议所有DS保持相同
    
    [nameserver] 该区域的配置只针对NS有效
    safe_mode_time=360  seconds NS启动后的保护时间，在该时间内，NS不会构建任何复制、迁移等任务
    ip_addr_list=10.232.36.201|192.168.0.254  如果使用HA,则配置vip对应的两个节点的实际ip；如果没有使用HA,第一项配置与ip_addr相同，第二项可以随便配置一个无效的地址(如本例中192.168.0.254是无效的IP地址）
    group_mask=255.255.255.255  用于区分机架的子网掩码，可根据实际网络环境修改。如果你配置为255.255.255.255，那么任意两个ip不同的机器都被认为在不同的机架；如果你配置为255.255.255.0， 那么192.168.0.x与192.168.0.y将被认为在同一个机架，依次类推；TFS数据块的多个副本不能分布在同一个机架内，以提高可靠性。
    max_replication=2  副本数，机器数必须大于副本数，单机环境测试，只能设置为1，否则不能存数据
    cluster_id=1  集群ID，存文件后生成的文件名会包含这个ID，建议不修改
    block_max_size=75497472  Bytes，保持与dataserver下mainblock_size配置一致，建议不修改
    repl_wait_time=180  seconds 当NS检测到block丢失时，等待多长时间开始复制block，可根据实际情况修改
    compact_delete_ratio=10  代表10%，当NS发现block中文件删除比例超出该比例，开始对block进行压缩，回收删除文件的存储空间
    compact_hour_range=1~10  代表1点~10点，NS只会在1-10点间进行block压缩，建议设置为访问低峰时间段
    balance_percent=0.05  代表5%，负载均衡时，当DS存储容量超出平均值5%时，就会被选择为数据迁移源，低于平均值5%，就会被选为数据迁移目标
    
    [dataserver] 该区域的配置只针对NS有效
    ip_addr=10.232.36.203  NS的ip地址，如果配置了HA，则为vip
    ip_addr_list=10.232.36.201|192.168.0.254  与nameserver区域的ip_addr_list配置保持一致
    port=8100  NS服务监听的端口
    heart_interval=2  seconds，DS向NS发送心跳的时间间隔，可根据实际环境修改
    mount_name=/data/disk  DS数据目录，如果机器上有2块盘，则它们应该挂载在/data/disk1、/data/disk2，依次类推
    mount_maxsize=961227000  KB, 磁盘实际使用的空间，应该小于df命令Available一项的输出
    base_filesystem_type=1  1代表ext4、3代表ext3，强烈建议使用ext4文件系统
    avg_file_size=15360 Bytes  集群内平均文件大小，预测值，可根据实际负载调整
    mainblock_size=75497472  Bytes 块的大小，建议不修改
    extblock_size=4194304  Bytes 扩展块，扩展块主要用于更新，建议不修改
    

运行Nameserver
--------------

    # cd ~/tfs_bin
    # ./script/tfs start_ns  或者 ./bin/nameserver -f conf/ns.conf -d
    

如果没有提示错误，则NS就已经在后台开始运行了，可通过ps查看相应进行，或进入logs下，查看nameserver.log，如包含“nameserver running”则说明启动正常

运行Dataserver
--------------

在本例的环境中，10.232.36.203/204上都有两块磁盘/dev/sda、/dev/sdb，磁盘空间均为1TB；TFS采用每个DS进程管理一块磁盘的方式，也就是说，每个机器上都会运行2个DS进程，分别管理/dev/sda、/dev/sdb, 并且这两块盘应该挂载到/data/disk1、/data/disk2两个目录（因为配置文件中指定mount_path为/data/disk），启动DS时指定序号（如1、2）这个DS进程就会加载对应目录里的数据（1对应/data/disk1、2对应/data/disk2，依次类推)开始服务。

对每个机器上多个DS进程使用的端口，TFS也做了统一以方便运维，比如DS配置port为8200，那么序号为1的DS进程会在8200端口监听，序号为2的DS进程会在8202端口监听，依次类推，序号为n的DS在8200 + （n - 1）* 2 号端口上监听。强烈建议每个DS public下的port项都配置相同的值，这样根据端口号就能直接换算出DS的序号（序号一旦确定，通过df命令就能确定具体磁盘），对定位问题很方便。

另外，TFS的DS对存储空间有自己独特的管理方式，将可用的存储空间分成多个block进行管理，使用前DS需要先进行format（使用TFS提供的工具），预分配各个block的存储空间，保证block的存储空间在磁盘上连续（理论上），从而避免动态分配时产生大量的磁盘碎片，降低服务效率。

在两台机器上分别进行如下操作，准备好数据目录

    # mkfs.ext4 /dev/sda  // 请确保你有相应权限
    # mount -t ext4 /dev/sda /data/disk1
    # mkfs.ext4 /dev/sdb
    # mount -t ext4 /dev/sdb /data/disk2
    

在两台机器上，对数据目录进行format

    # cd ~/tfs_bin
    # ./script/stfs format 1  // 结果会在命令行提示
    # ./script/stfs format 2
     以上两条命令也可合并为 ./script/stfs format 1-2 或 ./script/stfs format 1,2
     如果要清理格式化产生的数据，将上述命令中format换成clear即可
    

格式化成功后，会发现/data/disk1，/data/disk2下面产生了一堆以数字命名的文件（1-n），因为TFS的block文件是以数字命名的，从1开始递增编号。

在两台机器上，启动DS服务

    # cd ~/tfs_bin
    # ./script/tfs start_ds 1-2  // 序号的使用与stfs类似，可通过","分隔序号，通过"-"指定范围序列
      或./bin/dataserver -f conf/ds.conf -i 1 -d  (-i 指定序号）
    

通过ps可查看DS是否正常启动， 也可进入logs目录，查看dataserver_**i**.log（i用相应的序号代替），如果包含“dataservice start”，则说明DS启动正常。

开始TFS存储之旅
---------------

此时NS，DS的服务已经成功的启动了，TFS客户端可以向TFS存取文件，[tfstool][3]是tfs的命令行工具，可通过该工具向TFS存文件，从TFS取文件。

    # ./bin/tfstool -s 10.232.36.201:8100 -i "put testfile"
    将本地testfile存入tfs， -s 指定nameserver的ip:port。 如果成功，会打印 put testfile => T1QEBXXDpXXXXXXXXX success. 类似的消息，其中T开头一串字符是TFS为这个文件生成的文件名，一共18个字符；要想从TFS取出该文件时，需要指定该文件名。
    
    # ./bin/tfstoo -s 10.232.36.201:8100 -i "get T1QEBXXDpXXXXXXXXX localfile"
    从TFS读取T1QEBXXDpXXXXXXXXX，存储到本地localfile中；如果成功，会打印fetch T1QEBXXDpXXXXXXXXX => localfile success.类似的消息。
    

部署FAQ
-------

    Q: format TFS数据目录的过程中失败了？
    A: 可能的原因：你对这个目录写权限；目录空间不足，先检查mount_maxsize的配置；目录已经被格式化过，先clear，再format；磁盘换了； 具体错误，根据提示的errno来分析。
    
    Q: 启动服务的过程中，提示ip 'xxxx' is not local ip, local ip: yyyy类似错误？
    A: 确认dev_name对应的网卡是否存在；ip_addr的配置是否是该网卡上的ip地址。
    
    Q: 通过tfstool存数据一致不能成功，错误码-5001？
    A: 检查max_replication的配置，机器数一定要大于副本数。
    
    Q: ns.conf、ds.conf中为什么要配置ip_addr_list，而不是都直接使用vip？
    A: 主备NS为了实现宕机切换后，备NS能够立即服务；主ns会异步将操作日志同步到备NS；同时所有的DS会同时向主备NS发心跳；这样备NS上拥有的信息与主NS基本相同（异步同步会有延时，做不到完全相同），可随时提供服务。基于此原因，nameserver必须知道备NS的real ip，DS也必须知道vip对应的主备NS的real ip。
    
    Q: TFS HA怎么配置？
    A: 这个让google回答你吧，淘宝HA的使用方式和配置可能并不适合你；tfs对HA软件及配置没有特殊要求，只要能实现主NS宕机vip飘移到备NS即可。
    
    Q: 我部署好了TFS服务，有压力测试工具么，我想测试下性能？
    A: TFS源代码下test/batch目录里有test_batch_write、test_batch_read、test_batch_mix三个工具。
       示例：./test_batch_write -d ns-ip:port -c 1000 -r 10240:20480 -t 4
       该命令代表往ns-ip:port对应的集群，开启4个线程，每个线程写1000个10K-20K范围内大小的文件。
    

如果你想部署TFS Webservice服务，[TFS nginx模块][4]已经在github上开源。

 [1]: https://github.com/alibaba/tfs/blob/master/INSTALL.md
 [2]: http://e.hiphotos.baidu.com/album/s=550;q=90;c=xiangce,100,100/sign=36ba122263d0f703e2b295d938c12000/622762d0f703918f1c4579ff503d269759eec46b.jpg?referer=68835931359b033b759fc9ea2429&x=.jpg
 [3]: http://code.taobao.org/p/tfs/wiki/tools/
 [4]: https://github.com/taobao/nginx-tfs
