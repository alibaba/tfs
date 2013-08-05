安装依赖的软件包
----------------

*   automake TFS基于automake工具构建 
    *   [下载源码安装][1] 
    *   apt-get install automake
    *   yum install automake.noarch
*   libtool automake需要使用libtool 
    *   [下载源码安装][2]
    *   apt-get install libtool
    *   yum install libtool.x86_64
*   realine 用于命令行编辑的库 
    *   [下载源码安装][3]
    *   apt-get install libreadline-dev
    *   yum install readline-devel.x86_64
*   libz-devel 用于数据压缩/解压缩 
    *   [下载源码安装][4]
    *   apt-get install zlib1g-dev
    *   yum install zlib-devel.x86_64
*   uuid-devel 用于生成全局唯一ID 
    *   [下载源码安装][5]
    *   apt-get install uuid-dev
    *   yum install e2fsprogs-devel.x86_64
*   tcmalloc google的内存管理库 
    *   [下载源码安装][6]
    *   apt-get install libgoogle-perftools-dev
    *   yum install google-perftools.x86_64

安装tb-common-utils
-------------------

TFS使用tb-common-utils软件包，tb-common-utils包含淘宝使用的基础系统库tbsys和网络库tbnet两个组件；安装tb-common-utils前需要设置环境变量**TBLIB_ROOT**，tbsys和tbnet将会被安装TBLIB_ROOT对应的路径（必须是绝对路径）下，TFS会在这个路径下查找tbsys、tbnet头文件和库。

设置TBLIB_ROOT环境变量

    在~/.bash_profile文件中加入，export TBLIB_ROOT=path_to_tbutil , 然后执行source ~/.bash_profile
    

下载源码

    # svn co -r 18 http://code.taobao.org/svn/tb-common-utils/trunk tb-common-utils
    注意： 这里不要checkout最新版本，version18以后的修改导致部分接口不能前向兼容。
    

编译安装

    # cd tb-common-utils
    # sh build.sh
    

如果一切顺利，tb-common-utils已经安装成功到$TBLIB_ROOT路径下；如遇到问题请先阅读后面的**编译FAQ**。

安装TFS
-------

TFS开源用户大都只使用TFS的基本功能，所以这个版本我们默认只编译TFS的nameserver，dataserver，client和tool，以去除对mysql的依赖，需要使用到rcserver（全局资源管理服务），metaserver(TFS自定义文件名服务）的用户请自行编译安装这两个服务。

下载源码

    # svn co http://code.taobao.org/svn/tfs/branches/dev_for_outer_users tfs
    

编译安装

    # cd tfs
    # sh build.sh init
    # ./configure --prefix=path_to_tfs --with-release
    # make
    # make install
    

*   --prefix 指定tfs安装路径，默认会被安装到~/tfs_bin目录
*   --with-release 指定按release版本的参数进行编译，如果不指定这个参数，则会按开发版本比较严格的参数编译，包含-Werror参数，所有的警告都会被当错误，在高版本gcc下会导致项目编译不过，很多开源用户反馈的编译问题都跟这个有关，因为gcc高版本对代码的检查越来越严格，淘宝内部使用的gcc版本是gcc-4.1.2。

至此，TFS已经安装成功了，你可以开始[部署TFS服务][7]。

编译FAQ
-------

    Q: 使用TFS一定需要64bit Liunx？
    A: 是的，否则整个项目不能正常编译通过。
    
    Q: 编译TFS过程中出现出现类似tbnet.h:39: fatal error: tbsys.h: No such file or directory的错误提示？
    A: 需要先安装tb-common-utils软件包。
    
    Q: 在安装tb-common-utils过程中，提示设置please set TBLIB_ROOT varialbe first!!？
    A: 需要先设置TBLIB_ROOT环境变量，再编译安装tb-common-utils。
    
    Q: 编译过程中出现类似警告：格式 ‘%lu’ 需要类型 ‘long unsigned int’，但实参 3 的类型为 ‘size_t’ ？
    A: 你的机器使用的应该是32bit OS，如果你坚持要编译，可以自行修改代码或者直接忽略这些警告。
    
    Q: 执行./configure时，提示configure: error: readline header files not found, --disable-readline or install gnu readline library?
    A: 你需要安装readline库，或在configure时加上--disable-readline参数，不使用readline库。
    
    Q: 执行./configure时，提示configure: error: tcmalloc link failed (--without-tcmalloc to disable)？
    A: 你需要安装tcmalloc库, 或在configure时加上--without-tcmalloc参数，不使用tcmalloc库。
    
    Q: 编译过程中出现大量的错误信息，类似于warnings being treated as error？
    A: 请确认在./configure的时候是否加了--with-release参数。
    
    Q: 编译过程中提示类似client_request_server.cpp:722:38: error: no matching function for call to ‘atomic_inc(volatile uint64_t*)’的错误信息？
    A: 你的系统时32位的，请在64bit Linux编译安装TFS；因为32bit系统不支持对64bit整数的原子操作。
    
    Q: 编译时遇到session_util.cpp:2:23: fatal error: uuid/uuid.h: No such file or directory？
    A: 你需要先安装uuid库, 用于生成全局唯一ID, uuid库是e2fsprogs包工具的一部分；
    
    Q: 编译时提示tfs_meta_helper.cpp:15:18: fatal error: zlib.h: No such file or directory？
    A: 你需要先安装zlib,用于压缩/解压缩。
    

 [1]: http://www.gnu.org/software/automake/#downloading
 [2]: http://www.gnu.org/software/libtool/
 [3]: http://cnswww.cns.cwru.edu/php/chet/readline/rltop.html#Availability
 [4]: http://zlib.net/
 [5]: http://sourceforge.net/projects/e2fsprogs/
 [6]: http://code.google.com/p/gperftools/downloads/list
 [7]: https://github.com/alibaba/tfs/blob/master/DEPLOY.md
