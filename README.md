Welcome to TFS! 
---------------
TFS is a distributed file system developed by Taobao.com.

Overview
--------
Aiming at a distributed file system with high availability, high performance and low cost, TFS is a linux-based file system which provides high reliability and concurrent access by redundancy, backup and load balance technology.
TFS is mainly designed for small files less than 1MB in size. It adopts flat structure instead of the traditional directory structure. TFS will generate a 18 byte length filename after storing data uploaded by users. Users can access their data by the uniqle filename.

Documents
---------
### [Install](https://github.com/alibaba/tfs/blob/master/INSTALL.md)
### [Deploy](https://github.com/alibaba/tfs/blob/master/DEPLOY.md)

Licence
-------
GNU General Public License v2

Contribution
------------
This project is still in development stage, many bugs are not fixed or found yet. feel free to submit bugs if you find them. If you have any question or feedback, please contact and join us, we appreciate your kindly contribution.
