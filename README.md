![logo](https://raw.github.com/alibaba/oceanbase/oceanbase_0.3/doc/%E5%9B%BE%E7%89%87%E5%A4%B9/logo.jpg)
<font size=5></font>

OceanBase客户端主要用于开发人员编程时连接OceanBase数据库。

Oceanbase内置了对SQL的支持，用户可以通过libmysql，JDBC等方式直接访问Oceanbase，但由于OceanBase是一个分布式数据库，可以由多个节点（MergeServer）同时提供SQL服务。而MySQL客户端等都是针对单机系统，在连接OceanBase时，客户端会绑定其中一台MergeServer进行SQL操作，而不能有效利用其他MergeServer资源。

为了实现了多集群间流量分配和多MergeServer间的负载均衡，并给应用开发人员提供一个简单接入方案，我们在libmysql，JDBC的基础上封装一个OceanBase客户端。

<font size=7><div align="right"><b><a href="https://github.com/alibaba/oceanbase/wiki" target="_blank">返回Home</a></b></div></font>
