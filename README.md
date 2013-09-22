![logo](https://raw.github.com/alibaba/oceanbase/oceanbase_0.3/doc/%E5%9B%BE%E7%89%87%E5%A4%B9/logo.jpg)
<font size=5></font>

OceanBase是[阿里巴巴集团](http://page.china.alibaba.com/shtml/about/ali_group1.shtml)自主研发的可扩展的关系型数据库，OceanBase实现了跨行跨表的事务，支持数千亿条记录、数百TB数据上的SQL操作，截止到2012年8月为止，OceanBase数据库支持了阿里巴巴集团下多个重要业务的数据存储，支持业务包括收藏夹、直通车报表、天猫评价等，截止2013年4月份，OceanBase线上业务的数据量已经超过一千亿条。

OceanBase客户端主要用于开发人员编程时连接OceanBase数据库。

Oceanbase内置了对SQL的支持，用户可以通过libmysql，JDBC等方式直接访问Oceanbase，但由于OceanBase是一个分布式数据库，可以由多个节点（MergeServer）同时提供SQL服务。而MySQL客户端等都是针对单机系统，在连接OceanBase时，客户端会绑定其中一台MergeServer进行SQL操作，而不能有效利用其他MergeServer资源。

为了实现了多集群间流量分配和多MergeServer间的负载均衡，并给应用开发人员提供一个简单接入方案，我们在libmysql，JDBC的基础上封装一个OceanBase客户端。


<h1>1 发行日志</h1>
- <font color=“#0000E3”><b>2013/07/29，整理提交了C/JAVA客户端的RPM安装包。</b> </font>
- <font color=“#0000E3”><b>2013/09/13，整理提交了JAVA客户端V1.1.1代码和相关技术文档。</b> </font>


<h1>2 资源地址</h1>
<ul>
<li>OceanBase 源码：https://github.com/alibaba/oceanbase</li>
<li>OceanBase RPM包：https://github.com/alibaba/oceanbase_rpm_package</li>
<li>OceanBase 客户端：https://github.com/alibaba/oceanbase_client</li>
</ul>

<h1>3 产品文档导读</h1>
<table width="100%"  border="1" frame="all" rules="all">
  <tr>
    <td width=9% bgcolor="B0B0B0"><b>序号</b></div></td>
    <td width=29% bgcolor="B0B0B0"><b>文档名称</b></td>
    <td colspan="2" bgcolor="B0B0B0"><b>使用说明</b></td>
  </tr>
  <tr>
    <td width="9%"><div align="center">1</div></td>
    <td width="29%"><a href="https://github.com/alibaba/oceanbase/wiki/OceanBase-%E6%8F%8F%E8%BF%B0" target="_blank">《OceanBase 描述》</a></td>
    <td width="55%">该文档主要介绍OceanBase数据库的架构、存储引擎和功能等信息。</td>
    <td width="7%"><a href="https://raw.github.com/alibaba/oceanbase/oceanbase_0.4/doc/wiki/OceanBase 描述.pdf">下载</a></td>
  </tr>
  <tr>
    <td width="9%"><div align="center">2</div></td>
    <td width="29%"> <a href="https://github.com/alibaba/oceanbase/wiki/OceanBase-%E5%AE%89%E8%A3%85%E6%8C%87%E5%8D%97" target="_blank"> 《OceanBase 安装指南》</a></td>
    <td width="55%">该文档主要介绍OceanBase数据库的安装过程。</td>
    <td width="7%"><a href="https://raw.github.com/alibaba/oceanbase/oceanbase_0.4/doc/wiki/OceanBase 描述.pdf"></a><a href="https://raw.github.com/alibaba/oceanbase/oceanbase_0.4/doc/wiki/OceanBase 安装指南.pdf">下载</a></td>
  </tr>
  <tr>
    <td width="9%"><div align="center">3</div></td>
    <td width="29%"><a href="https://github.com/alibaba/oceanbase/wiki/OceanBase-%E5%BF%AB%E9%80%9F%E5%85%A5%E9%97%A8" target="_blank">《OceanBase 快速入门》</a></td>
    <td width="55%">该文档主要介绍如何快速入门OceanBase的方法。</td>
    <td width="7%"><a href="https://raw.github.com/alibaba/oceanbase/oceanbase_0.4/doc/wiki/OceanBase%20%E5%BF%AB%E9%80%9F%E5%85%A5%E9%97%A8.pdf">下载</a></td>
  </tr>
  <tr>
    <td width="9%"><div align="center">4</div></td>
    <td width="29%"><a href="https://github.com/alibaba/oceanbase/wiki/OceanBase-%E5%AE%A2%E6%88%B7%E7%AB%AF-%E7%94%A8%E6%88%B7%E6%8C%87%E5%8D%97" target="_blank">《OceanBase 客户端 用户指南》</a></td>
    <td width="55%">该文档主要介绍OceanBase数据库的Java客户端和C客户端的使用方法。</td>
    <td width="7%"><a href="https://raw.github.com/alibaba/oceanbase/oceanbase_0.4/doc/wiki/OceanBase 描述.pdf"></a><a href="https://raw.github.com/alibaba/oceanbase/oceanbase_0.4/doc/wiki/OceanBase 客户端 用户指南.pdf">下载</a></td>
  </tr>
    <tr>
    <td><div align="center">5</div></td>
    <td><a href="https://github.com/alibaba/oceanbase/wiki/OceanBase-%E5%AE%A2%E6%88%B7%E7%AB%AF-%E7%94%A8%E6%88%B7%E6%8C%87%E5%8D%97%EF%BC%88%E9%98%BF%E9%87%8C%E5%86%85%E9%83%A8%EF%BC%89" target="_blank">《OceanBase 客户端 用户指南（阿里内部）》</a></td>
    <td><p>该文档主要介绍OceanBase Java客户端和OceanBase C客户端的使用方法。与《OceanBase 客户端 用户指南》相比，本文档增加了OceanBase配置中心和OceanBase Java客户端中的阿里内部模块“oceanbase.jar”的介绍。</p>
    </td>
    <td><a href="https://github.com/alibaba/oceanbase/blob/oceanbase_0.4/doc/wiki/OceanBase%20%E5%AE%A2%E6%88%B7%E7%AB%AF%20%E7%94%A8%E6%88%B7%E6%8C%87%E5%8D%97%EF%BC%88%E9%98%BF%E9%87%8C%E5%86%85%E9%83%A8%EF%BC%89.pdf?raw=true">下载</a></td>
  </tr>
  <tr>
    <td width="9%"><div align="center">6</div></td>
    <td width="29%"><a href="https://github.com/alibaba/oceanbase/wiki/OceanBase-SQL-%E5%8F%82%E8%80%83%E6%8C%87%E5%8D%97" target="_blank">《OceanBase SQL 参考指南》</a></td>
    <td width="55%">该文档主要介绍OceanBase数据库支持的SQL语言、语法规则和使用方法等。</td>
    <td width="7%"><a href="https://raw.github.com/alibaba/oceanbase/oceanbase_0.4/doc/wiki/OceanBase 描述.pdf"></a><a href="https://raw.github.com/alibaba/oceanbase/oceanbase_0.4/doc/wiki/OceanBase%20SQL%20%E5%8F%82%E8%80%83%E6%8C%87%E5%8D%97.pdf">下载</a></td>
  </tr>
  <tr>
    <td width="9%"><div align="center">7</div></td>
    <td width="29%"><a href="https://github.com/alibaba/oceanbase/wiki/OceanBase-%E5%8F%82%E8%80%83%E6%8C%87%E5%8D%97" target="_blank">《OceanBase 参考指南》</a></td>
    <td width="55%">该文档主要介绍OceanBase的日志参考、系统结果码和术语等信息。</td>
    <td width="7%"><a href="https://raw.github.com/alibaba/oceanbase/oceanbase_0.4/doc/wiki/OceanBase 描述.pdf"></a><a href="https://raw.github.com/alibaba/oceanbase/oceanbase_0.4/doc/wiki/OceanBase 参考指南.pdf">下载</a></td>
  </tr>
</table>



<h1>4 OceanBase资源列表</h1>
* [OceanBase SQL 用户参考手册](https://github.com/alibaba/oceanbase/wiki/OceanBase-SQL-%E7%94%A8%E6%88%B7%E5%8F%82%E8%80%83%E6%89%8B%E5%86%8C)
* [OceanBase SQL管理员手册](https://github.com/alibaba/oceanbase/wiki/OceanBase-SQL%E7%AE%A1%E7%90%86%E5%91%98%E6%89%8B%E5%86%8C)
* [ChunkServer设计文档](https://github.com/alibaba/oceanbase/tree/oceanbase_0.4/doc/chunkserver%E8%AE%BE%E8%AE%A1%E6%96%87%E6%A1%A3)
* [mergeServer设计文档](https://github.com/alibaba/oceanbase/tree/oceanbase_0.4/doc/mergeserver%E8%AE%BE%E8%AE%A1%E6%96%87%E6%A1%A3)
* [rootServer设计文档](https://github.com/alibaba/oceanbase/tree/oceanbase_0.4/doc/rootserver%E8%AE%BE%E8%AE%A1%E6%96%87%E6%A1%A3)
* [updateServer设计文档](https://github.com/alibaba/oceanbase/tree/oceanbase_0.4/doc/updateserver%E8%AE%BE%E8%AE%A1%E6%96%87%E6%A1%A3)
* [运维文档](https://github.com/alibaba/oceanbase/tree/oceanbase_0.4/doc/%E4%BD%BF%E7%94%A8%E8%BF%90%E7%BB%B4)
* [Project Plan](https://github.com/alibaba/oceanbase/wiki/Project-Plan) 

<h1>5 联系我们</h1>
 <p align="left">如果您有任何疑问或是想了解OceanBase的最新开源动态消息，请联系我们：</p>
  <p align="left"><b>支付宝（中国）网络技术有限公司·OceanBase团队</b></p>
  <p align="left">地址：杭州市万塘路18号黄龙时代广场B座</p>
  <p align="left">邮编：310099</p>
  <p align="left">邮箱： <a href="mailto:alipay-oceanbase-support@list.alibaba-inc.com">alipay-oceanbase-support@list.alibaba-inc.com</a></p>
  <p align="left"> 新浪微博：<a href="http://weibo.com/u/2356115944">http://weibo.com/u/2356115944</a></p>
  <p align="left">技术交流群（阿里旺旺）：853923637</p>
