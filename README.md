# TinyDFS

此项目实现了一个基础的分布式文件存储系统。我们参考了[《The Google File System》](https://github.com/XutongLi/Learning-Notes/blob/master/Distributed_System/Paper_Reading/GFS/The%20Google%20File%20System.pdf)这篇论文，但摒除了对文件的修改操作。该项目仅支持对文件的基础访问和添加操作。

| 操作类型 | 上传文件 | 下载文件 | 修改文件 | 重命名 | 移动文件 | 删除文件 | 获取文件信息 |
|:----:|:----:|:----:|:----:|:---:|:----:|:----:|:------:|
| 是否支持 | ✔    | ✔    | ✘    | ✔   | ✔    | ✔    | ✔      |

此项目包含四个子项目，分别为：
* [base](https://github.com/zzhtttsss/tinydfs-base):该项目的工具模块，包含各个模块的工具以及公共使用的部分。
* [client](https://github.com/zzhtttsss/tinydfs-client):该项目的客户端，是用户与系统的交互起点。
* [chunkserver](https://github.com/zzhtttsss/tinydfs-chunkserver):该项目负责存储文件的节点，类似于HDFS中的DataNode
* [master](https://github.com/zzhtttsss/tinydfs-master):该项目的逻辑中心，负责管理chunkserver和元数据，类似于HDFS中的NameNode

## 背景

本项目的开发目的，一是为了熟悉分布式环境下，文件的存储方式；二是入门分布式算法Raft，理解该算法的执行逻辑和使用方式；三是为后来的初学者一个学习的思路。因为我们也是初学者，在开发过程中，遇到问题的解决方式有限，甚至是一边学习一边解决，所以不足之处还望谅解。

这个项目使用 `Go` 语言开发，为了模拟分布式环境，使用 `Docker` 进行测试。在`Docker compose`中，我们部署了`3`个`master`、`5`个`chunkserver`以及`ectd`作为服务注册和发现的组件，`cadvisor`+`prometheus`+`grafana`作为可视化监控的组件。

## 安装

1. 将各个模块编译为`docker`镜像，在各个模块的目录下：

```bash
docker build -t [name] .
```

2. 运行`docker compose`文件：
```bash
docker compose -f [compose.yaml] up -d
```


## 维护者

[@zzhtttsss](https://github.com/zzhtttsss)

[@DividedMoon](https://github.com/dividedmoon)

## 使用许可

[MIT](LICENSE) © Richard Littauer
