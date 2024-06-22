# 使用Ubuntu作为基础镜像
FROM ubuntu:latest

# 更新软件包列表
RUN apt-get update

# 安装Go语言环境
RUN apt-get install -y golang


# 创建/app目录
WORKDIR /app

# 将当前目录的所有文件复制到/app目录中
COPY . /app

# 暴露端口6767
EXPOSE 6767

