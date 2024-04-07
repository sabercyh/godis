# 使用Ubuntu作为基础镜像
FROM ubuntu:latest

# 更新软件包列表
RUN apt-get update

# 安装Go语言环境
RUN apt-get install -y golang

# 安装其他软件，例如安装nginx
RUN apt-get install -y nginx

# 创建/app目录
WORKDIR /app

# 将当前目录的所有文件复制到/app目录中
COPY . /app

# 暴露端口6767
EXPOSE 6767

# 当容器启动时，运行nginx
CMD ["nginx", "-g", "daemon off;"]
