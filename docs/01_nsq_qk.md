---
NSQ快速入门
---
+ 参考[文档](http://nsq.io/deployment/installing.html)进行安装
  + https://github.com/nsqio/nsq/releases/tag/v0.3.7 上面的路径下载编译好的版本
  + 放置路径在/home/frank/nsq-0.3.7/bin
  + 添加到环境变量PATH

     ```shell
     vim ~/.profile
     export PATH=/home/frank/nsq-0.3.7/bin:${PATH}
     source ~/.profile
     ```

+ 启动nsqlookupd
   ```shell
   $ nsqlookupd
   # 默认监听两个端口： TCP 4160 HTTP 4161
   ```

+ 启动nsqd
   ```shell
    $ nsqd --lookupd-tcp-address=127.0.0.1:4160
    # 默认监听两个端口： TCP 4150 HTTP 4151
   ```
     注：部署时需要指定--broadcast-address（对外ip），不然客户端根本连接不来

+ 启动nsqadmin
   ```shell
    $ nsqadmin --lookupd-http-address=127.0.0.1:4161
    默认监听一个端口： HTTP 4171
   ```

+ 推送一个初始化消息(同时也是在集群中创建一个topic)
    ```shell
    $ curl -d 'hello world 1' 'http://127.0.0.1:4151/put?topic=test'
    ```

+ 开启nsq_to_file（类似上面的Cosumer）
    将消息推送到file
    ```shell
    $ nsq_to_file --topic=test --output-dir=/tmp --lookupd-http-address=127.0.0.1:4161 
    ```
    将消息推送到stdout
    ```shell
    $ nsq_tail --topic=test --lookupd-http-address=127.0.0.1:4161
    ```

+ 推送更多消息到nsqd(curl在这类似生产者)

   ```shell
   $ curl -d 'hello world 2' 'http://127.0.0.1:4151/put?topic=test'
   $ curl -d 'hello world 3' 'http://127.0.0.1:4151/put?topic=test' 
   ```

+ 为了确认事情和我们预期的一样，在浏览器中浏览http://127.0.0.1:4171/ 查看nsqadmin的UI和统计信息。查看/tmp下面的log文件信息。

+ 这里最重要的是nsq_to_file（客户端）没有明确表明”test“ topic是在哪里生产的，它从nsqlookupd检索此信息(nsqd的tcp端口和ip信息)，然后不管连接的时间,没有消息丢失。