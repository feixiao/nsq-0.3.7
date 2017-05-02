# nsqd介绍

### nsqd介绍

+ nsq是负责接收，排队，投递消息给客户端。它可以独立运行，不过通常它是由nsqd 实例所在集群配置的（它在这能声明 topics 和 channels，以便大家能找到）。

+ 它在 2 个 TCP 端口监听，一个给客户端，另一个是 HTTP API。同时，它也能在第三个端口监听 HTTPS。

+ TCP接口说明：

  + 处理消费者连接，协议部分在protocol_v2.go中实现，[详细代码](https://github.com/feixiao/nsq-0.3.7/blob/master/nsqd/protocol_v2.go)。支持的操作如下，详细请参考[TCP协议详细说明](http://nsq.io/clients/tcp_protocol_spec.html)

    ![./img/003.png](./img/003.png)

  + FIN命令

    + 作用

      告知Channel这个id的消息已经被成功的处理

    + 消息格式

      ```shell
      FIN <message_id>\n
      ```

      <message_id> - message id as 16-byte hex string

  + RDY命令

    + 作用

      更新状态，通知nsqd，消费者最多接收多少条数据

    + 消息格式

      ```shell
      RDY <count>\n
      ```

      <count> - a string representation of integer N where 0 < N <= configured_max

  + REQ命令

    + 作用

      要求nsqd重新排序该消息(暗示消息出来出错)

    + 消息格式

      ```
      REQ <message_id> <timeout>\n
      ```

      <message_id> - message id as 16-byte hex string
      <timeout> - a string representation of integer N where N <= configured max timeout.0 is a special case that will not defer re-queueing

  + PUB命令

    + 作用

      发送消息给指定的TOPIC

    + 消息格式

      ```
      PUB <topic_name>\n
      [ 4-byte size in bytes ][ N-byte binary data ]
      ```

      <topic_name> - a valid string (optionally having #ephemeral suffix)

  + MPUB命令

    + 作用

      发送多条消息给指定的TOPIC

    + 消息格式

      ```shell
      MPUB <topic_name>\n
      [ 4-byte body size ]
      [ 4-byte num messages ]
      [ 4-byte message #1 size ][ N-byte binary data ]
            ... (repeated <num_messages> times)
      ```

  + DPUB命令(新版本已经废弃这个命令)
  + NOP命令(没有实际意义的命令，为什么存在？)
  + TOUCH命令

      + 作用

        重置待确认的消息的超时时间

      + 消息格式

        ```
        TOUCH <message_id>\n
        ```
  + SUB命令

      + 作用

        订阅某个topic下面的某个channel

      + 消息格式

        ```
        SUB <topic_name> <channel_name>\n
        ```
    + CLS命令

      + 作用

        关闭连接，没有消息会被发送

      + 消息格式

        ```
        CLS\n
        ```

    + AUTH命令

      + 作用

        认证(IDENTIFY)阶段如果auth_required=true，那么客户端必须在其他明确之前先进行认证

      + 消息格式

        ```
        AUTH\n
        [ 4-byte size in bytes ][ N-byte Auth Secret ]
        ```

+ HTTP接口提供的服务如下：

  ![./img/004.png](./img/004.png)

  在nsqd/http.go中实现上面的业务功能，[https://github.com/feixiao/nsq-0.3.7/blob/master/nsqd/http.go ](https://github.com/feixiao/nsq-0.3.7/blob/master/nsqd/http.go) 中有详细的说明。

  + GET /ping 		返回服务器的健康（内部出错就返回500）
  + POST /pub             推送一个消息给某个指定的topic（消息分为立即推送和延迟推送两种）
  + POST /mpub          推送多个消息给某个指定的topic（消息只有立即推送一种，但是有两种数据组织的形似，一个是以'\n'分隔、一个是4个字节长度加消息内容的方式，通过binary参数说明）
  + GET /stats              获取全面的运行时数据
  + POST /topic/create     创建topic（名字带有#ephemeral结尾，在删除完channel的时候需要删除topic）
  + POST /topic/delete     删除topic
  + POST /topic/empty     情况topic中的数据（未发送的数据）
  + POST /topic/pause      暂停一个topic
  + POST /topic/pause      启动一个topic
  + POST /channel/create 创建channel
  + POST /channel/delete 删除channel（topic是否为ephemeral觉得在没有channel的时候是否需要删除topic）
  + POST /channel/empty 清空channel
  + POST /channel/pause 暂停channel的数据流
  + POST /channel/unpause 重启channel的数据流

+ [详细的http协议](http://nsq.io/components/nsqd.html)

