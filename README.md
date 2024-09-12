## 目录结构
---
### alert
- 程序逻辑报警功能，使用飞书报警
- 主要是监听日志，根据日志中的msg前缀做报警

---
### apollo
- 实现apollo配置加载，外层用loader容器包配置即可

---
### backend
- 依赖consul实现的服务器发现，根据tag做发现
- 支持http和tcp服务器

---
### consul
- 实现consul监听服务器发现
- 实现consul注册
- 实现consul配置监听加载，外层用loader容器包配置即可

---
### ginserver
- 对gin的简单包装，外层负责初始化和注册回调函数

---
### gnetserver
- 对gnet的包装，外层实现EventHandler

---
### goredis
- goRedis的包装
  
---
### httprequest
- 对http调用的包装

---
### loader
- 加载配置

---
### log
- 日志包装

---
### metrics
- 指标包装，对Redis MySQL httprequest gin模块注册hook，实现指标监控

---
### mrchche
- mysql到redis的换存层
  
---
### mysql
- MySQL的包装

---
### redis
- Redis的包装，建议使用goredis

---
### tcp
- TCP连接的包装
  
---
### tcpserver
- TCP服务器监听的包装

---
### utils
- 基础包装
- 不依赖此目录的其他任何包
