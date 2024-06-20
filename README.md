# 功能介绍
`ch2s3`, 从字面意义上理解，就是将`clickhouse`的数据备份到`S3`对象存储上的意思。它同时提供了数据恢复的功能。

通过该工具，可以非常便捷地批量将表按照分区备份到`S3`，并且所有数据都是可恢复的。

`ch2s3`利用`clickhouse`的`backup`命令进行备份，在备份时会进行二次压缩，尽可能减小带宽压力和存储空间成本。对于有多副本的`clickhouse`集群，每个分片仅备份一个副本，不会重复存储数据。

该备份工具主要针对`clickhouse`集群，且表引擎为`MergeTree`的表，默认按照`toYYYYMMDD`格式的按天分区备份。如果有不同的分区方式，可自行指定分区。

之所以采用这种方式来备份，主要是为了将S3与clickhouse彻底解耦，如果将S3作为clickhouse的一块磁盘，如果S3出问题，很可能造成clickhouse集群无法正常工作。而ch2s3即使失败，也仅仅是备份失败，可以通过补数的方式重新备份，而不影响clickhouse集群的工作状态。

ch2s3会通过报表的形式输出每次备份的结果，包含一共备份了多少张表，成功了多少，失败了多少，每张表的条数，大小，耗时，以及总的大小和耗时。如果备份失败，也会罗列出失败原因，便于排障。
# 命令行参数
- `-p`
    - 指定`partition`，可以指定单个，也可以指定多个，当同时指定多个时，以逗号进行分隔
    - 如果不指定，默认以今天作为`partition`
- `-ttl`
    - 通过`ttl`的方式指定备份日期，比如可以指定7天前，3个月前，1年前的方式来动态备份
    - 注意通过指定`ttl`的方式备份时，注意清理备份后的原表数据（配置文件中`clean`设置为`true`）,否则存在重复备份的风险
- `--restore`
    - 是否恢复表， 如果指定了`--restore`， 代表这是一个恢复命令，它会将数据从S3恢复到原始表中。
    - 恢复表有几个前提：
        - S3上有原始数据， 且是通过ch2s3工具进行备份的
        - clickhouse集群有对应的表
        - 表内需要恢复的数据已被提前删除，否则恢复仍然可以成功，但是数据会重复
    - 如果指定了`--restore`选项，那么分区只能通过`-p`来指定，无法通过`-ttl`指定，因为原表数据已经不存在，我们已经无法通过查表的方式获取到具体的分区
# 配置文件
## 配置说明
配置文件放在`conf`目录下，配置文件名称为`backup.json`。包含以下内容：

- clickhouse

| 配置项| 默认值| 说明|
|------|------|-----|
|cluster||集群名|
|hosts||二层数组，外层为shard，内层为replica|
|port|9000|clickhouse端口|
|user|default|clickhouse连接用户|
|password||clickhouse连接密码|
|clean|true|备份成功后是否删除掉本地数据|
|database|default|需要备份的数据库|
|tables||需要备份的表，数组形式，可以是多个表|
|readTimeout|21600|client 连接超时时间， 默认6h|
- s3

| 配置项| 默认值| 说明|
|------|------|-----|
|endpoint||S3端点地址，需要带bucket名|
|region||S3区域,当备份失败要删除远端s3不完整数据时，该配置必填|
|cleanIfFail|false|备份失败是否删除S3数据|
|accessKey||访问秘钥|
|secretKey||秘钥|
|compress_method|lz4|压缩算法，支持lz4, lz4hc, zstd,deflate_qpl|
|compress_level|3|压缩等级|
|ignore_exists|true|如果S3上已有备份数据，是否报错，默认不报错， 仅当备份时该参数有效|
|retry_times|0|备份失败重试次数，默认不重试|
## 配置示例
```json
{
    "clickhouse": {
        "cluster":"abc",
        "hosts":[
            ["192.168.101.93", "192.168.101.94"],
            ["192.168.101.96", "192.168.101.97"]
        ],
        "port": 19000,
        "user":"default",
        "password":"123456",
		"clean": false,
        "database":"default",
        "tables":[
            "test_ck_dataq_r77",
            "test_ck_dataq_r76",
			"test_ck_dataq_r75",
			"test_ck_dataq_r74"
        ]
    },
    "s3":{
        "endpoint":"http://192.168.101.94:49000/backup",
        "accessKey":"VdmPbwvMlH8ryeqW",
        "secretKey":"8z16tUktXpvcjjy5M4MqXvCks5MMHb63",
        "compress_method":"zstd"
    }
}
```
# 如何使用
## 备份
- 指定分区
```bash
./ch2s3 -p "20230731"
```
- 指定TTL
```bash
./ch2s3 -ttl "5 DAY"  #备份5天前的数据（当天）
./ch2s3 -ttl "2 WEEK" #备份2周前的数据（当天）
./ch2s3 -ttl "3 MONTH" #备份3个月前的数据（当天）
./ch2s3 -ttl "1 YEAR" #备份1年前的数据（当天） 
```
## 恢复
- 恢复一个分区
```bash
./ch2s3 -p "20230731" --restore
```
- 恢复多个分区
```bash
./ch2s3 -p "19700101,20230101,20230731" --restore
```
# 报表
报表默认输出在reporter目录，示例如下：
```txt
Backup Date: 20230731

+--------------------------------+---------------+---------------+---------------+---------------+
|            table               |     rows      |  size(local)  |   elapsed     |    status     |
+--------------------------------+---------------+---------------+---------------+---------------+
|       default.test_ck_dataq_r77|       25442407|      48.93 GiB|         41 sec|        SUCCESS|
|       default.test_ck_dataq_r76|       20490054|      40.00 GiB|         24 sec|        SUCCESS|
|       default.test_ck_dataq_r75|       24731492|      48.16 GiB|         35 sec|        SUCCESS|
|       default.test_ck_dataq_r74|       16349252|      31.45 GiB|         26 sec|        SUCCESS|
+--------------------------------+---------------+---------------+---------------+---------------+

Total Tables: 4,  Success Tables: 4,  Failed Tables: 0,  Total Bytes: 168.53 GiB,  Elapsed: 126 sec


```
# 性能
经测试，备份160G数据，共计耗时120秒，平均1.33G/s， 存储到S3上数据大小为90G左右，有二次压缩。

# 最佳实践
## 定时任务
可以通过crontab拉起定时任务的方式实现每日备份，比如需要备份1年前的数据，只需要配置好配置文件后，通过crontab拉起下面的脚本即可：
```bash
0 2 * * * /usr/local/ch2s3/bin/ch2s3 -ttl "1 YEAR" > /var/log/ch2s3.log
```
以上表示每天晚上2点整执行ch2s3备份，每次备份一年前的数据。
## 失败补数
假设20230731备份失败，那么可以通过手动执行下面命令重新备份该分区数据：
```bash
/usr/local/bin/ch2s3 -p "20230731"
```
