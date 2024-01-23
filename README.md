## kafka

> 安装: `composer require love-dj/think-queue-kafka`
>
> 支持阿里云 账号密码->实例列表->静态用户名密码

~~~
 'kafka' => [
        'type' => 'kafka',
        'queue' => env('KAFKA_QUEUE', 'default'),
        'brokers' => env('KAFKA_BROKERS', 'localhost:9092'),
        'sasl_enable' => false,
        'sasl_plain_username' => env('KAFKA_SASL_PLAIN_USERNAME'),
        'sasl_plain_password' => env('KAFKA_SASL_PLAIN_PASSWORD'),
        'ssl_ca_location' => '',
        'consumer_group_id' => env('KAFKA_CONSUMER_GROUP_ID', 'think_queue'),
    ],

~~~

## 发布任务

> `think\facade\Queue::push($job, $data = '', $queue = null)`
> 和 `think\facade\Queue::later($delay, $job, $data = '', $queue = null)` 两个方法，前者是立即执行，后者是在`$delay`秒后执行

```
$job` 是任务名
且命名空间是`app\job`的，比如上面的例子一,写`Job1`类名即可
其他的需要些完整的类名，比如上面的例子二，需要写完整的类名`app\lib\job\Job2`
如果一个任务类里有多个小任务的话，如上面的例子二，需要用@+方法名`app\lib\job\Job2@task1`、`app\lib\job\Job2@task2
```

`$data` 是你要传到任务里的参数

`$queue` 队列名，指定这个任务是在哪个队列上执行，同下面监控队列的时候指定的队列名,可不填

## 监听任务并执行

```
&> php think queue:listen

&> php think queue:work
```

两种，具体的可选参数可以输入命令加 `--help` 查看

## 更多使用

> 详情请查看官方文档说明 https://github.com/top-think/think-queue/tree/3.0

