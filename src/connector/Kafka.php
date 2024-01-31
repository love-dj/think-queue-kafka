<?php

namespace think\queue\connector;

use Exception;
use RdKafka;
use think\queue\Connector;
use think\queue\InteractsWithTime;
use think\queue\job\Kafka as KafkaJob;


class Kafka extends Connector
{
    use InteractsWithTime;

    /**
     * @var string
     */
    protected $defaultQueue;
    /**
     * @var array
     */
    protected $config;
    /**
     * @var string
     */
    private $correlationId;
    /**
     * @var \RdKafka\Producer
     */
    private $producer;
    /**
     * @var \RdKafka\TopicConf
     */
    private $topicConf;
    /**
     * @var \RdKafka\Consumer
     */
    private $consumer;
    /**
     * @var array
     */
    private $topics = [];
    /**
     * @var array
     */
    private $queues = [];

    /**
     * @throws \Exception
     */
    public function __construct($config)
    {
        $this->createConnection($config);
    }

    /**
     * @param $config
     * @throws Exception
     */
    public function createConnection($config): void
    {
        /** @var Producer $producer */
        $producer = new \RdKafka\Producer();
        $producer->addBrokers($config['brokers']);

        /** @var TopicConf $topicConf */
        $topicConf = new \RdKafka\TopicConf();
        $topicConf->set('auto.offset.reset', 'largest');

        /** @var Conf $conf */
        $conf = new \RdKafka\Conf();
        if (true === $config['sasl_enable']) {
            $conf->set('sasl.mechanisms', 'PLAIN');
            $conf->set('sasl.username', $config['sasl_plain_username']);
            $conf->set('sasl.password', $config['sasl_plain_password']);
            $conf->set('ssl.ca.location', $config['ssl_ca_location']);
        }
        $conf->set('group.id', array_get($config, 'consumer_group_id', 'php-pubsub'));
        $conf->set('metadata.broker.list', $config['brokers']);
        $conf->set('enable.auto.commit', 'false');
        $conf->set('offset.store.method', 'broker');
        $conf->setDefaultTopicConf($topicConf);

        /** @var Consumer $consumer */
        $this->consumer     = new \RdKafka\Consumer($conf);
        $this->defaultQueue = $config['queue'];
        $this->producer     = $producer;
        $this->topicConf    = $topicConf;
        $this->consumer     = $consumer;
        $this->config       = $config;
    }


    /**
     * @param null $queue
     * @return int
     */
    public function size($queue = null): int
    {
        return $this->producer->getProducer()->getOutQLen();
    }


    /**
     * 将一个新作业推入队列。
     * @param        $job
     * @param string $data
     * @param null   $queue
     * @return string|null
     */
    public function push($job, $data = '', $queue = null)
    {
        $topic = $this->getTopic($queue);
        return $this->pushRaw(
            $this->createPayload($job, $data),
            $topic
        );
    }


    /**
     * 生产消息.
     * @param       $payload
     * @param null  $queue
     * @param array $options
     * @return string|void
     */
    public function pushRaw($payload, $queue = null, array $options = [])
    {
        $pushRawCorrelationId = $this->getRandomId();
        $this->producer->produce(
            $queue,
            $payload,
            data_get($options, 'available_at'),
            $pushRawCorrelationId
        );
        return $pushRawCorrelationId;
    }

    /**
     * 根据名称返回一个Kafka Topic
     *
     * @param string|null $queue
     * @param bool        $isDelayed
     * @return \RdKafka\ProducerTopic
     */
    private function getTopic(?string $queue, bool $isDelayed = false): string
    {
        $queueName = $queue ?? $this->defaultQueue;

        return $isDelayed ? $queueName . '-delayed' : $queueName;
    }

    /**
     * 检索相关id或唯一id。
     *
     * @return string
     */
    public function getRandomId(): string
    {
        return $this->correlationId ?: uniqid('', true);
    }

    /**
     * 延迟后将新作业推入队列。
     *
     * @param \DateTime|int $delay
     * @param string        $job
     * @param mixed         $data
     * @param string        $queue
     *
     * @return string|null
     */
    public function later($delay, $job, $data = '', $queue = null)
    {
        $topic = $this->getTopic($queue, true);
        return $this->pushRaw(
            $this->createPayload($job, $data),
            $topic,
            ['available_at' => (string)$this->availableAt($delay)]
        );
    }


    /**
     * 消费消息.
     * @param null $queue
     * @return \think\queue\job\Kafka|void|null
     */
    public function pop($queue = null)
    {
        try {
            $queue = $queue ?: $this->defaultQueue;
            if (!array_key_exists($queue, $this->queues)) {
                $this->queues[$queue] = $this->consumer->newQueue();
                $this->topics[$queue] = $this->consumer->newTopic($queue, $this->topicConf);
                $this->topics[$queue]->consumeQueueStart(0, RD_KAFKA_OFFSET_STORED, $this->queues[$queue]);
            }
            $message = $this->queues[$queue]->consume(1000);
            if ($message === null) {
                return null;
            }
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    return new KafkaJob(
                        $this, $message, $queue ?: $this->defaultQueue, $this->topics[$queue]
                    );
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    break;
                default:
                    throw new \RuntimeException($message->errstr(), $message->err);
            }
        } catch (\RdKafka\Exception $exception) {
            throw new \RuntimeException('不能从队列中跳出来', 0, $exception);
        }
    }

    /**
     * @param        $job
     * @param string $data
     * @return array
     */
    protected function createPayloadArray($job, $data = '')
    {
        return array_merge(parent::createPayloadArray($job, $data), [
            'id'       => $this->getRandomId(),
            'attempts' => 0,
        ]);
    }

    /**
     * @return \RdKafka\Consumer
     */
    public function getConsumer(): \RdKafka\Consumer
    {
        return $this->consumer;
    }
}
