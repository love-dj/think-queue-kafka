<?php

namespace think\queue\job;

use RdKafka\ConsumerTopic;
use RdKafka\Message;
use think\queue\connector\Kafka as KafkaQueue;
use think\queue\Job;

class Kafka extends Job
{
    /**
     * @var Kafka
     */
    protected $connection;
    /**
     * @var Kafka
     */
    protected $queue;
    /**
     * @var Message
     */
    protected $message;

    /**
     * @var ConsumerTopic
     */
    protected $topic;

    /**
     * @var array
     */
    protected $decoded;

    /**
     * Kafka constructor.
     * @param KafkaQueue    $connection
     * @param Message       $message
     * @param string        $queue
     * @param ConsumerTopic $topic
     */
    public function __construct(KafkaQueue $connection, Message $message, $queue, ConsumerTopic $topic)
    {
        $this->connection = $connection;
        $this->message    = $message;
        $this->queue      = $queue;
        $this->topic      = $topic;
    }

    /**
     * 删除任务
     * @return void
     */
    public function delete()
    {
        try {
            parent::delete();
            $this->connection->getConsumer()->commitAsync($this->message);
        } catch (\RdKafka\Exception $exception) {
            throw new \RuntimeException('无法从队列中删除作业', 0, $exception);
        }
    }

    /**
     * 重新发布任务
     * @param int $delay
     * @return void
     */
    public function release($delay = 0)
    {
        parent::release($delay);
        $this->delete();
        /*
         * 有些作业没有命令集，所以退回到只向它发送作业名称字符串
         */
        $body = $this->payload();
        if (isset($body['data']['command']) === true) {
            $job = $this->unserialize($body);
        } else {
            $job = $this->getName();
        }
        $data = $body['data'];
        if ($delay > 0) {
            $this->connection->later($delay, $job, $data, $this->getQueue());
        } else {
            $this->connection->push($job, $data, $this->getQueue());
        }
    }


    /**
     * 非系列化 job.
     *
     * @param array $body
     *
     * @return mixed
     * @throws Exception
     *
     */
    private function unserialize(array $body)
    {
        try {
            return unserialize($body['data']['command']);
        } catch (\Exception $exception) {
            throw new \RuntimeException('序列化失败', 0);
        }
    }

    /**
     * @return int
     */
    public function attempts()
    {
        return $this->payload('attempts') + 1;
    }

    /**
     * Get the raw body string for the job.
     * @return string
     */
    public function getRawBody()
    {
        return $this->message->payload;
    }

    /**
     * Get the job identifier.
     *
     * @return string
     */
    public function getJobId()
    {
        return $this->payload('id');
    }
}
