<?php

namespace Cake\Redis\Driver;

use Cake\Redis\DriverInterface;

use RedisCluster;

class PHPRedisClusterDriver extends RedisCluster implements DriverInterface
{
    /**
     * Redis cluster wrapper.
     *
     * @var \RedisCluster
     */
    protected $_Redis;

    /**
     * Initializes the Redis client
     *
     * @param array $config
     */
    public function __construct($config)
    {
        $config = array_filter($config);
        $config += [
            'name' => null,
            'timeout' => 0,
            'read_timeout' => 0,
            'persistent' => false,
            'failover' => null,
            'options' => [],
        ];

        parent::__construct($config['name'], $config['nodes'], (float)$config['timeout'], $config['read_timeout'], $config['persistent']);

        try {
            $slaveFailover = \RedisCluster::FAILOVER_NONE;
            switch ($config['failover']) {
                case 'distribute':
                    $slaveFailover = \RedisCluster::FAILOVER_DISTRIBUTE;
                    break;
                case 'error':
                    $slaveFailover = \RedisCluster::FAILOVER_ERROR;
                    break;
                case 'slaves':
                    $slaveFailover = \RedisCluster::FAILOVER_DISTRIBUTE_SLAVES;
                    break;
            }

            /** @phpstan-ignore-next-line */
            $this->setOption(\RedisCluster::OPT_SLAVE_FAILOVER, $slaveFailover);
        } catch (\RedisClusterException $e) {
            if (class_exists(Log::class)) {
                Log::error('RedisClusterEngine could not connect. Got error: ' . $e->getMessage());
            }
        }
    }

    /**
     * Starts a multi command and calls the provided callable by passing the
     * pipeline object as first parameter.
     *
     * @param callable $operation Callable that will get the Redis client as first parameter
     * @param array The result of each command executed in the trasaction
     */
    public function transactional(callable $operation)
    {
        $transaction = $this->multi();

        try {
            $operation($transaction);
            return $transaction->exec();
        } catch (\Exception $e) {
            $transaction->discard();
            throw $e;
        }
    }
}
