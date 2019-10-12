<?php

namespace ElasticSearchBundle\Helper;

use Elastica\Client;

class ElasticSearchHelper
{
    private $elasticaConfig;

    const  HOST = 'host';
    const PORT = 'port';
    const TIMEOUT = 'timeout';
    const CONNECTION_TIMEOUT = 'connectTimeout';

    /**
     * ElasticSearchHelper constructor.
     * @param $elasticaConfig
     */
    public function __construct($elasticaConfig)
    {
        $this->elasticaConfig = $elasticaConfig;
    }

    /**
     * @param string $connectionName
     * @return Client
     */
    public function getClient($connectionName)
    {
        $elasticaClient = new Client([
            self::HOST => $this->elasticaConfig[$connectionName][self::HOST],
            self::PORT => $this->elasticaConfig[$connectionName][self::PORT],
            self::TIMEOUT => $this->elasticaConfig[$connectionName][self::TIMEOUT],
            self::CONNECTION_TIMEOUT => $this->elasticaConfig[$connectionName][self::CONNECTION_TIMEOUT]
        ]);

        return $elasticaClient;
    }

    /**
     * @param array $servers
     * @return Client
     */
    static public function getCluster(array $servers)
    {
        $cluster = new Client([
            'servers' => [$servers]
        ]);

        return $cluster;
    }

    /**
     * @param Client $elasticaClient
     * @return bool
     */
    static public function isConnected(Client $elasticaClient)
    {
        $status = $elasticaClient->getStatus();
        $status->refresh();

        return true;
    }
}
