<?php

namespace ElasticSearchBundle\Handler;

use Elastica\Exception\NotFoundException;
use ElasticSearchBundle\Helper\ElasticSearchHelper;
use Exception;
use Symfony\Component\DependencyInjection\Container;

/**
 * Class ElasticSearchHandler
 * @package ElasticSearchBundle\Handler
 */
class ElasticSearchHandler
{
    /** @var  ElasticSearchHelper */
    protected $elasticSearchHelper;

    /** @var  Container */
    protected $container;

    /** @var  array */
    protected $mappings;

    public function __construct(Container $container, ElasticSearchHelper $elasticSearchHelper)
    {
        $this->container = $container;
        $this->elasticSearchHelper = $elasticSearchHelper;
        $this->mappings = $this->container->getParameter('elastica_mappings');

    }


    /**
     * @param $entity
     * @param $transformer
     * @param $connectionName
     * @param $indexName
     * @throws Exception
     */
    public function sendToElastic($entity, $transformer, $connectionName,$indexName)
    {
        $array      = explode("\\",get_class($entity));
        $type       = end($array);
        $document   = $this->container->get($transformer)->transform($entity);
        $index      = $this->elasticSearchHelper->getClient($connectionName)->getIndex($indexName);

        try {
            $index->getType($type)->addDocument($document);
            $index->refresh();
        } catch(NotFoundException $e) {

        }
    }

    /**
     * @param $entity
     * @param $connectionName
     * @param $indexName
     */
    public function removeFromElastic($entity, $connectionName, $indexName)
    {
        if (empty($entity->getId())) {
            return;
        }

        $array      = explode("\\",get_class($entity));
        $type       = end($array);
        $index      = $this->elasticSearchHelper->getClient($connectionName)->getIndex($indexName);

        try {
            $index->getType($type)->deleteById($entity->getId());
            $index->refresh();
        } catch(NotFoundException $e) {

        }
    }

    /**
     * @param $type
     * @return string
     */
    public function getIndexName($type)
    {
        if(array_key_exists('index_name', $this->mappings[$type])){
            $index_name = $this->mappings[$type]['index_name'];
        }else{
            $index_name = strtolower($type);
        }
        return $index_name;
    }

    /**
     * @return array
     */
    public function getMappings()
    {
        return $this->mappings;
    }

}
