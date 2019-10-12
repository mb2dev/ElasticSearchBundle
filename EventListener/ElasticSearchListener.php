<?php

namespace ElasticSearchBundle\EventListener;

use Doctrine\Common\EventSubscriber;
use Doctrine\ORM\Event\LifecycleEventArgs;
use ElasticSearchBundle\Event\ElasticSearchEvent;
use ElasticSearchBundle\Handler\ElasticSearchHandler;
<<<<<<< Updated upstream
=======
use Exception;
use Symfony\Component\EventDispatcher\EventDispatcher;
>>>>>>> Stashed changes
use Symfony\Component\EventDispatcher\EventDispatcherInterface;

class ElasticSearchListener implements EventSubscriber
{
    /** @var array  */
    protected $aTypes;

    /** @var EventDispatcherInterface  */
    protected $eventDispatcher;

    /** @var ElasticSearchHandler  */
    protected $elasticSearchHandler;

    /** @var array  */
    protected $mapping;

    /** @var array $subscribedEvents */
    static protected $subscribedEvents = [
        'postPersist',
        'postUpdate',
        'postRemove',
        'preRemove'
    ];


    /**
     * ElasticSearchListener constructor.
     * @param EventDispatcher $eventDispatcher
     * @param ElasticSearchHandler $elasticSearchHandler
     */
    public function __construct(EventDispatcher $eventDispatcher, ElasticSearchHandler $elasticSearchHandler)
    {
        $this->eventDispatcher = $eventDispatcher;
        $this->elasticSearchHandler = $elasticSearchHandler;

        $this->mapping = $elasticSearchHandler->getMappings();
        foreach ($this->mapping as $key=>$mapping){
            $this->aTypes[] = $key;
        }
    }

    /**
     * @return array|string[]
     */
    public function getSubscribedEvents()
    {
        return self::$subscribedEvents;
    }

    /**
     * @param LifecycleEventArgs $args
     * @throws Exception
     */
    public function postPersist(LifecycleEventArgs $args)
    {
        $this->sendEvent($args, 'persist');
    }

    /**
     * @param LifecycleEventArgs $args
     * @throws Exception
     */
    public function preRemove(LifecycleEventArgs $args)
    {
        $this->sendEvent($args, 'remove');
    }

    /**
     * @param LifecycleEventArgs $args
     */
    public function postRemove(LifecycleEventArgs $args)
    {
        $this->sendEvent($args, 'remove');
    }

    /**
     * @param LifecycleEventArgs $args
     * @throws Exception
     */
    public function postUpdate(LifecycleEventArgs $args)
    {
        $this->sendEvent($args, 'update');
    }

    /**
     * @param LifecycleEventArgs $args
     * @param $action
     * @throws Exception
     */
    public function sendEvent(LifecycleEventArgs $args, $action)
    {
        $entity = $args->getEntity();
        $array  = explode("\\",get_class($entity));
        $type   = end($array);

        if (!in_array($type, $this->aTypes)) {
            return;
        }

        if (!array_key_exists('auto_event', $this->mapping[$type]) || !$this->mapping[$type]['auto_event']) {
            $event = new ElasticSearchEvent($action, $entity);
            $this->eventDispatcher->dispatch("elasticsearch.event", $event);
            return;
        }

        $this->_catchEvent(
            $entity,
            $this->mapping[$type]['transformer'],
            $this->mapping[$type]['connection'],
            $this->elasticSearchHandler->getIndexName($type),
            $action
        );
    }

    /**
     * @param $entity
     * @param $transformer
     * @param $connectionName
     * @param $indexName
     * @param $action
     */
    private function _catchEvent($entity, $transformer, $connectionName, $indexName, $action)
    {
        if (($action == 'persist' || $action == 'update') && !$this->isSoftDeleted($entity)) {
            $this->elasticSearchHandler->sendToElastic($entity, $transformer, $connectionName, $indexName);
            return;
        }

        $this->elasticSearchHandler->removeFromElastic($entity, $connectionName, $indexName);
    }

    /**
     * @param $entity
     * @return bool
     */
    private function isSoftDeleted($entity)
    {
        return (method_exists($entity, 'getDeletedAt') && $entity->getDeletedAt() != null);
    }

}
