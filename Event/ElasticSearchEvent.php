<?php

namespace ElasticSearchBundle\Event;

use Symfony\Component\EventDispatcher\Event;

class ElasticSearchEvent extends Event
{
    /** @var  string */
    protected $action;
    protected $entity;

    /**
     * ElasticSearchEvent constructor.
     * @param $action
     * @param $entity
     */
    public function __construct($action, $entity)
    {
        $this->action          = $action;
        $this->entity          = $entity;
    }

    /**
     * @return string
     */
    public function getAction()
    {
        return $this->action;
    }


    /**
     * @return mixed
     */
    public function getEntity()
    {
        return $this->entity;
    }

}
