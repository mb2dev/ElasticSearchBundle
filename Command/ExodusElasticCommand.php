<?php

namespace ElasticSearchBundle\Command;

use Doctrine\Common\Persistence\Mapping\MappingException;
use Doctrine\ORM\OptimisticLockException;
use Doctrine\ORM\ORMException;
use Elastica\Index;
use Elastica\Query;
use Elastica\ResultSet;
use ElasticSearchBundle\Exception\ImplementationException;
use ElasticSearchBundle\Index\ElasticSearchEntityInterface;
use ElasticSearchBundle\Index\ElasticSearchIndexInterface;
use ElasticSearchBundle\Providers\AbstractProvider;
use LogicException;
use ReflectionException;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

class ExodusElasticCommand extends AbstractCommand
{
    protected $nbrDocumentsFound = 0;
    protected $nbrDocumentTested = 0;
    protected $nbrEntitiesRemoved = 0;
    protected $nbrDone = 0;

    /** @var int */
    protected $batch = 100;

    const TYPE_EXCLUDES = [
    ];

    protected function configure()
    {
        $this->setName('elastic:exodus')
            ->setDescription('Remove not linked entities from Elastic Search')
            ->addOption('type', 't', InputOption::VALUE_REQUIRED, 'Type of document you want to exodus. You must to have configure it before use', null)
            ->addOption('batch', 'b', InputOption::VALUE_REQUIRED, 'Number of Documents per batch', null)
            ->addOption('dry-run', 'd', InputOption::VALUE_NONE, 'Dry run: do not make any change on ES')
            ->addOption('max-id', null, InputOption::VALUE_REQUIRED, 'Refresh from that _id (id from ES) Put the Max id.', null)
            ->addOption('id', null, InputOption::VALUE_REQUIRED, 'Refresh a specific object with his Id.', null)
            ->addOption('where', null, InputOption::VALUE_REQUIRED, 'Refresh objects with specific ElasticIndex field (`client_id`) ', null);
    }

    /**
     * @param InputInterface $input
     * @param OutputInterface $output
     *
     * @return int
     *
     * @throws MappingException
     * @throws ORMException
     * @throws OptimisticLockException
     * @throws ImplementationException
     * @throws ReflectionException
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $this->init($input, $output);

        $this->writeln('<info>'.self::completeLine($this->getName().' '.date('H:i:s')).'</info>');

        if ($this->verbose) {
            $sMsg = ($this->type) ? "Type: {$this->type}\n" : '';
            $sMsg .= "Batch: {$this->batch}";
            $this->output->writeln($sMsg);
        }

        if (false === empty($this->type)) {
            if (false === $this->checkType($this->type)) {
                return self::EXIT_FAILED;
            }

            return $this->processBatch($this->type);
        }

        $returnValue = self::EXIT_SUCCESS;

        foreach ($this->mappings as $type) {
            $returnedValue = $this->processBatch($type);
            if (self::EXIT_SUCCESS != $returnedValue) {
                $returnValue = self::EXIT_FAILED;
            }
        }

        return $returnValue;
    }

    /**
     * @param Index $index
     * @param string|ElasticSearchEntityInterface $type
     * @param AbstractProvider $provider
     * @param ResultSet $resultSet
     * @param int $maxId
     * @throws ImplementationException
     * @throws ReflectionException
     */
    private function removeFromElasticSearchByBatch($index, $type, $provider, ResultSet $resultSet, int &$maxId)
    {
        $ESIds = [];
        foreach ($resultSet as $result) {
            $data = $result->getDocument()->getData();
            if (false === isset($data['id'])) {
                throw new LogicException($type.' class has no field `id`.');
            }

            $id = $result->getDocument()->getData()['id'];
            $doctrineIds[] = $id;
            $ESIds[$id] = $result->getDocument()->getParams();

            if ($id > $maxId) {
                $maxId = $id;
            }
        }

        $doctrineIds = array_unique($doctrineIds ?? []);

        if (empty($doctrineIds)) {
            return;
        }

        $this->nbrDocumentTested += count($doctrineIds);

        $condition = $provider::createINCondition('id', $doctrineIds);
        $query = $provider->getQuery([$condition], 'u.id');
        $entities = $query->getResult();

        if (count($entities) === count($doctrineIds)) {
            // Same number of entities that expected: no pb
            return;
        }

        // Remove from ES entities not present in DB
        $entitiesIds = [];
        foreach ($entities as $entity) {
            $entitiesIds[] = $entity['id'];
        }

        $removedIds = array_diff($doctrineIds, $entitiesIds);
        foreach ($removedIds as $id) {
            $this->writeln('<error>'.self::CLEAR_LINE."Entity not found: {$id}</error>", OutputInterface::VERBOSITY_DEBUG);

            // Remove document from ElasticSearch
            ++$this->nbrEntitiesRemoved;

            if ($this->dryRun) {
                continue;
            }

            $type = $index->getType($ESIds[$id]['_type']);
            $response = $type->deleteById($ESIds[$id]['_id']);

            if ($response->hasError()) {
                $this->output->writeln(self::CLEAR_LINE."\tError: {$response->getError()}", OutputInterface::VERBOSITY_DEBUG);
            }
        }
    }

    /**
     * @param string|ElasticSearchIndexInterface $indexClass
     *
     * @return int
     */
    private function countAllResult(string $indexClass)
    {
        $index = $this->getIndexFromType($indexClass);
        $query = new Query();

        return $index->count($query);
    }

    /**
     * @param string|ElasticSearchEntityInterface $type
     *
     * @return int
     *
     * @throws ImplementationException
     * @throws ReflectionException
     */
    private function processBatch(string $type)
    {
        if (in_array($type, self::TYPE_EXCLUDES)) {
            $this->writeln("Excluded type: <error>{$type}</error>");

            return AbstractCommand::EXIT_SUCCESS;
        }

        $this->writeln("Type: <info>{$type}</info>");

        $provider = $this->getProvider($type);
        $index = $this->getIndexFromType($type::getIndex());

        $from = $this->offset;
        $countTotalDocuments = $this->countAllResult($type::getIndex());

        $progressBar = $this->getProgressBar($this->output, $countTotalDocuments - $this->offset);
        $this->initCounter();

        $maxId = $this->maxId ?? 0;

        do {
            $bool = new Query\BoolQuery();
            $range = new Query\Range();
            $query = (new Query())
                ->addSort(['id' => ['order' => 'asc']])
                ->setSize($this->batch);

            if ($this->where && $this->id) {
                $match = (new Query\Match())->setFieldQuery($this->where, $this->id);
            }

            $range->addField('id', ['gt' => $maxId]);
            $bool->addMust($range);
            if (isset($match)) {
                $bool->addMust($match);
            }

            $query->setQuery($bool);
            // Get documents from ElasticSearch
            try {
                $resultSet = $index->search($query);
            } catch(\Elastica\Exception\ResponseException $t) {
                $this->output->writeln(self::CLEAR_LINE."Error: <error>{$t->getMessage()}</error>");

                return AbstractCommand::EXIT_FAILED;
            }

            try {
                $this->removeFromElasticSearchByBatch($index, $type, $provider, $resultSet, $maxId);
            } catch (LogicException $exception) {
                $this->writeln('<error>'.$exception->getMessage().'</error>');
                break;
            }

            $addMore = $this->getNextStep($this->batch, $this->offset, $from, $this->limit);
            $from += $this->batch;

            $progressBar->setMessage("$from/$countTotalDocuments (MaxId: $maxId)");
            $progressBar->advance(count($resultSet));
        } while ((count($resultSet) > 0) && ($addMore > 0));

        $progressBar->finish();

        $this->output->writeln(self::CLEAR_LINE."Documents tested: <info>{$this->nbrDocumentTested}</info>");
        if ($this->nbrEntitiesRemoved) {
            $this->output->writeln(self::CLEAR_LINE."Entities removed: <error>{$this->nbrEntitiesRemoved}</error>");
        } else {
            $this->output->writeln(self::CLEAR_LINE."Entities removed: <info>{$this->nbrEntitiesRemoved}</info>");
        }

        return AbstractCommand::EXIT_SUCCESS;
    }

    /**
     * @param int $batch  Number of document to get each loop
     * @param int $offset Offset
     * @param int $from   The last query FROM
     * @param int $limit  The number of total result
     *
     * @return mixed
     */
    private function getNextStep($batch, $offset, $from, $limit)
    {
        // No limit
        if (empty($limit) || $limit <= 0) {
            return $batch;
        }

        $resultRest = ($offset + $limit) - $from;

        return ($resultRest > $batch) ?
            $batch :
            $resultRest;
    }

    private function initCounter()
    {
        $this->nbrEntitiesRemoved = 0;
        $this->nbrDocumentTested = 0;
        $this->nbrDocumentsFound = 0;
        $this->nbrDone = 0;
    }
}
