<?php

namespace ElasticSearchExtensionBundle\Command;

use Doctrine\Common\Persistence\Mapping\MappingException;
use Doctrine\ORM\EntityNotFoundException;
use Doctrine\ORM\OptimisticLockException;
use Doctrine\ORM\ORMException;
use Elastica\Document;
use Elastica\Type;
use Elastica\Type\Mapping;
use ElasticSearchBundle\Command\AbstractCommand;
use ElasticSearchBundle\Exception\ImplementationException;
use ReflectionException;
use Symfony\Component\Console\Exception\LogicException;
use Symfony\Component\Console\Helper\ProgressBar;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Process\Process;

/**
 * Class PopulateElasticCommand.
 */
class PopulateElasticCommand extends AbstractCommand
{
    /** @var int Microseconds */
    const POLLING_INTERVAL = 1000;

    const FIELD_PROCESS = 'process';
    const FIELD_PROGRESSBAR = 'progressbar';
    const FIELD_PROGRESSION = 'progression';
    const RESULT = 'Succeed: %d, Error: %d, Empty: %d';

    protected function configure()
    {
        $this->setName('elastic:populate')
            ->setDescription('Repopulate Elastic Search')
            ->addOption('limit', 'l', InputOption::VALUE_REQUIRED, 'Limit For selected Type', 0)
            ->addOption('offset', 'o', InputOption::VALUE_REQUIRED, 'Offset For selected Type', 0)
            ->addOption(
                'type',
                null,
                InputOption::VALUE_REQUIRED,
                'Type of document you want to populate. You must to have configure it before use',
                null
            )
            ->addOption('threads', null, InputOption::VALUE_REQUIRED, 'number of simultaneous threads')
            ->addOption('reset', null)
            ->addOption('batch', 'b', InputOption::VALUE_REQUIRED, 'Number of Document per batch')
            ->addOption('id', null, InputOption::VALUE_REQUIRED, 'Refresh a specific object with his Id', null)
            ->addOption('where', null, InputOption::VALUE_REQUIRED, 'Refresh objects with specific Doctrine field (`client`).', null)
            ->addOption('from', null, InputOption::VALUE_REQUIRED, 'From this date.', null)
            ->addOption('created', 'c', InputOption::VALUE_NONE, 'Use field `createdAt` to filter with date (default:`updatedAt`).')
            ->addOption('updated', 'u', InputOption::VALUE_NONE, 'Use field `updatedAt` to filter with date (default:`updatedAt`).')
            ->addOption('to', null, InputOption::VALUE_REQUIRED, 'To this date.', null);
    }

    /**
     * @param InputInterface $input
     * @param OutputInterface $output
     *
     * @return int
     *
     * @throws ImplementationException
     * @throws OptimisticLockException
     * @throws ReflectionException
     */
    protected function execute(InputInterface $input, OutputInterface $output)
    {
        ini_set('memory_limit', '1000M');

        $this->init($input, $output);

        if (false === empty($this->type)) {
            if (false === $this->checkType($this->type)) {
                return self::EXIT_FAILED;
            }

            return $this->_switchType($this->type);
        }

        $returnValue = self::EXIT_SUCCESS;
        foreach ($this->mappings as $type) {
            $returnedValue = $this->_switchType($type);
            if (self::EXIT_SUCCESS != $returnedValue) {
                $returnValue = self::EXIT_FAILED;
            }
        }

        return $returnValue;
    }

    /**
     * @param string|ElasticSearchEntityInterface $type
     *
     * @return int
     *
     * @throws ImplementationException
     * @throws ReflectionException
     */
    private function _switchType(string $type)
    {
        $numberPopulatedObjects = $numberTransformedError = $numberDocumentEmpty = 0;
        $this->type = $type;

        if ($this->reset) {
            $this->_resetType($type);
        }

        $transformerFactory = $this->getContainer()->get('module.synchronization.synctag.transformer.factory');

        $returnValue = ($this->batch)
            ? $this->createBatches($numberPopulatedObjects, $numberTransformedError, $numberDocumentEmpty)
            : $this->processBatch(
                $transformerFactory->create($type::getTransformer(), $this->getProvider($type)),
                $numberPopulatedObjects,
                $numberTransformedError,
                $numberDocumentEmpty);

        $this->writeln($type.":\t <info>[COMPLETE]</info>\t ".
            ($numberPopulatedObjects ?
                "$numberPopulatedObjects entities populated"
                : '<comment>No entity populated</comment>')
            ." ($numberTransformedError Transformer Errors / $numberDocumentEmpty Empty documents) "
        );

        return $returnValue;
    }

    /**
     * @param $type
     * @param $properties
     */
    private function _mappingFields($type, $properties)
    {
        // Define mapping
        $mapping = new Mapping();
        $mapping->setType($type);

        // Set mapping
        $mapping->setProperties($properties);
        $mapping->send();
    }

    /**
     * @param ProgressBar $progressBar
     * @param array[]     $processesQueue
     * @param int         $numberObjects          Total number of entities inside DB without limit/offset
     * @param int         $numberPopulatedObjects
     * @param int|null    $numberTransformedError
     * @param int|null    $numberDocumentEmpty
     *
     * @return int
     */
    private function runParallel(
        ProgressBar $progressBar,
        array &$processesQueue,
        int $numberObjects,
        ?int &$numberPopulatedObjects = 0,
        ?int &$numberTransformedError = 0,
        ?int &$numberDocumentEmpty = 0
    ) {
        // fix maxParallel to be max the number of processes or positive
        $maxParallel = min(abs($this->threads), count($processesQueue));
        // get the first stack of processes to start at the same time
        /** @var Process[] $runningProcesses */
        $runningProcesses = array_splice($processesQueue, 0, $maxParallel);

        // start the initial stack of processes
        foreach ($runningProcesses as $aProcess) {
            $aProcess[self::FIELD_PROCESS]->start();
        }

        $progression = $this->offset;
        $returnValue = self::EXIT_SUCCESS;

        do {
            // wait for the given time
            usleep(self::POLLING_INTERVAL);
            // remove all finished processes from the stack
            foreach ($runningProcesses as $index => $aProcess) {
                /** @var Process $process */
                $process = $aProcess[self::FIELD_PROCESS];
                $processLimit = $aProcess[self::FIELD_PROGRESSION];

                if ('' !== $process->getErrorOutput()) {
                    $this->write("<error>{$process->getErrorOutput()}</error>");
                    $process->clearErrorOutput();
                    $progressBar->display();
                }

                if (!$process->isRunning()) {
                    if (self::EXIT_SUCCESS != $process->getExitCode()) {
                        $this->writeln('<error>'.$process->getErrorOutput().'</error>');
                        $returnValue = self::EXIT_FAILED;
                    }

                    if ($result = $process->getOutput()) {
                        sscanf($result, self::RESULT, $procNbrPopulated, $procNbrError, $procNbrEmpty);
                        $numberPopulatedObjects += $procNbrPopulated;
                        $numberTransformedError += $procNbrError;
                        $numberDocumentEmpty += $procNbrEmpty;
                    } else {
                        $numberPopulatedObjects += (int) $processLimit;
                    }

                    unset($runningProcesses[$index]);

                    // directly add and start new process after the previous finished
                    if (count($processesQueue) > 0) {
                        $nextProcess = array_shift($processesQueue);
                        $nextProcess[self::FIELD_PROCESS]->start();

                        $runningProcesses[] = $nextProcess;
                    }

                    $progression += (int) $processLimit;

                    $message = $this->type.":\t <comment>[POPULATING]</comment> $progression/$numberObjects ";
                    if ($this->verbose) {
                        $this->isMemoryFull();
                        $message .= '('.count($runningProcesses).' threads / '.$this->debugMemoryUsage.' Mo)';
                    }
                    $progressBar->setMessage($message);
                    $progressBar->advance($processLimit);
                }
            }

            // continue loop while there are processes being executed or waiting for execution
        } while (count($processesQueue) > 0 || count($runningProcesses) > 0);

        $progressBar->display();
        $progressBar->finish();

        return $returnValue;
    }

    /**
     * @param int $numberObjects
     * @param int $offset
     * @param int $limit
     *
     * @return int
     */
    private static function getNumberEntitiesToPopulate(int $numberObjects, int $offset, int $limit): int
    {
        $nbr = $numberObjects - $offset;

        if ($limit <= 0) {
            return $nbr;
        }

        return ($nbr > $limit)
            ? $limit
            : $nbr;
    }

    /**
     * @param int      $numberPopulatedObjects
     * @param int|null $numberTransformedError
     * @param int|null $numberDocumentEmpty
     *
     * @return int
     *
     * @throws ReflectionException
     */
    private function createBatches(?int &$numberPopulatedObjects = 0, ?int &$numberTransformedError = 0, ?int &$numberDocumentEmpty = 0)
    {
        try {
            $numberObjects = $this->getProvider($this->type)->getCount($this->conditions);
        } catch (ImplementationException $e) {
            $this->writeln('<error>'.$e->getMessage().'</error>');

            return 0;
        }

        $aProcess = [];
        $numberOfEntities = self::getNumberEntitiesToPopulate($numberObjects, $this->offset, $this->limit);
        $numberOfProcess = ceil($numberOfEntities / $this->batch);
        $sOptions = $this->getOptionsToString(['type', 'limit', 'offset', 'threads', 'batch', 'reset']);
        $progressBar = $this->getProgressBar($this->output, $numberOfEntities);

        for ($i = 0; $i < $numberOfProcess; ++$i) {
            $_offset = $this->offset + ($this->batch * $i);
            $stillToPopulate = $numberOfEntities - ($this->batch * $i);
            $processLimit = min($this->batch, $stillToPopulate);
            $commandLine = "php bin/console elastic:populate --quiet --type=\"{$this->type}\" --limit={$processLimit} --offset={$_offset} ".$sOptions;

            $aProcess[] = [
                self::FIELD_PROCESS => new Process($commandLine),
                self::FIELD_PROGRESSION => $processLimit,
            ];
        }

        return $this->runParallel($progressBar, $aProcess, $numberObjects, $numberPopulatedObjects, $numberTransformedError, $numberDocumentEmpty);
    }

    /**
     * @return Type
     */
    private function createIndex()
    {
        $this->write($this->type.":\t <comment>[CREATING]</comment>");

        $namespace = explode('\\', $this->type::getIndexType());
        $objectType = $this->getIndexFromType($this->type::getIndex())->getType(end($namespace));

        /** @var AbstractElasticSearchIndex $index */
        $index = $this->getContainer()->get('bundle.elasticsearch.factory.index')->create($this->type::getIndex());
        $this->_mappingFields($objectType, $index->getMapping());

        $this->writeln($this->type.":\t <info>[CREATED]</info>");

        return $objectType;
    }

    /**
     * @param AbstractTransformer $transformer
     * @param int $numberPopulatedObjects
     * @param int|null $numberTransformedError
     * @param int|null $numberDocumentEmpty
     *
     * @throws ReflectionException
     */
    private function processBatch($transformer, ?int &$numberPopulatedObjects = 0, ?int &$numberTransformedError = 0, ?int &$numberDocumentEmpty = 0)
    {
        $objectType = $this->createIndex();

        try {
            $q = $this->getProvider($this->type)->getQuery($this->conditions);
            $iResults = $this->getProvider($this->type)->getCount($this->conditions);
        } catch (ImplementationException $e) {
            $this->writeln('<error>'.$e->getMessage().'</error>');

            return;
        }

        if ($this->offset) {
            $q->setFirstResult($this->offset);
            $iResults = $iResults - $this->offset;
        }

        if ($this->limit) {
            $q->setMaxResults($this->limit);
        }

        $iterableResult = $q->iterate();

        $progressBar = $this->getProgressBar($this->output, (0 == $this->limit) ? $iResults : $this->limit);
        $progressBar->setMessage($this->type.":\t <comment>[RUNNING]</comment>"); //$message);

        $progression = $this->offset;
        $progressMax = $iResults + $this->offset;

        $aDocuments = [];

        foreach ($iterableResult as $row) {
            $entity = $row[0];
            try {
                $documents = $transformer->transform($entity);
                ++$numberPopulatedObjects;
            } catch (EntityNotFoundException $e) {
                $this->output->getErrorOutput()->writeln(self::CLEAR_LINE.'<error>'.$e->getMessage().'</error>', OutputInterface::VERBOSITY_QUIET);
                ++$numberTransformedError;
                continue;
            }

            $progressBar->setMessage($this->type.":\t <comment>[POPULATING]</comment> ".(++$progression)."/{$progressMax} ({$this->debugMemoryUsage} Mo)");
            $progressBar->advance();

            if (!$documents) {
                $this->output->getErrorOutput()->writeln(self::CLEAR_LINE.'<error>Document is empty: '.json_encode($entity).'</error>', OutputInterface::VERBOSITY_QUIET);
                ++$numberDocumentEmpty;
                continue;
            }

            $aDocuments[] = $documents;
            // $this->entityManager->detach($entity);
            unset($entity);

            if ($this->isMemoryFull()) {
                $this->flushDocuments($objectType, $aDocuments, true);
            }
        }

        try {
            $progressBar->setProgress($iResults);
        } catch (LogicException $e) {
            // You can't regress the progress bar.
        }

        $progressBar->display();
        $progressBar->finish();

        $this->flushDocuments($objectType, $aDocuments);

        $this->write($this->type.":\t  <info>[COMPLETE]</info>");

        if ($this->quiet) {
            $this->output->writeln(
                printf(self::RESULT, $numberPopulatedObjects, $numberTransformedError, $numberDocumentEmpty)
            );
        }
    }

    /**
     * @param Type             $objectType
     * @param array|Document[] $aDocuments
     * @param bool             $memoryLimited
     */
    private function flushDocuments(Type $objectType, &$aDocuments, bool $memoryLimited = false)
    {
        $nbrDocument = count($aDocuments);
        $objectType->getIndex()->refresh();

        if ($nbrDocument) {
            $objectType->addDocuments($aDocuments);
            $objectType->getIndex()->refresh();
        }

        if ($memoryLimited) {
            $this->output->getErrorOutput()->writeln(
                '<error>Get memory limit / Flush '.$nbrDocument.' documents (PID:'.getmypid().')</error>',
                OutputInterface::VERBOSITY_QUIET
            );
        }

        foreach ($aDocuments as $document) {
            unset($document);
        }
        $aDocuments = [];
    }

    /**
     * @param string|ElasticSearchEntityInterface $type
     *
     * @return bool
     */
    private function _resetType(string $type)
    {
        $this->write($this->type.":\t <comment>[RESETTING]</comment>");

        /** @var AbstractElasticSearchIndex $index */
        $indexFactory = $this->getContainer()->get('bundle.elasticsearch.factory.index');
        $index = $indexFactory->create($type::getIndex());
        $connection = $index->getConnection();

        $index_name = $this->getContainer()->get('elasticsearch.handler')->getIndexNameFromType($type::getIndex());

        $response = $this->elasticSearchHelper->getClient($connection)->getIndex($index_name)->create(
            $index->getIndex(),
            true
        );

        if ($response->hasError()) {
            $this->writeln("Cannot reset index '$type': ".$response->getErrorMessage());

            return false;
        }

        $this->writeln($this->type.":\t <info>[RESET]</info>");

        return true;
    }

}
