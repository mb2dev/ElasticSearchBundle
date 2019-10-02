<?php

namespace ElasticSearchBundle\Command;

use DateTime;
use Doctrine\Common\Persistence\Mapping\MappingException;
use Doctrine\ORM\OptimisticLockException;
use Doctrine\ORM\ORMException;
use ElasticSearchBundle\Helper\ElasticSearchHelper;
use Doctrine\ORM\EntityManager;
use Elastica\Client;
use Elastica\Index;
use ElasticSearchBundle\Index\ElasticSearchEntityInterface;
use ElasticSearchBundle\Index\ElasticSearchIndexInterface;
use ElasticSearchBundle\Providers\AbstractProvider;
use Exception;
use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;
use Symfony\Component\Console\Helper\ProgressBar;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

/**
 * Class AbstractCommand.
 */
abstract class AbstractCommand extends ContainerAwareCommand
{
    const LINE_LENGTH = 70;
    const CLEAR_LINE = "\r\e[2K\r";

    const EXIT_SUCCESS = 0;
    const EXIT_FAILED = 127;

    /** @var OutputInterface */
    protected $output;
    /** @var InputInterface */
    protected $input;
    /** @var false|int */
    protected $threads = false;
    /** @var int */
    protected $limit = null;
    /** @var int */
    protected $offset = 0;
    /** @var false|int */
    protected $batch = false;
    /** @var string */
    protected $type;
    /** @var ElasticSearchHelper */
    protected $elasticSearchHelper;
    /** @var array */
    protected $mappings;
    /** @var EntityManager */
    protected $entityManager;
    /** @var bool */
    protected $reset = false;
    /** @var bool $verbose More verbose */
    protected $verbose = false;
    /** @var bool $dryRun Do not make any change on ES */
    protected $dryRun = false;
    /** @var bool $quiet */
    protected $quiet = false;
    /** @var int $id Option ID: use with Where */
    protected $id = null;
    /** @var string $where Option Where: use with ID */
    protected $where = null;
    /** @var int From max _id */
    protected $maxId = 0;
    /** @var string From this date */
    protected $from = null;
    /** @var string To this date */
    protected $to = null;
    /** @var string Field used in DB to filter with date (From/To) */
    protected $dateField = 'updatedAt';
    protected $conditions = [];

    /** @var int Limit of memory per process in Octets */
    protected $memoryLimit = -1;
    /** @var float Percent of memory before flushing document */
    protected $memoryLimitPercent = 0.97;
    /** @var int */
    protected $debugMemoryUsage = 0;

    /** @var string */
    protected $environment;

    /**
     * @param InputInterface $input
     * @param OutputInterface $output
     *
     * @throws MappingException
     * @throws ORMException
     * @throws OptimisticLockException
     * @throws Exception
     */
    protected function init(InputInterface $input, OutputInterface $output)
    {
        $this->output = $output;
        $this->input = $input;

        $this->elasticSearchHelper = $this->getContainer()->get('elasticsearch.helper');
        $this->mappings = $this->getContainer()->getParameter('elastica_mappings');

        $this->entityManager = $this->getContainer()->get('doctrine.orm.entity_manager');
        $this->environment = $this->getContainer()->get('kernel')->getEnvironment();

        // $this->aTypes = array_keys($this->mappings);

        $this->readOptions($input);
        $this->checkOptions();

        if ('prod' == $this->environment) {
            $this->entityManager->getConnection()->getConfiguration()->setSQLLogger(null);
            $this->entityManager->flush();
            $this->entityManager->clear();
        }

        $this->convertMemoryLimit();
    }

    protected function convertMemoryLimit()
    {
        $memoryLimit = ini_get('memory_limit');
        if (preg_match('/^(\d+)(.)$/', $memoryLimit, $matches)) {
            switch (strtoupper($matches[2])) {
                case 'G':
                    $this->memoryLimit = $matches[1] * 1024 * 1024 * 1024; // GB
                    return;
                case 'M':
                    $this->memoryLimit = $matches[1] * 1024 * 1024; // MB
                    return;
                case 'K':
                    $this->memoryLimit = $matches[1] * 1024; // KB
                    return;
            }
        }
    }

    /**
     * @param $type: Check if the given type is set into the mapping. Otherwise, search for the near type.
     *
     * @return bool
     */
    protected function checkType($type)
    {
        if (in_array($type, $this->mappings)) {
            return true;
        }

        $this->output->writeln('<error>Cannot find type:</error> <comment>'.$type.'</comment>');
        $this->output->writeln('Did you mean: ');

        foreach ($this->mappings as $mapping) {
            if (false !== stripos($mapping, $type)) {
                $this->output->writeln("<comment>$mapping</comment>");
            }
        }

        return false;
    }

    /**
     * @param array $excludedOption
     *
     * @return string
     */
    protected function getOptionsToString($excludedOption = [])
    {
        $aOptions = $this->input->getOptions();
        $sOptions = '';

        foreach ($aOptions as $key => $value) {
            if (false === $value || in_array($key, $excludedOption)) {
                continue;
            }

            if (true === $value) {
                $sOptions .= " --$key";
                continue;
            }

            $sOptions .= (is_string($value))
                ? " --$key='$value'"
                : " --$key=$value";
        }

        return $sOptions;
    }

    /**
     * @param InputInterface $input
     */
    protected function readOptions(InputInterface $input)
    {
        $this->type = $input->getOption('type');
        $this->batch = (int) $input->getOption('batch') ?: $this->batch;

        if ($input->hasOption('threads') && null !== $input->getOption('threads')) {
            $this->threads = (int) $input->getOption('threads');
        }

        if ($input->hasOption('limit')) {
            $this->limit = (int) $input->getOption('limit');
        }

        if ($input->hasOption('offset')) {
            $this->offset = (int) $input->getOption('offset');
        }

        if ($input->hasOption('reset')) {
            $this->reset = $input->getOption('reset');
        }

        if ($input->hasOption('dry-run')) {
            $this->dryRun = $input->getOption('dry-run');
        }

        if ($input->hasOption('verbose')) {
            $this->verbose = $input->getOption('verbose');
        }

        if ($input->hasOption('quiet')) {
            $this->quiet = $input->getOption('quiet');
        }

        if ($input->hasOption('id')) {
            $this->id = $input->getOption('id');
        }

        if ($input->hasOption('where')) {
            $this->where = $input->getOption('where');
        }

        if ($input->hasOption('max-id')) {
            $this->maxId = $input->getOption('max-id');
        }

        if ($input->hasOption('from')) {
            $this->from = $input->getOption('from');
        }

        if ($input->hasOption('to')) {
            $this->to = $input->getOption('to');
        }

        if ($input->hasOption('created') && $input->getOption('created')) {
            $this->dateField = 'createdAt';
        }

        if ($input->hasOption('updated') && $input->getOption('updated')) {
            $this->dateField = 'updatedAt';
        }
    }

    /**
     * @throws Exception
     */
    protected function checkOptions()
    {
        if (!empty($this->id) && !empty($this->where)) {
            $this->conditions[] = [$this->where, $this->id];
        }

        if ((empty($this->id) && !empty($this->where))
            || (!empty($this->id) && empty($this->where))
        ) {
            $this->output->writeln('<error>Options Id/Where must be used together</error>');

            exit(self::EXIT_FAILED);
        }

        if (false == $this->threads && false !== $this->batch) {
            $this->threads = 2;
        }

        if (false !== $this->threads && false == $this->batch) {
            $this->batch = 100;
        }

        if (!empty($this->from)) {
            $from = new DateTime($this->from);
            $this->conditions[] = [$this->dateField, "'$this->from'", ' >= '];
        }

        if (!empty($this->to)) {
            $to = new DateTime($this->to);
            $this->conditions[] = [$this->dateField, "'$this->to'", ' <= '];
        }

        if (isset($from) && isset($to) && (1 === $from->diff($to)->invert)) {
            $this->output->writeln('<error>The date `from` is latter  date `to`</error>');

            exit(self::EXIT_FAILED);
        }

        if ($this->reset && !empty($this->conditions)) {
            $this->output->writeln('<error>Options `Id/Where/From/To` cannot be used with option `reset`</error>');

            exit(self::EXIT_FAILED);
        }
    }

    /**
     * @param OutputInterface $output
     * @param int             $max
     *
     * @return ProgressBar
     */
    protected function getProgressBar($output, $max)
    {
        $max = ($max > 0) ? $max : 1;
        $progressBar = new ProgressBar($output, $max);

        $sFormat = ($this->verbose)
            ? '%message% Doc. %percent%% [%bar%] (%elapsed% - %remaining%) (%memory%)'."\r"
            : '%message% %percent%% [%bar%] (%elapsed% - %remaining%)'."\r";

        $progressBar->setFormat($sFormat);
        $progressBar->setMessage('');
        $progressBar->start();

        return $progressBar;
    }

    /**
     * @param $msg
     *
     * @return string
     */
    public static function completeLine($msg)
    {
        $nbrAstrix = (self::LINE_LENGTH - strlen($msg) - 4) / 2;

        if ($nbrAstrix <= 0) {
            return $msg;
        }

        $sAstrix = str_repeat('*', $nbrAstrix);
        $sReturn = "$sAstrix  $msg  $sAstrix";

        return (self::LINE_LENGTH == strlen($sReturn))
            ? self::CLEAR_LINE.$sReturn
            : self::CLEAR_LINE.$sReturn.'*';
    }

    /**
     * @param bool $forceDisplayMemory
     *
     * @return bool
     */
    protected function isMemoryFull(bool $forceDisplayMemory = false)
    {
        $memoryUsage = memory_get_usage();
        $percentUsed = ($memoryUsage / $this->memoryLimit);
        $isMemoryFull = $percentUsed > $this->memoryLimitPercent;

        $this->debugMemoryUsage = round($memoryUsage / 1024 / 1024);

        if ($isMemoryFull || $forceDisplayMemory) {
            $this->output->write("\n");
            $this->output->writeln(
                $this::CLEAR_LINE.
                '<info>MemUsage:</info> '.round($memoryUsage / 1024 / 1024, 2).' Mo '.
                '<info>MemLimit:</info> '.round($this->memoryLimit / 1024 / 1024).' Mo '.
                '<info>MemPercent:</info> '.round($percentUsed * 100, 1).' % '.
                '<info>MemLimitPercent:</info> '.($this->memoryLimitPercent * 100).' % '
            );
        }

        return (-1 === $this->memoryLimit)
            ? false // Not memory limit
            : $isMemoryFull;
    }

    /**
     * @param string|ElasticSearchIndexInterface $indexClass
     *
     * @return Index
     */
    protected function getIndexFromType(string $indexClass): Index
    {
        $indexName = $this->getContainer()
            ->get('elasticsearch.handler')
            ->getIndexNameFromType($indexClass);

        return $this->getClient($indexClass)->getIndex($indexName);
    }

    /**
     * @param string|ElasticSearchIndexInterface $type
     *
     * @return Client
     */
    protected function getClient(string $type): Client
    {
        $indexFactory = $this->getContainer()->get('bundle.elasticsearch.factory.index');

        return $this->elasticSearchHelper->getClient($indexFactory->create($type)->getConnection());
    }

    /**
     * @param string|ElasticSearchEntityInterface $type
     *
     * @return AbstractProvider
     */
    protected function getProvider(string $type): AbstractProvider
    {
        $p = $type::getProvider();

        return new $p($this->entityManager);
    }

    /**
     * @param string $message
     * @param int    $options
     */
    protected function write(string $message = '', int $options = 0)
    {
        $this->output->write("\r\e[2K$message", $options);
    }

    /**
     * @param string $message
     * @param int    $options
     */
    protected function writeln(string $message = '', int $options = 0)
    {
        $this->output->writeln("\r\e[2K$message", $options);
    }
}
