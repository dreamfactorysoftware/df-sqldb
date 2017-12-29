<?php

namespace DreamFactory\Core\SqlDb\Services;

/**
 * Class SqliteDb
 *
 * @package DreamFactory\Core\SqlDb\Services
 */
class SqliteDb extends SqlDb
{
    /**
     * SqliteDb constructor.
     * @param array $settings
     * @throws \Exception
     */
    public function __construct($settings = [])
    {
        parent::__construct($settings);

        $this->setConfigBasedCachePrefix(array_get($this->config, 'database') . ':');
    }

    public static function adaptConfig(array &$config)
    {
        $config['driver'] = 'sqlite';
        parent::adaptConfig($config);
    }

    protected function initStatements($statements = [])
    {
        if (is_string($statements)) {
            $statements = [$statements];
        } elseif (!is_array($statements)) {
            $statements = [];
        }

        array_unshift($statements, 'PRAGMA foreign_keys=1');
        foreach ($statements as $statement) {
            $this->dbConn->statement($statement);
        }
    }
}