<?php

namespace DreamFactory\Core\SqlDb\Services;

use DreamFactory\Core\SqlDb\Resources\Schema;
use DreamFactory\Core\SqlDb\Resources\Table;

/**
 * Class SqliteDb
 *
 * @package DreamFactory\Core\SqlDb\Services
 */
class SqliteDb extends SqlDb
{
    /**
     * @var array
     */
    protected static $resources = [
        Schema::RESOURCE_NAME => [
            'name'       => Schema::RESOURCE_NAME,
            'class_name' => Schema::class,
            'label'      => 'Schema',
        ],
        Table::RESOURCE_NAME  => [
            'name'       => Table::RESOURCE_NAME,
            'class_name' => Table::class,
            'label'      => 'Tables',
        ],
    ];

    //*************************************************************************
    //	Methods
    //*************************************************************************

    public static function adaptConfig(array &$config)
    {
        $config['driver'] = 'sqlite';
        $dsn = isset($config['dsn']) ? $config['dsn'] : null;
        if (!empty($dsn)) {
            // default PDO DSN pieces
            $dsn = str_replace(' ', '', $dsn);
            if (!isset($config['database'])) {
                $file = substr($dsn, 7);
                $config['database'] = $file;
            }
        }

        if (!isset($config['collation'])) {
            $config['collation'] = 'utf8_unicode_ci';
        }

        // must be there
        if (!array_key_exists('database', $config)) {
            $config['database'] = null;
        }

        // must be there
        if (!array_key_exists('prefix', $config)) {
            $config['prefix'] = null;
        }

        // laravel database config requires options to be [], not null
        if (array_key_exists('options', $config) && is_null($config['options'])) {
            $config['options'] = [];
        }
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