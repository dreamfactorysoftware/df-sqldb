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