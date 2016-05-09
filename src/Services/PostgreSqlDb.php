<?php

namespace DreamFactory\Core\SqlDb\Services;

/**
 * Class PostgreSqlDb
 *
 * @package DreamFactory\Core\SqlDb\Services
 */
class PostgreSqlDb extends SqlDb
{
    public static function adaptConfig(array &$config)
    {
        $config['driver'] = 'pgsql';
        parent::adaptConfig($config);
    }
}