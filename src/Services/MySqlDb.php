<?php

namespace DreamFactory\Core\SqlDb\Services;

/**
 * Class MySqlDb
 *
 * @package DreamFactory\Core\SqlDb\Services
 */
class MySqlDb extends SqlDb
{
    public static function adaptConfig(array &$config)
    {
        $config['driver'] = 'mysql';
        parent::adaptConfig($config);
    }
}