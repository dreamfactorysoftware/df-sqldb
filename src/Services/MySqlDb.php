<?php

namespace DreamFactory\Core\SqlDb\Services;

/**
 * Class MySqlDb
 *
 * @package DreamFactory\Core\SqlDb\Services
 */
class MySqlDb extends SqlDb
{
    public function __construct($settings = [])
    {
        parent::__construct($settings);

        $prefix = parent::getConfigBasedCachePrefix();
        if ($socket = array_get($this->config, 'unix_socket')) {
            $prefix = $socket . $prefix;
        }
        $this->setConfigBasedCachePrefix($prefix);
    }

    public static function adaptConfig(array &$config)
    {
        $config['driver'] = 'mysql';
        parent::adaptConfig($config);
    }
}