<?php

namespace DreamFactory\Core\SqlDb\Services;

use DreamFactory\Core\SqlDb\Resources\StoredFunction;
use DreamFactory\Core\SqlDb\Resources\StoredProcedure;

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

    public static function getDriverName()
    {
        return 'mysql';
    }

    public function getResourceHandlers()
    {
        $handlers = parent::getResourceHandlers();

        $handlers[StoredProcedure::RESOURCE_NAME] = [
            'name'       => StoredProcedure::RESOURCE_NAME,
            'class_name' => StoredProcedure::class,
            'label'      => 'Stored Procedure',
        ];
        $handlers[StoredFunction::RESOURCE_NAME] = [
            'name'       => StoredFunction::RESOURCE_NAME,
            'class_name' => StoredFunction::class,
            'label'      => 'Stored Function',
        ];

        return $handlers;
    }
}