<?php

namespace DreamFactory\Core\SqlDb\Services;

use DreamFactory\Core\SqlDb\Resources\StoredFunction;
use DreamFactory\Core\SqlDb\Resources\StoredProcedure;

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