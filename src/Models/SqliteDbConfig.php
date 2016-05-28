<?php
namespace DreamFactory\Core\SqlDb\Models;

use DreamFactory\Core\Exceptions\BadRequestException;

/**
 * SqliteDbConfig
 *
 */
class SqliteDbConfig extends SqlDbConfig
{
    public static function getDriverName()
    {
        return 'sqlite';
    }

    public static function getDefaultPort()
    {
        return null;
    }

    public static function getDefaultConnectionInfo()
    {
        $defaults = [
            [
                'name'        => 'database',
                'label'       => 'Database',
                'type'        => 'string',
                'description' =>
                    'The name or path of the database to connect to. This can be a lookup key.'
            ],
        ];

        return $defaults;
    }

    public static function validateConfig($config, $create = true)
    {
        $db = isset($config['database']) ? $config['database'] : null;
        if (empty($db)) {
            throw new BadRequestException('Database name must be provided.');
        }

        return true;
    }
}