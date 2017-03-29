<?php
namespace DreamFactory\Core\SqlDb\Models;

use DreamFactory\Core\Exceptions\BadRequestException;

/**
 * SqliteDbConfig
 *
 */
class SqliteDbConfig extends BaseSqlDbConfig
{
    public static function getDriverName()
    {
        return 'sqlite';
    }

    public static function getDefaultPort()
    {
        return null;
    }

    public static function getSchema()
    {
        $extras = [
            [
                'name'        => 'database',
                'label'       => 'Database',
                'type'        => 'string',
                'description' => 'The name or path of the database to connect to. This can be a lookup key.'
            ],
        ];

        return array_merge($extras, parent::getSchema());
    }

    public static function validateConfig($config, $create = true)
    {
        if (empty($db = array_get($config, 'database'))) {
            throw new BadRequestException('Database name must be provided.');
        }

        return true;
    }
}