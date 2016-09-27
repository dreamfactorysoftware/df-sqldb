<?php
namespace DreamFactory\Core\SqlDb\Models;

use DreamFactory\Core\Exceptions\BadRequestException;

/**
 * SqliteDbConfig
 *
 */
class SqliteDbConfig extends BaseSqlDbConfig
{
    protected $appends = ['database'];

    protected function getConnectionFields()
    {
        return ['database'];
    }

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
        if (empty($db = array_get($config, 'database'))) {
            throw new BadRequestException('Database name must be provided.');
        }

        return true;
    }
}