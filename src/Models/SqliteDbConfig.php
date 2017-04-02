<?php
namespace DreamFactory\Core\SqlDb\Models;

/**
 * SqliteDbConfig
 *
 */
class SqliteDbConfig extends BaseSqlDbConfig
{
    protected $appends = ['database'];

    protected $rules = ['database' => 'required'];

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
                'description' => 'The name or path of the database to connect to. This can be a lookup key.'
            ],
        ];

        return $defaults;
    }
}