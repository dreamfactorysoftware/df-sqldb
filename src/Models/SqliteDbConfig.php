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

    public function validate($data, $throwException = true)
    {
        $connection = $this->getAttribute('connection');
        if (empty(array_get($connection, 'database'))) {
            throw new BadRequestException("Database connection information must contain at least database name.");
        }

        return parent::validate($data, $throwException);
    }

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