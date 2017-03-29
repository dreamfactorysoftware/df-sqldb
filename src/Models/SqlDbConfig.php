<?php
namespace DreamFactory\Core\SqlDb\Models;

use DreamFactory\Core\Exceptions\BadRequestException;

/**
 * SqlDbConfig
 *
 */
class SqlDbConfig extends BaseSqlDbConfig
{
    protected $encrypted = ['username', 'password'];

    protected $protected = ['password'];

    public static function getSchema()
    {
        $defaults = [
            'host' => [
                'name'        => 'host',
                'label'       => 'Host',
                'type'        => 'string',
                'description' => 'The name of the database host, i.e. localhost, 192.168.1.1, etc.'
            ],
            'port' => [
                'name'        => 'port',
                'label'       => 'Port Number',
                'type'        => 'integer',
                'description' => 'The number of the database host port, i.e. ' . static::getDefaultPort()
            ],
            'database' => [
                'name'        => 'database',
                'label'       => 'Database',
                'type'        => 'string',
                'description' =>
                    'The name of the database to connect to on the given server. This can be a lookup key.'
            ],
            'username' => [
                'name'        => 'username',
                'label'       => 'Username',
                'type'        => 'string',
                'description' => 'The name of the database user. This can be a lookup key.'
            ],
            'password' => [
                'name'        => 'password',
                'label'       => 'Password',
                'type'        => 'password',
                'description' => 'The password for the database user. This can be a lookup key.'
            ],
            'schema' => [
                'name'        => 'schema',
                'label'       => 'Schema',
                'type'        => 'string',
                'description' => 'Leave blank to work with all available schemas, ' .
                    'type "default" to only work with the default schema for the given credentials, ' .
                    'or type in a specific schema to use for this service.'
            ],
//            'prefix' => [
//                'name'        => 'prefix',
//                'label'       => 'Table Prefix',
//                'type'        => 'string',
//                'description' => 'The name of the database table prefix.'
//            ],
        ];

        return array_merge($defaults, parent::getSchema());
    }

    public function validate(array $data, $throwException = true)
    {
        if (empty(array_get($data, 'host')) || empty(array_get($data, 'database'))) {
            throw new BadRequestException("Database connection information must contain at least host and database name.");
        }

        return parent::validate($data);
    }
}