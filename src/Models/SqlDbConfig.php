<?php
namespace DreamFactory\Core\SqlDb\Models;

use DreamFactory\Core\Exceptions\BadRequestException;

/**
 * SqlDbConfig
 *
 * @property integer $service_id
 * @property array   $connection
 * @property array   $options
 * @property array   $attributes
 * @property array   $statements
 */
class SqlDbConfig extends BaseSqlDbConfig
{
    protected $appends = ['host', 'port', 'database', 'username', 'password', 'schema'];

    protected $encrypted = ['username', 'password'];

    protected $protected = ['password'];

    protected function getConnectionFields()
    {
        return ['host', 'port', 'database', 'username', 'password', 'schema'];
    }

    public static function getDefaultConnectionInfo()
    {
        $defaults = [
            [
                'name'        => 'host',
                'label'       => 'Host',
                'type'        => 'string',
                'description' => 'The name of the database host, i.e. localhost, 192.168.1.1, etc.'
            ],
            [
                'name'        => 'port',
                'label'       => 'Port Number',
                'type'        => 'integer',
                'description' => 'The number of the database host port, i.e. ' . static::getDefaultPort()
            ],
            [
                'name'        => 'database',
                'label'       => 'Database',
                'type'        => 'string',
                'description' =>
                    'The name of the database to connect to on the given server. This can be a lookup key.'
            ],
            [
                'name'        => 'username',
                'label'       => 'Username',
                'type'        => 'string',
                'description' => 'The name of the database user. This can be a lookup key.'
            ],
            [
                'name'        => 'password',
                'label'       => 'Password',
                'type'        => 'password',
                'description' => 'The password for the database user. This can be a lookup key.'
            ],
            [
                'name'        => 'schema',
                'label'       => 'Schema',
                'type'        => 'string',
                'description' => 'Leave blank to work with all available schemas, ' .
                    'type "default" to only work with the default schema for the given credentials, ' .
                    'or type in a specific schema to use for this service.'
            ],
//            [
//                'name'        => 'prefix',
//                'label'       => 'Table Prefix',
//                'type'        => 'string',
//                'description' => 'The name of the database table prefix.'
//            ],
        ];

        return $defaults;
    }

    public function validate($data, $throwException = true)
    {
        $connection = $this->getAttribute('connection');
        if (empty(array_get($connection, 'host')) || empty(array_get($connection, 'database'))) {
            throw new BadRequestException("Database connection information must contain at least host and database name.");
        }

        return parent::validate($data, $throwException);
    }

    /**
     * @param array $schema
     */
    protected static function prepareConfigSchemaField(array &$schema)
    {
        parent::prepareConfigSchemaField($schema);

        switch ($schema['name']) {
            case 'options':
                $schema['label'] = 'Driver Options';
                $schema['type'] = 'object';
                $schema['object'] =
                    [
                        'key'   => ['label' => 'Name', 'type' => 'string'],
                        'value' => ['label' => 'Value', 'type' => 'string']
                    ];
                $schema['description'] = 'A key-value array of driver-specific connection options.';
                break;
            case 'attributes':
                $schema['label'] = 'Driver Attributes';
                $schema['type'] = 'object';
                $schema['object'] =
                    [
                        'key'   => ['label' => 'Name', 'type' => 'string'],
                        'value' => ['label' => 'Value', 'type' => 'string']
                    ];
                $schema['description'] =
                    'A key-value array of attributes to be set after connection.' .
                    ' For further information, see http://php.net/manual/en/pdo.setattribute.php';
                break;
            case 'statements':
                $schema['label'] = 'Additional SQL Statements';
                $schema['type'] = 'array';
                $schema['items'] = 'string';
                $schema['description'] = 'An array of SQL statements to run during connection initialization.';
                break;
        }
    }
}