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
 * @method static SqlDbConfig whereServiceId($value)
 */
class SqlDbConfig extends BaseSqlDbConfig
{
    protected $appends = ['host','port','database','username','password','default_schema_only'];

    protected $encrypted = ['username', 'password'];

    protected $protected = ['password'];

    protected function getConnectionFields()
    {
        return ['host','port','database','username','password','default_schema_only'];
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
//            [
//                'name'        => 'prefix',
//                'label'       => 'Table Prefix',
//                'type'        => 'string',
//                'description' => 'The name of the database table prefix.'
//            ],
//            [
//                'name'        => 'schema',
//                'label'       => 'Schema',
//                'type'        => 'string',
//                'description' => 'Do not include other schemas/databases on this server ' .
//                    'regardless of permissions given to the supplied credentials.'
//            ],
            [
                'name'        => 'default_schema_only',
                'label'       => 'Use Default Schema Only',
                'type'        => 'boolean',
                'description' => 'Do not include other schemas/databases on this server ' .
                    'regardless of permissions given to the supplied credentials.'
            ]
        ];

        return $defaults;
    }

    public static function validateConfig($config, $create = true)
    {
        $host = isset($config['host']) ? $config['host'] : null;
        $db = isset($config['database']) ? $config['database'] : null;
        if (empty($host) || empty($db)) {
            throw new BadRequestException("Database connection information must contain host and database name must be provided.");
        }

        return parent::validateConfig($config, $create);
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