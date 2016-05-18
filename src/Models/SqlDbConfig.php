<?php
namespace DreamFactory\Core\SqlDb\Models;

use DreamFactory\Core\Components\RequireExtensions;
use DreamFactory\Core\Database\Schema\ColumnSchema;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Models\BaseServiceConfigModel;
use DreamFactory\Core\Models\ServiceCacheConfig;
use Crypt;

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
class SqlDbConfig extends BaseServiceConfigModel
{
    use RequireExtensions;

    protected $table = 'sql_db_config';

    protected $fillable = [
        'service_id',
        'connection',
        'options',
        'attributes',
        'statements',
    ];

    // deprecated, service has type designation now
    protected $hidden = ['driver', 'dsn', 'username', 'password', 'default_schema_only'];

    protected $casts = [
        'service_id' => 'integer',
        'connection' => 'array',
        'options'    => 'array',
        'attributes' => 'array',
        'statements' => 'array',
    ];

    public static function getDriverName()
    {
        return 'Unknown';
    }

    public static function getDefaultPort()
    {
        return 1234;
    }

    public static function getDefaultCharset()
    {
        return 'utf8';
    }

    public static function getDefaultCollation()
    {
        return 'utf8_unicode_ci';
    }

    /**
     * @param string $driver
     *
     * @return bool Returns true if all required extensions are loaded, otherwise an exception is thrown
     * @throws \Exception
     */
    public static function requireDriver($driver)
    {
        if (empty($driver)) {
            throw new \Exception("Database driver name can not be empty.");
        }

        // clients now use indirect association to drivers, dblib can be sqlsrv or sqlanywhere
        if ('dblib' === $driver) {
            $driver = 'sqlsrv';
        }
        $pdoDriver = null;
        switch ($driver) {
            case 'ibm':
                if (!extension_loaded('ibm_db2')) {
                    throw new \Exception("Required extension 'ibm_db2' is not detected, but may be compiled in.");
                }
                $pdoDriver = 'ibm';
                break;
            case 'mysql':
                if (!extension_loaded('mysql') && !extension_loaded('mysqlnd')) {
                    throw new \Exception("Required extension 'mysql' is not detected, but may be compiled in.");
                }
                $pdoDriver = 'mysql';
                break;
            case 'oracle':
                if (!extension_loaded('oci8')) {
                    throw new \Exception("Required extension 'oci8' is not detected, but may be compiled in.");
                }
                break;
            case 'pgsql':
                if (!extension_loaded('pgsql')) {
                    throw new \Exception("Required extension 'pgsql' is not detected, but may be compiled in.");
                }
                $pdoDriver = 'pgsql';
                break;
            case 'sqlanywhere':
                $extension = 'mssql';
                if (!extension_loaded($extension)) {
                    throw new \Exception("Required extension 'mssql' is not detected, but may be compiled in.");
                }
                $pdoDriver = 'dblib';
                break;
            case 'sqlite':
                if (!extension_loaded('sqlite3')) {
                    throw new \Exception("Required extension 'sqlite3' is not detected, but may be compiled in.");
                }
                $pdoDriver = 'sqlite';
                break;
            case 'sqlsrv':
                if (substr(PHP_OS, 0, 3) == 'WIN') {
                    $pdoDriver = 'sqlsrv';
                    $extension = 'sqlsrv';
                } else {
                    $pdoDriver = 'dblib';
                    $extension = 'mssql';
                }

                if (!extension_loaded($extension)) {
                    throw new \Exception("Required extension '$extension' is not detected, but may be compiled in.");
                }
                break;
            default:
                throw new \Exception("Driver '$driver' is not supported by this software.");
                break;
        }

        if ($pdoDriver) {
            static::checkForPdoDriver($pdoDriver);
        }

        return true;
    }

    /**
     * @param string $driver
     *
     * @throws \Exception
     */
    public static function checkForPdoDriver($driver)
    {
        if (!extension_loaded('PDO')) {
            throw new \Exception("Required PDO extension is not installed or loaded.");
        }

        // see overrides for specific driver checks
        $drivers = \PDO::getAvailableDrivers();
        if (!in_array($driver, $drivers)) {
            throw new \Exception("Required PDO driver '$driver' is not installed or loaded properly.");
        }
    }

    public static function getDefaultConnectionInfo()
    {
        $defaults = [
            [
                'name'        => 'host',
                'label'       => 'Host',
                'type'        => 'string',
                'description' => 'The name of the database host, i.e. localhost, 192.168.1.1, etc..'
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

    /**
     * @param int $id
     *
     * @return array
     */
    public static function getConfig($id)
    {
        $config = parent::getConfig($id);
        // merge up the connection parameters
        $connection = array_get($config, 'connection', []);
        unset($config['connection']);
        if (!empty($username = array_get($connection, 'username'))) {
            $connection['username'] = Crypt::decrypt($username);
        }
        if (!empty($password = array_get($connection, 'password'))) {
            $connection['password'] = Crypt::decrypt($password);
        }
        $config = array_merge($config, $connection);

        $cacheConfig = ServiceCacheConfig::whereServiceId($id)->first();
        $config['cache_enabled'] = (empty($cacheConfig)) ? false : $cacheConfig->getAttribute('cache_enabled');
        $config['cache_ttl'] = (empty($cacheConfig)) ? 0 : $cacheConfig->getAttribute('cache_ttl');

        return $config;
    }

    public static function validateConfig($config, $create = true)
    {
        $host = isset($config['host']) ? $config['host'] : null;
        $db = isset($config['database']) ? $config['database'] : null;
        if (empty($host) || empty($db)) {
            throw new BadRequestException('Database connection information must contain host and database name must be provided.');
        }

        return true;
    }

    /**
     * {@inheritdoc}
     */
    public static function setConfig($id, $config)
    {
        $cache = [];
        if (isset($config['cache_enabled'])) {
            $cache['cache_enabled'] = $config['cache_enabled'];
            unset($config['cache_enabled']);
        }
        if (isset($config['cache_ttl'])) {
            $cache['cache_ttl'] = $config['cache_ttl'];
            unset($config['cache_ttl']);
        }
        if (!empty($cache)) {
            ServiceCacheConfig::setConfig($id, $cache);
        }

        $dbConfig =
            [
                'options'    => array_get($config, 'options'),
                'attributes' => array_get($config, 'attributes'),
                'statements' => array_get($config, 'statements')
            ];
        unset($config['service_id']);
        unset($config['options']);
        unset($config['attributes']);
        unset($config['statements']);
        if (!empty($username = array_get($config, 'username'))) {
            $config['username'] = Crypt::encrypt($username);
        }
        if (!empty($password = array_get($config, 'password'))) {
            $config['password'] = Crypt::encrypt($password);
        }
        $dbConfig['connection'] = $config;

        parent::setConfig($id, $dbConfig);
    }

    /**
     * {@inheritdoc}
     */
    public static function getConfigSchema()
    {
        $model = new static;

        $schema = $model->getTableSchema();
        if ($schema) {
            $out = [];
            foreach ($schema->columns as $name => $column) {
                // Skip if column is hidden
                if (in_array($name, $model->getHidden())) {
                    continue;
                }
                /** @var ColumnSchema $column */
                if (('service_id' === $name) || $column->autoIncrement) {
                    continue;
                }

                if ('connection' === $name) {
                    // specific attributes to the different databases
                    $temp = static::getDefaultConnectionInfo();
                    $out = array_merge($out, $temp);
                } else {
                    $temp = $column->toArray();
                    static::prepareConfigSchemaField($temp);
                    $out[] = $temp;
                }
            }

            return $out;
        }

        return null;
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