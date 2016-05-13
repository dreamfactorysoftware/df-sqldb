<?php
namespace DreamFactory\Core\SqlDb\Models;

use DreamFactory\Core\Components\RequireExtensions;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Models\BaseServiceConfigModel;
use DreamFactory\Core\Models\ServiceCacheConfig;

/**
 * SqlDbConfig
 *
 * @property integer $service_id
 * @property string  $dsn
 * @property string  $host
 * @property integer $port
 * @property string  $database
 * @property string  $username
 * @property string  $password
 * @property string  $prefix
 * @property string  $options
 * @property string  $attributes
 * @method static SqlDbConfig whereServiceId($value)
 */
class SqlDbConfig extends BaseServiceConfigModel
{
    use RequireExtensions;

    protected $table = 'sql_db_config';

    protected $fillable = [
        'service_id',
        'dsn',
        'host',
        'port',
        'database',
        'username',
        'password',
        'prefix',
        'options',
        'attributes',
        'default_schema_only'
    ];

    // deprecated, service has type designation now
    protected $hidden = ['driver'];

    protected $casts = [
        'options'             => 'array',
        'attributes'          => 'array',
        'service_id'          => 'integer',
        'port'                => 'integer',
        'default_schema_only' => 'boolean'
    ];

    protected $encrypted = ['username', 'password'];

    /**
     * Returns the name of the DB driver from a connection string
     *
     * @param string $dsn The connection string
     *
     * @return string name of the DB driver
     */
    public static function getDriverFromDSN($dsn)
    {
        if (is_string($dsn)) {
            if (($pos = strpos($dsn, ':')) !== false) {
                return strtolower(substr($dsn, 0, $pos));
            }
        }

        return null;
    }

    public static function getDriverName()
    {
        return 'Unknown';
    }

    public static function getDefaultDsn()
    {
        return '<driver>:host=localhost;port=1234;dbname=database';
    }

    public static function getDefaultPort()
    {
        return 1234;
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

    /**
     * @param int $id
     *
     * @return array
     */
    public static function getConfig($id)
    {
        $config = parent::getConfig($id);

        $cacheConfig = ServiceCacheConfig::whereServiceId($id)->first();
        $config['cache_enabled'] = (empty($cacheConfig)) ? false : $cacheConfig->getAttribute('cache_enabled');
        $config['cache_ttl'] = (empty($cacheConfig)) ? 0 : $cacheConfig->getAttribute('cache_ttl');

        return $config;
    }

    public static function validateConfig($config, $create = true)
    {
        $dsn = isset($config['dsn']) ? $config['dsn'] : null;
        $db = isset($config['database']) ? $config['database'] : null;
        if (empty($dsn) && empty($db)) {
            throw new BadRequestException('Database connection string (DSN) or host and database name must be provided.');
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

        parent::setConfig($id, $config);
    }

    /**
     * {@inheritdoc}
     */
    public static function getConfigSchema()
    {
        $schema = parent::getConfigSchema();
        $schema = array_merge($schema, ServiceCacheConfig::getConfigSchema());

        return $schema;
    }

    /**
     * @param array $schema
     */
    protected static function prepareConfigSchemaField(array &$schema)
    {
        parent::prepareConfigSchemaField($schema);

        switch ($schema['name']) {
            case 'dsn':
                $schema['label'] = 'Connection String (DSN)';
                $schema['description'] =
                    'Enter the full DSN or use the fields below to complete the connection information.' .
                    ' For example, DSN could be ' . static::getDefaultDsn();
                break;
            case 'host':
                $schema['type'] = 'string';
                $schema['description'] = 'The name of the database host, i.e. localhost, 192.168.1.1, etc..';
                break;
            case 'port':
                $schema['type'] = 'integer';
                $schema['description'] = 'The number of the database host port, i.e. ' . static::getDefaultPort();
                break;
            case 'database':
                $schema['type'] = 'string';
                $schema['description'] = 'The name of the database to connect to on the given server. This can be a lookup key.';
                break;
            case 'username':
                $schema['type'] = 'string';
                $schema['description'] = 'The name of the database user. This can be a lookup key.';
                break;
            case 'password':
                $schema['type'] = 'password';
                $schema['description'] = 'The password for the database user. This can be a lookup key.';
                break;
            case 'prefix':
                $schema['type'] = 'string';
                $schema['description'] = 'The name of the database table prefix.';
                break;
            case 'options':
                $schema['label'] = 'Driver Options';
                $schema['type'] = 'object';
                $schema['object'] =
                    [
                        'key'   => ['label' => 'Name', 'type' => 'string'],
                        'value' => ['label' => 'Value', 'type' => 'string']
                    ];
                $schema['description'] = 'A key=>value array of driver-specific connection options.';
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
                    'A key=>value array of attributes to be set after connection.' .
                    ' For further information, see http://php.net/manual/en/pdo.setattribute.php';
                break;
            case 'default_schema_only':
                $schema['description'] =
                    'Do not include other schemas/databases on this server ' .
                    'regardless of permissions given to the supplied credentials.';
                break;
        }
    }
}