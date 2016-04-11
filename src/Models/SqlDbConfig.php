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
 * @property string  $username
 * @property string  $password
 * @property string  $db
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
        'driver',
        'dsn',
        'username',
        'password',
        'options',
        'attributes',
        'default_schema_only'
    ];

    protected $casts = [
        'options'             => 'array',
        'attributes'          => 'array',
        'service_id'          => 'integer',
        'default_schema_only' => 'boolean'
    ];

    protected $encrypted = ['username', 'password'];

    /**
     * @var array mapping between database driver and connection class name.
     */
    public static $driverConnectorMap = [
        // PostgreSQL
        'pgsql'       => 'pgsql',
        // MySQL
        'mysql'       => 'mysql',
        // SQLite
        'sqlite'      => 'sqlite',
        // Oracle driver
        'oracle'      => 'oci',
        // IBM DB2 driver
        'ibm'         => 'ibm',
        // MS SQL Server on Windows hosts, alias for dblib on Linux, Mac OS X, and maybe others
        'sqlsrv'      => 'sqlsrv',
        // SAP SQL Anywhere alias for dblib on Linux, Mac OS X, and maybe others
        'sqlanywhere' => 'dblib',
    ];

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

    /**
     * Returns a list of available PDO drivers.
     *
     * @return array list of available PDO drivers
     * @see http://www.php.net/manual/en/function.PDO-getAvailableDBs.php
     */
    public static function getAllDrivers()
    {
        $values = [];
        $supported = \PDO::getAvailableDrivers();

        foreach (static::$driverConnectorMap as $driver => $pdoDriver) {
            switch ($driver) {
                case 'ibm':
                    // http://php.net/manual/en/ref.pdo-ibm.connection.php
                    $dsn = 'ibm:DRIVER={IBM DB2 ODBC DRIVER};DATABASE=db;HOSTNAME=localhost;PORT=56789;PROTOCOL=TCPIP;';
                    $label = 'IBM DB2';
                    break;
                case 'mysql':
                    // http://php.net/manual/en/ref.pdo-mysql.connection.php
                    $dsn = 'mysql:host=localhost;port=3306;dbname=db;charset=utf8';
                    $label = 'MySQL';
                    break;
                case 'oracle':
                    // http://php.net/manual/en/ref.pdo-oci.connection.php
                    $dsn =
                        'oci:dbname=(DESCRIPTION = (ADDRESS_LIST = (ADDRESS = (PROTOCOL = TCP)(HOST = 192.168.1.1)(PORT = 1521))) (CONNECT_DATA = (SID = db)))';
                    $label = 'Oracle Database';
                    break;
                case 'pgsql':
                    // http://php.net/manual/en/ref.pdo-pgsql.connection.php
                    $dsn = 'pgsql:host=localhost;port=5432;dbname=db;user=name;password=pwd';
                    $label = 'PostgreSQL';
                    break;
                case 'sqlanywhere':
                    // http://php.net/manual/en/ref.pdo-dblib.connection.php
                    $dsn = 'dblib:host=localhost:2638;dbname=database';
                    $label = 'SAP SQL Anywhere';
                    break;
                case 'sqlite':
                    // http://php.net/manual/en/ref.pdo-sqlite.connection.php
                    $dsn = 'sqlite:db.sq3';
                    $label = 'SQLite';
                    break;
                case 'sqlsrv':
                    if (substr(PHP_OS, 0, 3) == 'WIN') {
                        // http://php.net/manual/en/ref.pdo-sqlsrv.connection.php
                        $dsn = 'sqlsrv:Server=localhost,1433;Database=db';
                        $pdoDriver = 'sqlsrv';
                    } else {
                        // http://php.net/manual/en/ref.pdo-dblib.connection.php
                        $dsn = 'dblib:host=localhost:1433;dbname=database;charset=UTF-8';
                        $pdoDriver = 'dblib';
                    }
                    $label = 'SQL Server';
                    break;
                default:
                    $label = 'Unknown';
                    $dsn = '<driver>:host=localhost;port=1234;dbname=database';
            }
            $disable = !in_array($pdoDriver, $supported);
            $values[] = ['name' => $driver, 'label' => $label, 'disable' => $disable, 'dsn' => $dsn];
        }

        return $values;
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
                // don't call parent method here, no need for PDO driver
                $pdoDriver = 'oci';
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
            static::checkForPdoDriver($driver);
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
        if (empty($dsn)) {
            throw new BadRequestException('Database connection string (DSN) can not be empty.');
        }

        $driver = isset($config['driver']) ? $config['driver'] : null;
        static::requireDriver($driver);

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
            case 'driver':
                $values = static::getAllDrivers();
                $schema['type'] = 'picklist';
                $schema['values'] = $values;
                $schema['affects'] = 'dsn';
                $schema['description'] =
                    'Select the driver that matches the database type for which you want to connect.' .
                    ' For further information, see http://php.net/manual/en/pdo.drivers.php.';
                break;
            case 'dsn':
                $schema['label'] = 'Connection String (DSN)';
                $schema['description'] =
                    'The Data Source Name, or DSN, contains the information required to connect to the database.' .
                    ' For further information, see http://php.net/manual/en/pdo.construct.php.';
                break;
            case 'username':
                $schema['type'] = 'string';
                $schema['description'] = 'The name of the database user. This can be a lookup key.';
                break;
            case 'password':
                $schema['type'] = 'password';
                $schema['description'] = 'The password for the database user. This can be a lookup key.';
                break;
            case 'options':
                $schema['type'] = 'object';
                $schema['object'] =
                    [
                        'key'   => ['label' => 'Name', 'type' => 'string'],
                        'value' => ['label' => 'Value', 'type' => 'string']
                    ];
                $schema['description'] = 'A key=>value array of connection options.';
                break;
            case 'attributes':
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