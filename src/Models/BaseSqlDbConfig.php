<?php
namespace DreamFactory\Core\SqlDb\Models;

use DreamFactory\Core\Components\RequireExtensions;
use DreamFactory\Core\Database\Models\BaseDbConfig;

/**
 * BaseSqlDbConfig
 *
 * @property array   $options
 * @property array   $attributes
 * @property array   $statements
 */
class BaseSqlDbConfig extends BaseDbConfig
{
    use RequireExtensions;

    /**
     * {@inheritdoc}
     */
    public static function getSchema()
    {
        $schema = parent::getSchema();

        $extras = [
            'options' => [
                'name'        => 'options',
                'label'       => 'Driver Options',
                'type'        => 'object',
                'object'      => [
                    'key'   => ['label' => 'Name', 'type' => 'string'],
                    'value' => ['label' => 'Value', 'type' => 'string']
                ],
                'description' => 'A key-value array of driver-specific connection options.'
            ],
            'attributes' => [
                'name'        => 'attributes',
                'label'       => 'Driver Attributes',
                'type'        => 'object',
                'object'      =>
                    [
                        'key'   => ['label' => 'Name', 'type' => 'string'],
                        'value' => ['label' => 'Value', 'type' => 'string']
                    ],
                'description' =>
                    'A key-value array of attributes to be set after connection.' .
                    ' For further information, see http://php.net/manual/en/pdo.setattribute.php',
            ],
            'statements' => [
                'name'        => 'statements',
                'label'       => 'Additional SQL Statements',
                'type'        => 'array',
                'items'       => 'string',
                'description' => 'An array of SQL statements to run during connection initialization.',
            ]
        ];

        return array_merge($extras, $schema);
    }

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
                $pdoDriver = 'sqlsrv';
                $extension = 'sqlsrv';

                if (!extension_loaded($extension)) {
                    $pdoDriver = 'dblib';
                    $extension = 'mssql';
                    if (!extension_loaded($extension)) {
                        throw new \Exception("Required extension '$extension' is not detected, but may be compiled in.");
                    }
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
}