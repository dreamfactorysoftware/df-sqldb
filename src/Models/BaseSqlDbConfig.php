<?php
namespace DreamFactory\Core\SqlDb\Models;

use DreamFactory\Core\Components\RequireExtensions;
use DreamFactory\Core\Database\Components\SupportsUpsert;
use DreamFactory\Core\Database\Schema\ColumnSchema;
use DreamFactory\Core\Models\BaseServiceConfigModel;

/**
 * BaseSqlDbConfig
 *
 * @property integer $service_id
 * @property array   $connection
 * @property array   $options
 * @property array   $attributes
 * @property array   $statements
 * @method static BaseSqlDbConfig whereServiceId($value)
 */
class BaseSqlDbConfig extends BaseServiceConfigModel
{
    use RequireExtensions, SupportsUpsert;

    protected $table = 'sql_db_config';

    // deprecated, service has type designation now
    protected $hidden = ['connection', 'driver', 'dsn'];

    protected $fillable = ['service_id', 'options', 'attributes', 'statements'];

    protected $casts = [
        'service_id' => 'integer',
        'connection' => 'array',
        'options'    => 'array',
        'attributes' => 'array',
        'statements' => 'array',
    ];

    public function getFillable()
    {
        return array_merge(parent::getFillable(), $this->getConnectionFields());
    }

    protected function getConnectionFields()
    {
        return [];
    }


    public static function getDefaultConnectionInfo()
    {
        return [];
    }

    /**
     * {@inheritdoc}
     */
    public function attributesToArray()
    {
        $fields = $this->getConnectionFields();
        $this->appends = array_values(array_flip(array_except(array_flip($this->appends), $fields)));

        $attributes = parent::attributesToArray();
        foreach ($fields as $field) {
            $attributes[$field] = $this->getAttributeValue($field);
        }

        $this->appends = array_merge($this->appends, $fields);

        return $attributes;
    }

    /**
     * {@inheritdoc}
     */
    protected function getAttributeFromArray($key)
    {
        if (in_array($key, $this->getConnectionFields())) {
            $value = array_get($this->getAttribute('connection'), $key);
            $this->decryptAttribute($key, $value);

            return $value;
        }

        return parent::getAttributeFromArray($key);
    }

    /**
     * {@inheritdoc}
     */
    public function setAttribute($key, $value)
    {
        // wish they had a setAttributeToArray() to override
        if (in_array($key, $this->getConnectionFields())) {
            // if protected, and trying to set the mask, throw it away
            if ($this->isProtectedAttribute($key, $value)) {
                return $this;
            }

            $this->encryptAttribute($key, $value);

            $connection = (array)$this->getAttribute('connection');
            array_set($connection, $key, $value);
            parent::setAttribute('connection', $connection);

            return $this;
        }

        return parent::setAttribute($key, $value);
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
                if ('connection' === $name) {
                    // specific attributes to the different databases
                    $temp = static::getDefaultConnectionInfo();
                    $out = array_merge($out, $temp);
                }

                // Skip if column is hidden
                if (in_array($name, $model->getHidden())) {
                    continue;
                }
                /** @var ColumnSchema $column */
                if (('service_id' === $name) || $column->autoIncrement) {
                    continue;
                }

                $temp = $column->toArray();
                static::prepareConfigSchemaField($temp);
                $out[] = $temp;
            }

            // Add allow upsert here
            $out[] = [
                'name'        => 'allow_upsert',
                'label'       => 'Allow Upsert',
                'type'        => 'boolean',
                'allow_null'  => false,
                'default'     => false,
                'description' => 'Allow PUT to create records if they do not exist and the service is capable.',
            ];

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