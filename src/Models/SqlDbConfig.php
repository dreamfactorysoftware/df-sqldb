<?php
namespace DreamFactory\Core\SqlDb\Models;

use DreamFactory\Core\Components\RequireExtensions;
use DreamFactory\Core\Database\Connection;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Models\BaseServiceConfigModel;
use DreamFactory\Core\Models\ServiceCacheConfig;
use DreamFactory\Library\Utility\ArrayUtils;

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
 * @method static \Illuminate\Database\Query\Builder|SqlDbConfig whereServiceId($value)
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
        if (null === ($dsn = ArrayUtils::get($config, 'dsn', null, true))) {
            throw new BadRequestException('Database connection string (DSN) can not be empty.');
        }

        $driver = Connection::getDriverFromDSN($dsn);
        Connection::requireDriver($driver);

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
                $values = Connection::getAllDrivers();
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