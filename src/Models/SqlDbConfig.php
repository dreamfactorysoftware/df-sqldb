<?php
namespace DreamFactory\Core\SqlDb\Models;

use DreamFactory\Library\Utility\ArrayUtils;
use DreamFactory\Core\Components\RequireExtensions;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Models\BaseServiceConfigModel;
use DreamFactory\Core\SqlDbCore\Connection;

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

    protected $fillable = ['service_id', 'driver', 'dsn', 'username', 'password', 'options', 'attributes'];

    protected $casts = ['options' => 'array', 'attributes' => 'array'];

    protected $encrypted = ['username', 'password'];

    public static function validateConfig($config)
    {
        if (null === ($dsn = ArrayUtils::get($config, 'dsn', null, true))) {
            throw new BadRequestException('Database connection string (DSN) can not be empty.');
        }

        $driver = Connection::getDriverFromDSN($dsn);
        Connection::requireDriver($driver);

        return true;
    }

    /**
     * @param array $schema
     */
    protected static function prepareConfigSchemaField(array &$schema)
    {
        parent::prepareConfigSchemaField($schema);

        switch ($schema['name'])
        {
            case 'driver':
                $schema['options'] = Connection::getAvailableDrivers();
                break;
            case 'dsn':
                $schema['label'] = 'Connection String (DSN)';
                break;
            case 'username':
                $schema['type'] = 'string';
                break;
            case 'password':
                $schema['type'] = 'password';
                break;
            case 'options':
                $schema['type'] = 'object(string,string)';
                break;
            case 'attributes':
                $schema['type'] = 'object(string,string)';
                break;
        }
    }
}