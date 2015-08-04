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

    protected $casts = ['options' => 'array', 'attributes' => 'array', 'service_id' => 'integer'];

    protected $encrypted = ['username', 'password'];

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
     * @param array $schema
     */
    protected static function prepareConfigSchemaField(array &$schema)
    {
        parent::prepareConfigSchemaField($schema);

        switch ($schema['name']) {
            case 'driver':
                $values = [];
                $supported = Connection::getAvailableDrivers();
                foreach (Connection::$driverLabelMap as $driver => $label) {
                    $disable = !in_array($driver, $supported);
                    $dsn = ArrayUtils::get(Connection::$driverDsnMap, $driver, '');
                    $values[] = ['name' => $driver, 'label' => $label, 'disable' => $disable, 'dsn' => $dsn];
                }
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
                break;
            case 'password':
                $schema['type'] = 'password';
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
        }
    }
}