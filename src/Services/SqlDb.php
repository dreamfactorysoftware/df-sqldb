<?php

namespace DreamFactory\Core\SqlDb\Services;

use DreamFactory\Core\Database\Services\BaseDbService;
use DreamFactory\Core\SqlDb\Resources\Schema;
use DreamFactory\Core\SqlDb\Resources\StoredFunction;
use DreamFactory\Core\SqlDb\Resources\StoredProcedure;
use DreamFactory\Core\SqlDb\Resources\Table;
use DreamFactory\Core\Utility\ResourcesWrapper;
use Illuminate\Database\DatabaseManager;
use DbSchemaExtensions;

/**
 * Class SqlDb
 *
 * @package DreamFactory\Core\SqlDb\Services
 */
class SqlDb extends BaseDbService
{
    //*************************************************************************
    //	Members
    //*************************************************************************

    /**
     * @var array
     */
    protected static $resources = [
        Schema::RESOURCE_NAME          => [
            'name'       => Schema::RESOURCE_NAME,
            'class_name' => Schema::class,
            'label'      => 'Schema',
        ],
        Table::RESOURCE_NAME           => [
            'name'       => Table::RESOURCE_NAME,
            'class_name' => Table::class,
            'label'      => 'Tables',
        ],
        StoredProcedure::RESOURCE_NAME => [
            'name'       => StoredProcedure::RESOURCE_NAME,
            'class_name' => StoredProcedure::class,
            'label'      => 'Stored Procedures',
        ],
        StoredFunction::RESOURCE_NAME  => [
            'name'       => StoredFunction::RESOURCE_NAME,
            'class_name' => StoredFunction::class,
            'label'      => 'Stored Functions',
        ],
    ];

    //*************************************************************************
    //	Methods
    //*************************************************************************

    /**
     * Create a new SqlDbSvc
     *
     * @param array $settings
     *
     * @throws \InvalidArgumentException
     * @throws \Exception
     */
    public function __construct($settings = [])
    {
        parent::__construct($settings);

        static::adaptConfig($this->config);

        $prefix = '';
        $parts = ['host', 'port', 'database', 'username', 'schema'];
        foreach ($parts as $part) {
            $prefix .= array_get($this->config, $part);
        }

        $this->setConfigBasedCachePrefix($prefix . ':');
    }

    /**
     * {@inheritdoc}
     */
    public static function adaptConfig(array &$config)
    {
        if (!isset($config['charset'])) {
            $config['charset'] = 'utf8';
        }

        if (!isset($config['collation'])) {
            $config['collation'] = 'utf8_unicode_ci';
        }

        // must be there
        if (!array_key_exists('database', $config)) {
            $config['database'] = null;
        }

        // must be there
        if (!array_key_exists('prefix', $config)) {
            $config['prefix'] = null;
        }

        $options = array_get($config, 'options', []);
        if (!is_array($options)) {
            // laravel database config requires options to be [], not null
            $config['options'] = [];
        } else {
            foreach ($options as $key => $value) {
                // Convert key and value constants like PDO::XXX
                if (is_string($key) && defined($key)) {
                    $key = constant($key);
                }
                if (is_string($value) && defined($value)) {
                    $value = constant($value);
                }
                $config['options'][$key] = $value;
            }
        }
    }

    protected function initializeConnection()
    {
        $name = 'service.' . $this->name;
        $this->config['name'] = $name;
        // add config to global for reuse, todo check existence and update?
        config(['database.connections.service.' . $this->name => $this->config]);

        /** @type DatabaseManager $db */
        $db = app('db');
        $this->dbConn = $db->connection('service.'.$this->name);

        $this->initStatements(array_get($this->config, 'statements', []));

        $driver = $this->dbConn->getDriverName();
        if (null === $this->schema = DbSchemaExtensions::getSchemaExtension($driver, $this->dbConn)) {
            throw new \Exception("Driver '$driver' is not supported by this software.");
        }
    }

    /**
     * Object destructor
     */
    public function __destruct()
    {
        /** @type DatabaseManager $db */
        $db = app('db');
        $db->disconnect('service.' . $this->name);

        parent::__destruct();
    }

    protected function initStatements($statements = [])
    {
        if (is_string($statements)) {
            $statements = [$statements];
        } elseif (!is_array($statements)) {
            $statements = [];
        }

        foreach ($statements as $statement) {
            $this->dbConn->statement($statement);
        }
    }

    protected function getApiDocSchemas()
    {
        $wrapper = ResourcesWrapper::getWrapper();

        $add = [
            'StoredRoutineSchemas'  => [
                'type'       => 'object',
                'properties' => [
                    $wrapper => [
                        'type'        => 'array',
                        'description' => 'An array of routine definitions.',
                        'items'       => [
                            '$ref' => '#/components/schemas/StoredRoutineSchema',
                        ],
                    ],
                ],
            ],
            'StoredRoutineSchema' => [
                'type'       => 'object',
                'properties' => [
                    'name'        => [
                        'type'        => 'string',
                        'description' => 'Identifier/Name for the routine.',
                    ],
                    'label'       => [
                        'type'        => 'string',
                        'description' => 'Displayable name for the routine.',
                    ],
                    'description'      => [
                        'type'        => 'string',
                        'description' => 'Description for the routine.',
                    ],
                    'return_type'      => [
                        'type'        => 'string',
                        'description' => 'Displayable plural name for the routine.',
                    ],
                    'return_schema' => [
                        'type'        => 'string',
                        'description' => 'Field(s), if any, that represent the primary key of each record.',
                    ],
                    'params'        => [
                        'type'        => 'array',
                        'description' => 'An array of available fields in each record.',
                        'items'       => [
                            '$ref' => '#/components/schemas/StoredRoutineParameterSchema',
                        ],
                    ],
                ],
            ],
            'StoredRoutineParameterSchema' => [
                'type'       => 'object',
                'properties' => [
                    'name'        => [
                        'type'        => 'string',
                        'description' => 'Identifier/Name for the parameter.',
                    ],
                    'position'       => [
                        'type'        => 'string',
                        'description' => 'Displayable singular name for the parameter.',
                    ],
                    'param_type'      => [
                        'type'        => 'string',
                        'description' => 'Displayable plural name for the parameter.',
                    ],
                    'type'               => [
                        'type'        => 'string',
                        'description' => 'The DreamFactory abstract data type for this parameter.',
                    ],
                    'db_type'            => [
                        'type'        => 'string',
                        'description' => 'The native database type used for this parameter.',
                    ],
                    'length'             => [
                        'type'        => 'integer',
                        'format'      => 'int32',
                        'description' => 'The maximum length allowed (in characters for string, displayed for numbers).',
                    ],
                    'precision'          => [
                        'type'        => 'integer',
                        'format'      => 'int32',
                        'description' => 'Total number of places for numbers.',
                    ],
                    'scale'              => [
                        'type'        => 'integer',
                        'format'      => 'int32',
                        'description' => 'Number of decimal places allowed for numbers.',
                    ],
                    'default'      => [
                        'type'        => 'string',
                        'description' => 'Default value for this parameter.',
                    ],
                ],
            ],
        ];

        return array_merge(parent::getApiDocSchemas(), $add);
    }
}