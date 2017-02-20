<?php

namespace DreamFactory\Core\SqlDb\Services;

use DreamFactory\Core\Enums\DbResourceTypes;
use DreamFactory\Core\Database\Services\BaseDbService;
use DreamFactory\Core\SqlDb\Resources\Schema;
use DreamFactory\Core\SqlDb\Resources\StoredFunction;
use DreamFactory\Core\SqlDb\Resources\StoredProcedure;
use DreamFactory\Core\SqlDb\Resources\Table;
use DreamFactory\Core\Utility\Session;
use Illuminate\Database\DatabaseManager;
use DbSchemaExtensions;

/**
 * Class SqlDb
 *
 * @package DreamFactory\Core\SqlDb\Services
 */
class SqlDb extends BaseDbService
{
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
    }

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

        $config = array_get($settings, 'config', []);
        Session::replaceLookups($config, true);

        static::adaptConfig($config);

        $options = array_get($config, 'options', []);
        if (!is_array($options)) {
            // laravel database config requires options to be [], not null
            $config['options'] = [];
        }

        $name = 'service.' . $this->name;
        $config['name'] = $name;
        // add config to global for reuse, todo check existence and update?
        config(['database.connections.service.' . $this->name => $config]);
        /** @type DatabaseManager $db */
        $db = app('db');
        $this->dbConn = $db->connection($name);

        $this->initStatements(array_get($config, 'statements', []));

        $driver = $this->dbConn->getDriverName();
        if (null === $this->schema = DbSchemaExtensions::getSchemaExtension($driver, $this->dbConn)) {
            throw new \Exception("Driver '$driver' is not supported by this software.");
        }

        $this->schema->setCache($this);
        $this->schema->setExtraStore($this);

        $schema = array_get($config, 'schema');
        $this->schema->setUserSchema($schema);
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

    /**
     * {@inheritdoc}
     */
    public function getResources($only_handlers = false)
    {
        $types = $this->schema->getSupportedResourceTypes();
        $resources = [
            Schema::RESOURCE_NAME => [
                'name'       => Schema::RESOURCE_NAME,
                'class_name' => Schema::class,
                'label'      => 'Schema',
            ],
            Table::RESOURCE_NAME  => [
                'name'       => Table::RESOURCE_NAME,
                'class_name' => Table::class,
                'label'      => 'Tables',
            ]
        ];
        if (in_array(DbResourceTypes::TYPE_PROCEDURE, $types)) {
            $resources[StoredProcedure::RESOURCE_NAME] = [
                'name'       => StoredProcedure::RESOURCE_NAME,
                'class_name' => StoredProcedure::class,
                'label'      => 'Stored Procedures',
            ];
        }
        if (in_array(DbResourceTypes::TYPE_FUNCTION, $types)) {
            $resources[StoredFunction::RESOURCE_NAME] = [
                'name'       => StoredFunction::RESOURCE_NAME,
                'class_name' => StoredFunction::class,
                'label'      => 'Stored Functions',
            ];
        }

        return ($only_handlers) ? $resources : array_values($resources);
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
}