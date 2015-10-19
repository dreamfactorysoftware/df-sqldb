<?php

namespace DreamFactory\Core\SqlDb\Services;

use DreamFactory\Core\Components\DbSchemaExtras;
use DreamFactory\Core\Contracts\CacheInterface;
use DreamFactory\Core\Database\Connection;
use DreamFactory\Core\Database\ConnectionFactory;
use DreamFactory\Core\Database\DbExtrasInterface;
use DreamFactory\Core\Enums\SqlDbDriverTypes;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Services\BaseDbService;
use DreamFactory\Core\SqlDb\Resources\Schema;
use DreamFactory\Core\SqlDb\Resources\StoredFunction;
use DreamFactory\Core\SqlDb\Resources\StoredProcedure;
use DreamFactory\Core\SqlDb\Resources\Table;
use DreamFactory\Core\Utility\Session;
use DreamFactory\Library\Utility\ArrayUtils;
use DreamFactory\Managed\Support\Managed;

/**
 * Class SqlDb
 *
 * @package DreamFactory\Core\SqlDb\Services
 */
class SqlDb extends BaseDbService implements CacheInterface, DbExtrasInterface
{
    use DbSchemaExtras;

    //*************************************************************************
    //	Members
    //*************************************************************************

    /**
     * @var Connection
     */
    protected $dbConn;
    /**
     * @var integer
     */
    protected $driverType = SqlDbDriverTypes::DRV_OTHER;

    /**
     * @var array
     */
    protected $resources = [
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

        $config = ArrayUtils::clean(ArrayUtils::get($settings, 'config'));
        Session::replaceLookups($config, true);

        $driver = isset($config['driver']) ? $config['driver'] : null;
        $this->dbConn = ConnectionFactory::createConnection($driver, $config);
        $this->dbConn->setCache($this);
        $this->dbConn->setExtraStore($this);

        $defaultSchemaOnly = ArrayUtils::getBool($config, 'default_schema_only');
        $this->dbConn->setDefaultSchemaOnly($defaultSchemaOnly);

        switch ($this->dbConn->getDBName()) {
            case SqlDbDriverTypes::MYSQL:
            case SqlDbDriverTypes::MYSQLI:
                $this->dbConn->setAttribute(\PDO::ATTR_EMULATE_PREPARES, true);
                break;

            case SqlDbDriverTypes::DBLIB:
                $this->dbConn->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
                break;
        }

        $attributes = ArrayUtils::clean(ArrayUtils::get($settings, 'attributes'));

        if (!empty($attributes)) {
            $this->dbConn->setAttributes($attributes);
        }
    }

    /**
     * Object destructor
     */
    public function __destruct()
    {
        if (isset($this->dbConn)) {
            try {
                $this->dbConn->setActive(false);
                $this->dbConn = null;
            } catch (\PDOException $ex) {
                error_log("Failed to disconnect from database.\n{$ex->getMessage()}");
            } catch (\Exception $ex) {
                error_log("Failed to disconnect from database.\n{$ex->getMessage()}");
            }
        }
    }

    /**
     * @return Connection
     */
    public function getConnection()
    {
        $this->checkConnection();

        return $this->dbConn;
    }

    /**
     * @throws \Exception
     */
    protected function checkConnection()
    {
        if (!isset($this->dbConn)) {
            throw new InternalServerErrorException('Database connection has not been initialized.');
        }

        try {
            $this->dbConn->setActive(true);
        } catch (\PDOException $ex) {
            throw new InternalServerErrorException("Failed to connect to database.\n{$ex->getMessage()}");
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to connect to database.\n{$ex->getMessage()}");
        }
    }

    /**
     * @param string|null $schema
     * @param bool        $refresh
     * @param bool        $use_alias
     *
     * @return \DreamFactory\Core\Database\TableNameSchema[]
     * @throws \Exception
     */
    public function getTableNames($schema = null, $refresh = false, $use_alias = false)
    {
        $tables = $this->dbConn->getSchema()->getTableNames($schema, true, $refresh);
        if ($use_alias) {
            $temp = []; // reassign index to alias
            foreach ($tables as $table) {
                $temp[strtolower($table->getName(true))] = $table;
            }

            return $temp;
        }

        return $tables;
    }

    public function refreshTableCache()
    {
        $this->dbConn->getSchema()->refresh();
    }
}