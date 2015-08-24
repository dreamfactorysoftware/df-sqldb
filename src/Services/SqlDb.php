<?php

namespace DreamFactory\Core\SqlDb\Services;

use DreamFactory\Core\Components\DbSchemaExtras;
use DreamFactory\Core\Utility\Session;
use DreamFactory\Library\Utility\ArrayUtils;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Exceptions\NotFoundException;
use DreamFactory\Core\SqlDb\Resources\Schema;
use DreamFactory\Core\SqlDb\Resources\StoredFunction;
use DreamFactory\Core\SqlDb\Resources\StoredProcedure;
use DreamFactory\Core\SqlDb\Resources\Table;
use DreamFactory\Core\SqlDbCore\Connection;
use DreamFactory\Core\Enums\SqlDbDriverTypes;
use DreamFactory\Core\Services\BaseDbService;

/**
 * Class SqlDb
 *
 * @package DreamFactory\Core\SqlDb\Services
 */
class SqlDb extends BaseDbService
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
        Session::replaceLookups( $config, true );

        if (null === ($dsn = ArrayUtils::get($config, 'dsn', null, true))) {
            throw new \InvalidArgumentException('Database connection string (DSN) can not be empty.');
        }

        $user = ArrayUtils::get($config, 'username');
        $password = ArrayUtils::get($config, 'password');

        $this->dbConn = new Connection($dsn, $user, $password, $this, $this);

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
     * {@InheritDoc}
     */
    protected function handleResource(array $resources)
    {
        try {
            return parent::handleResource($resources);
        } catch (NotFoundException $ex) {
            // If version 1.x, the resource could be a table
//            if ($this->request->getApiVersion())
//            {
//                $resource = $this->instantiateResource( Table::class, [ 'name' => $this->resource ] );
//                $newPath = $this->resourceArray;
//                array_shift( $newPath );
//                $newPath = implode( '/', $newPath );
//
//                return $resource->handleRequest( $this->request, $newPath, $this->outputFormat );
//            }

            throw $ex;
        }
    }
}