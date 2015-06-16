<?php

namespace DreamFactory\Core\SqlDb\Services;

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
use DreamFactory\Core\Resources\BaseRestResource;
use DreamFactory\Core\Utility\Session;

/**
 * Class SqlDb
 *
 * @package DreamFactory\Core\SqlDb\Services
 */
class SqlDb extends BaseDbService
{
    //*************************************************************************
    //	Constants
    //*************************************************************************

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
            'class_name' => 'DreamFactory\\Core\\SqlDb\\Resources\\Schema',
            'label'      => 'Schema',
        ],
        Table::RESOURCE_NAME           => [
            'name'       => Table::RESOURCE_NAME,
            'class_name' => 'DreamFactory\\Core\\SqlDb\\Resources\\Table',
            'label'      => 'Tables',
        ],
        StoredProcedure::RESOURCE_NAME => [
            'name'       => StoredProcedure::RESOURCE_NAME,
            'class_name' => 'DreamFactory\\Core\\SqlDb\\Resources\\StoredProcedure',
            'label'      => 'Stored Procedures',
        ],
        StoredFunction::RESOURCE_NAME  => [
            'name'       => StoredFunction::RESOURCE_NAME,
            'class_name' => 'DreamFactory\\Core\\SqlDb\\Resources\\StoredFunction',
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
//        Session::replaceLookups( $config, true );

        if (null === ($dsn = ArrayUtils::get($config, 'dsn', null, true))) {
            throw new \InvalidArgumentException('Database connection string (DSN) can not be empty.');
        }

        $user = ArrayUtils::get($config, 'username');
        $password = ArrayUtils::get($config, 'password');

        $this->dbConn = new Connection($dsn, $user, $password);

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
                $this->dbConn->active = false;
                $this->dbConn = null;
            } catch (\PDOException $_ex) {
                error_log("Failed to disconnect from database.\n{$_ex->getMessage()}");
            } catch (\Exception $_ex) {
                error_log("Failed to disconnect from database.\n{$_ex->getMessage()}");
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
        } catch (\PDOException $_ex) {
            throw new InternalServerErrorException("Failed to connect to database.\n{$_ex->getMessage()}");
        } catch (\Exception $_ex) {
            throw new InternalServerErrorException("Failed to connect to database.\n{$_ex->getMessage()}");
        }
    }

    /**
     * {@InheritDoc}
     */
    protected function handleResource(array $resources)
    {
        try {
            return parent::handleResource($resources);
        } catch (NotFoundException $_ex) {
            // If version 1.x, the resource could be a table
//            if ($this->request->getApiVersion())
//            {
//                $resource = $this->instantiateResource( 'DreamFactory\\Core\\SqlDb\\Resources\\Table', [ 'name' => $this->resource ] );
//                $newPath = $this->resourceArray;
//                array_shift( $newPath );
//                $newPath = implode( '/', $newPath );
//
//                return $resource->handleRequest( $this->request, $newPath, $this->outputFormat );
//            }

            throw $_ex;
        }
    }

    /**
     * {@inheritdoc}
     */
//    protected function respond()
//    {
//        if ( Verbs::POST === $this->getRequestedAction() )
//        {
//            switch ( $this->resource )
//            {
//                case Table::RESOURCE_NAME:
//                case Schema::RESOURCE_NAME:
//                    if ( !( $this->response instanceof ServiceResponseInterface ) )
//                    {
//                        $this->response = ResponseFactory::create( $this->response, $this->outputFormat, ServiceResponseInterface::HTTP_CREATED );
//                    }
//                    break;
//            }
//        }
//
//        parent::respond();
//    }

    /**
     * {@inheritdoc}
     */
    public function getApiDocInfo()
    {
        $base = parent::getApiDocInfo();

        $apis = [];
        $models = [];
        foreach ($this->resources as $resourceInfo) {
            $className = $resourceInfo['class_name'];

            if (!class_exists($className)) {
                throw new InternalServerErrorException('Service configuration class name lookup failed for resource ' .
                    $this->resourcePath);
            }

            /** @var BaseRestResource $resource */
            $resource = $this->instantiateResource($className, $resourceInfo);

            $_access = $this->getPermissions($resource->name);
            if (!empty($_access)) {
                $results = $resource->getApiDocInfo();
                if (isset($results, $results['apis'])) {
                    $apis = array_merge($apis, $results['apis']);
                }
                if (isset($results, $results['models'])) {
                    $models = array_merge($models, $results['models']);
                }
            }
        }

        $base['apis'] = array_merge($base['apis'], $apis);
        $base['models'] = array_merge($base['models'], $models);

        return $base;
    }
}