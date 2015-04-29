<?php
/**
 * This file is part of the DreamFactory Rave(tm)
 *
 * DreamFactory Rave(tm) <http://github.com/dreamfactorysoftware/rave>
 * Copyright 2012-2014 DreamFactory Software, Inc. <support@dreamfactory.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace DreamFactory\Rave\SqlDb\Services;

use DreamFactory\Library\Utility\ArrayUtils;
use DreamFactory\Rave\Exceptions\InternalServerErrorException;
use DreamFactory\Rave\Exceptions\NotFoundException;
use DreamFactory\Rave\SqlDb\Resources\Schema;
use DreamFactory\Rave\SqlDb\Resources\StoredFunction;
use DreamFactory\Rave\SqlDb\Resources\StoredProcedure;
use DreamFactory\Rave\SqlDb\Resources\Table;
use DreamFactory\Rave\SqlDbCore\Connection;
use DreamFactory\Rave\Enums\SqlDbDriverTypes;
use DreamFactory\Rave\Services\BaseDbService;
use DreamFactory\Rave\Resources\BaseRestResource;

/**
 * Class SqlDb
 *
 * @package DreamFactory\Rave\SqlDb\Services
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
            'class_name' => 'DreamFactory\\Rave\\SqlDb\\Resources\\Schema',
            'label'      => 'Schema',
        ],
        Table::RESOURCE_NAME           => [
            'name'       => Table::RESOURCE_NAME,
            'class_name' => 'DreamFactory\\Rave\\SqlDb\\Resources\\Table',
            'label'      => 'Table',
        ],
        StoredProcedure::RESOURCE_NAME => [
            'name'       => StoredProcedure::RESOURCE_NAME,
            'class_name' => 'DreamFactory\\Rave\\SqlDb\\Resources\\StoredProcedure',
            'label'      => 'Stored Procedures',
        ],
        StoredFunction::RESOURCE_NAME  => [
            'name'       => StoredFunction::RESOURCE_NAME,
            'class_name' => 'DreamFactory\\Rave\\SqlDb\\Resources\\StoredFunction',
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
    public function __construct( $settings = [ ] )
    {
        parent::__construct( $settings );

        $config = ArrayUtils::clean( ArrayUtils::get( $settings, 'config' ) );
//        Session::replaceLookups( $config, true );

        if ( null === ( $dsn = ArrayUtils::get( $config, 'dsn', null, true ) ) )
        {
            throw new \InvalidArgumentException( 'Database connection string (DSN) can not be empty.' );
        }

        $user = ArrayUtils::get( $config, 'username' );
        $password = ArrayUtils::get( $config, 'password' );

        $this->dbConn = new Connection( $dsn, $user, $password );

        switch ( $this->dbConn->getDBName() )
        {
            case SqlDbDriverTypes::MYSQL:
            case SqlDbDriverTypes::MYSQLI:
                $this->dbConn->setAttribute( \PDO::ATTR_EMULATE_PREPARES, true );
                break;

            case SqlDbDriverTypes::DBLIB:
                $this->dbConn->setAttribute( \PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION );
                break;
        }

        $attributes = ArrayUtils::clean( ArrayUtils::get( $settings, 'attributes' ) );

        if ( !empty( $attributes ) )
        {
            $this->dbConn->setAttributes( $attributes );
        }
    }

    /**
     * Object destructor
     */
    public function __destruct()
    {
        if ( isset( $this->dbConn ) )
        {
            try
            {
                $this->dbConn->active = false;
                $this->dbConn = null;
            }
            catch ( \PDOException $_ex )
            {
                error_log( "Failed to disconnect from database.\n{$_ex->getMessage()}" );
            }
            catch ( \Exception $_ex )
            {
                error_log( "Failed to disconnect from database.\n{$_ex->getMessage()}" );
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
        if ( !isset( $this->dbConn ) )
        {
            throw new InternalServerErrorException( 'Database connection has not been initialized.' );
        }

        try
        {
            $this->dbConn->setActive( true );
        }
        catch ( \PDOException $_ex )
        {
            throw new InternalServerErrorException( "Failed to connect to database.\n{$_ex->getMessage()}" );
        }
        catch ( \Exception $_ex )
        {
            throw new InternalServerErrorException( "Failed to connect to database.\n{$_ex->getMessage()}" );
        }
    }

    /**
     * @param BaseRestResource $class
     * @param array            $info
     *
     * @return mixed
     */
    protected function instantiateResource( $class, $info = [ ] )
    {
        return new $class( $this, $info );
    }

    /**
     * @param string $main   Main resource or empty for service
     * @param string $sub    Subtending resources if applicable
     * @param string $action Action to validate permission
     */
    protected function validateResourceAccess( $main, $sub, $action )
    {
        if ( !empty( $main ) )
        {
            $_resource = rtrim( $main, '/' ) . '/';
            switch ( $main )
            {
                case Schema::RESOURCE_NAME:
                case Table::RESOURCE_NAME:
                    if ( !empty( $sub ) )
                    {
                        $_resource .= $sub;
                    }
                    break;
                case StoredProcedure::RESOURCE_NAME:
                case StoredFunction::RESOURCE_NAME:
                    if ( !empty( $sub ) )
                    {
                        $_resource .= rtrim( ( false !== strpos( $sub, '(' ) ) ? strstr( $sub, '(', true ) : $sub );
                    }
                    break;
            }

            $this->checkPermission( $action, $_resource );

            return;
        }

        parent::validateResourceAccess( $main, $sub, $action );
    }

    /**
     * {@InheritDoc}
     */
    protected function handleResource( array $resources )
    {
        try
        {
            return parent::handleResource( $resources );
        }
        catch ( NotFoundException $_ex )
        {
            // If version 1.x, the resource could be a table
//            if ($this->request->getApiVersion())
//            {
//                $resource = $this->instantiateResource( 'DreamFactory\\Rave\\SqlDb\\Resources\\Table', [ 'name' => $this->resource ] );
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
     * @return array
     */
    protected function getResources()
    {
        return $this->resources;
    }

    // REST service implementation

    /**
     * {@inheritdoc}
     */
    public function listResources( $include_properties = null )
    {
        if ( !$this->request->getParameterAsBool( 'as_access_components' ) )
        {
            return parent::listResources( $include_properties );
        }

        $output = [ ];
        foreach ($this->resources as $resourceInfo)
        {
            $className = $resourceInfo['class_name'];

            if ( !class_exists( $className ) )
            {
                throw new InternalServerErrorException( 'Service configuration class name lookup failed for resource ' . $this->resourcePath );
            }

            /** @var BaseRestResource $resource */
            $resource = $this->instantiateResource( $className, $resourceInfo );

            $name = $className::RESOURCE_NAME . '/';
            $_access = $this->getPermissions( $name );
            if ( !empty( $_access ) )
            {
                $output[] = $name;
                $output[] = $name . '*';
            }

            $results = $resource->listResources(false);
            foreach ( $results as $name )
            {
                $name = $className::RESOURCE_NAME . '/' . $name;
                $_access = $this->getPermissions( $name );
                if ( !empty( $_access ) )
                {
                    $output[] = $name;
                }
            }
        }

        return [ 'resource' => $output ];
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

        $apis = [ ];
        $models = [ ];
        foreach ($this->resources as $resourceInfo)
        {
            $className = $resourceInfo['class_name'];

            if ( !class_exists( $className ) )
            {
                throw new InternalServerErrorException( 'Service configuration class name lookup failed for resource ' . $this->resourcePath );
            }

            /** @var BaseRestResource $resource */
            $resource = $this->instantiateResource( $className, $resourceInfo );

            $name = $className::RESOURCE_NAME . '/';
            $_access = $this->getPermissions( $name );
            if ( !empty( $_access ) )
            {
                $results = $resource->getApiDocInfo();
                if (isset($results, $results['apis']))
                {
                    $apis = array_merge( $apis, $results['apis'] );
                }
                if (isset($results, $results['models']))
                {
                    $models = array_merge( $models, $results['models'] );
                }
            }
        }

        $base['apis'] = array_merge( $base['apis'], $apis );
        $base['models'] = array_merge( $base['models'], $models );

        return $base;
    }
}