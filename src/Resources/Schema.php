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

namespace DreamFactory\SqlDb\Resources;

use DreamFactory\Library\Utility\ArrayUtils;
use DreamFactory\Library\Utility\Inflector;
use DreamFactory\Rave\Common\Exceptions\BadRequestException;
use DreamFactory\Rave\Common\Exceptions\InternalServerErrorException;
use DreamFactory\Rave\Common\Exceptions\RestException;
use DreamFactory\SqlDb\Services\SqlDbService;
use Rave\Resources\BaseDbSchemaResource;
use Rave\Utility\SqlDbUtilities;

// Handle schema options, table add, delete, etc
class Schema extends BaseDbSchemaResource
{
    //*************************************************************************
    //	Members
    //*************************************************************************

    /**
     * @var null|SqlDbService
     */
    protected $service = null;

    //*************************************************************************
    //	Methods
    //*************************************************************************

    /**
     * @return null|SqlDbService
     */
    public function getService()
    {
        return $this->service;
    }

    /**
     * {@inheritdoc}
     */
    public function listResources($include_properties = null)
    {
        $refresh = $this->request->queryBool('refresh');
        $_names = SqlDbUtilities::describeDatabase( $this->service->getConnection(), true, $refresh );

        if (empty($include_properties))
        {
            return array('resource' => $_names);
        }

        $_extras = SqlDbUtilities::getSchemaExtrasForTables( $this->service->getServiceId(), $_names, false, 'table,label,plural' );

        $_tables = array();
        foreach ( $_names as $name )
        {
            $label = '';
            $plural = '';
            foreach ( $_extras as $each )
            {
                if ( 0 == strcasecmp( $name, ArrayUtils::get( $each, 'table', '' ) ) )
                {
                    $label = ArrayUtils::get( $each, 'label' );
                    $plural = ArrayUtils::get( $each, 'plural' );
                    break;
                }
            }

            if ( empty( $label ) )
            {
                $label = Inflector::camelize( $name, ['_','.'], true );
            }

            if ( empty( $plural ) )
            {
                $plural = Inflector::pluralize( $label );
            }

            $_tables[] = array('name' => $name, 'label' => $label, 'plural' => $plural);
        }

        return $this->makeResourceList($_tables, $include_properties, true);
    }

    /**
     * {@inheritdoc}
     */
    public function describeTables( $tables, $refresh = false )
    {
        $_tables = SqlDbUtilities::validateAsArray( $tables, ',', true );

        try
        {
            $_resources = array();
            foreach ( $_tables as $_table )
            {
                if ( null != $_name = ArrayUtils::get( $_table, 'name', $_table, false, true ) )
                {
                    $_access = $this->getPermissions( $_name );
                    if ( !empty( $_access ) )
                    {
                        $_result = SqlDbUtilities::describeTable( $this->service->getServiceId(), $this->service->getConnection(), $_name, null, $refresh );
                        $_result['access'] = $_access;
                        $_resources[] = $_result;
                    }
                }
            }

            return $_resources;
        }
        catch ( RestException $ex )
        {
            throw $ex;
        }
        catch ( \Exception $ex )
        {
            throw new InternalServerErrorException( "Error describing database tables.\n" . $ex->getMessage(), $ex->getCode() );
        }
    }

    /**
     * {@inheritdoc}
     */
    public function describeTable( $table, $refresh = false )
    {
        if ( empty( $table ) )
        {
            throw new BadRequestException( 'Table name can not be empty.' );
        }

        try
        {
            $_result = SqlDbUtilities::describeTable( $this->service->getServiceId(), $this->service->getConnection(), $table, null, $refresh );
            $_result['access'] = $this->getPermissions( $table );

            return $_result;
        }
        catch ( RestException $ex )
        {
            throw $ex;
        }
        catch ( \Exception $ex )
        {
            throw new InternalServerErrorException( "Error describing database table '$table'.\n" . $ex->getMessage(), $ex->getCode() );
        }
    }

    /**
     * {@inheritdoc}
     */
    public function describeField( $table, $field, $refresh = false )
    {
        if ( empty( $table ) )
        {
            throw new BadRequestException( 'Table name can not be empty.' );
        }

        try
        {
            $_result = SqlDbUtilities::describeTableFields( $this->service->getServiceId(), $this->service->getConnection(), $table, $field );

            return ArrayUtils::get( $_result, 0 );
        }
        catch ( RestException $ex )
        {
            throw $ex;
        }
        catch ( \Exception $ex )
        {
            throw new InternalServerErrorException( "Error describing database table '$table' field '$field'.\n" . $ex->getMessage(), $ex->getCode() );
        }
    }

    /**
     * {@inheritdoc}
     */
    public function createTables( $tables, $check_exist = false, $return_schema = false )
    {
        $tables = SqlDbUtilities::validateAsArray( $tables, null, true, 'There are no table sets in the request.' );

        // check for system tables and deny
        foreach ( $tables as $_table )
        {
            if ( null === ( $_name = ArrayUtils::get( $_table, 'name' ) ) )
            {
                throw new BadRequestException( "Table schema received does not have a valid name." );
            }
        }

        $_result = SqlDbUtilities::updateTables( $this->service->getServiceId(), $this->service->getConnection(), $tables );

        //  Any changes here should refresh cached schema
        SqlDbUtilities::refreshCachedTables( $this->service->getConnection() );

        if ( $return_schema )
        {
            return $this->describeTables( $tables );
        }

        return $_result;
    }

    /**
     * {@inheritdoc}
     */
    public function createTable( $table, $properties = array(), $check_exist = false, $return_schema = false )
    {
        $properties = ArrayUtils::clean( $properties );
        $properties['name'] = $table;

        $_tables = SqlDbUtilities::validateAsArray( $properties, null, true, 'Bad data format in request.' );
        $_result = SqlDbUtilities::updateTables( $this->service->getServiceId(), $this->service->getConnection(), $_tables );
        $_result = ArrayUtils::get( $_result, 0, array() );

        //  Any changes here should refresh cached schema
        SqlDbUtilities::refreshCachedTables( $this->service->getConnection() );

        if ( $return_schema )
        {
            return $this->describeTable( $table );
        }

        return $_result;
    }

    /**
     * {@inheritdoc}
     */
    public function createField( $table, $field, $properties = array(), $check_exist = false, $return_schema = false )
    {
        $properties = ArrayUtils::clean( $properties );
        $properties['name'] = $field;

        $_fields = SqlDbUtilities::validateAsArray( $properties, null, true, 'Bad data format in request.' );

        $_result = SqlDbUtilities::updateFields( $this->service->getServiceId(), $this->service->getConnection(), $table, $_fields );

        //  Any changes here should refresh cached schema
        SqlDbUtilities::refreshCachedTables( $this->service->getConnection() );

        if ( $return_schema )
        {
            return $this->describeField( $table, $field );
        }

        return $_result;
    }

    /**
     * {@inheritdoc}
     */
    public function updateTables( $tables, $allow_delete_fields = false, $return_schema = false )
    {
        $tables = SqlDbUtilities::validateAsArray( $tables, null, true, 'There are no table sets in the request.' );

        foreach ( $tables as $_table )
        {
            if ( null === ( $_name = ArrayUtils::get( $_table, 'name' ) ) )
            {
                throw new BadRequestException( "Table schema received does not have a valid name." );
            }
        }

        $_result = SqlDbUtilities::updateTables( $this->service->getServiceId(), $this->service->getConnection(), $tables, true, $allow_delete_fields );

        //  Any changes here should refresh cached schema
        SqlDbUtilities::refreshCachedTables( $this->service->getConnection() );

        if ( $return_schema )
        {
            return $this->describeTables( $tables );
        }

        return $_result;
    }

    /**
     * {@inheritdoc}
     */
    public function updateTable( $table, $properties, $allow_delete_fields = false, $return_schema = false )
    {
        $properties = ArrayUtils::clean( $properties );
        $properties['name'] = $table;

        $_tables = SqlDbUtilities::validateAsArray( $properties, null, true, 'Bad data format in request.' );

        $_result = SqlDbUtilities::updateTables( $this->service->getServiceId(), $this->service->getConnection(), $_tables, true, $allow_delete_fields );
        $_result = ArrayUtils::get( $_result, 0, array() );

        //  Any changes here should refresh cached schema
        SqlDbUtilities::refreshCachedTables( $this->service->getConnection() );

        if ( $return_schema )
        {
            return $this->describeTable( $table );
        }

        return $_result;
    }

    /**
     * {@inheritdoc}
     */
    public function updateField( $table, $field, $properties = array(), $allow_delete_parts = false, $return_schema = false )
    {
        if ( empty( $table ) )
        {
            throw new BadRequestException( 'Table name can not be empty.' );
        }

        $properties = ArrayUtils::clean( $properties );
        $properties['name'] = $field;

        $_fields = SqlDbUtilities::validateAsArray( $properties, null, true, 'Bad data format in request.' );

        $_result = SqlDbUtilities::updateFields( $this->service->getServiceId(), $this->service->getConnection(), $table, $_fields, true );

        //  Any changes here should refresh cached schema
        SqlDbUtilities::refreshCachedTables( $this->service->getConnection() );

        if ( $return_schema )
        {
            return $this->describeField( $table, $field );
        }

        return $_result;
    }

    /**
     * {@inheritdoc}
     */
    public function deleteTable( $table, $check_empty = false )
    {
        if ( empty( $table ) )
        {
            throw new BadRequestException( 'Table name can not be empty.' );
        }

        //  Any changes here should refresh cached schema
        SqlDbUtilities::refreshCachedTables( $this->service->getConnection() );

        SqlDbUtilities::dropTable( $this->service->getConnection(), $table );

        //  Any changes here should refresh cached schema
        SqlDbUtilities::refreshCachedTables( $this->service->getConnection() );

        SqlDbUtilities::removeSchemaExtrasForTables( $this->service->getServiceId(), $table );
    }

    /**
     * {@inheritdoc}
     */
    public function deleteField( $table, $field )
    {
        if ( empty( $table ) )
        {
            throw new BadRequestException( 'Table name can not be empty.' );
        }

        SqlDbUtilities::dropField( $this->service->getConnection(), $table, $field );

        //  Any changes here should refresh cached schema
        SqlDbUtilities::refreshCachedTables( $this->service->getConnection() );

        SqlDbUtilities::removeSchemaExtrasForFields( $this->service->getServiceId(), $table, $field );
    }
}