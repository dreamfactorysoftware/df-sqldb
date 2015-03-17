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
use Rave\Utility\SqlDbUtilities;
use DreamFactory\Rave\Common\Exceptions\BadRequestException;
use DreamFactory\Rave\Common\Exceptions\InternalServerErrorException;
use DreamFactory\Rave\Common\Exceptions\RestException;
use DreamFactory\SqlDb\Services\SqlDbService;
use Rave\Resources\BaseRestResource;

class StoredFunction extends BaseRestResource
{
    //*************************************************************************
    //	Constants
    //*************************************************************************

    /**
     * Resource tag for dealing with table schema
     */
    const RESOURCE_NAME = '_func';

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
     * @param SqlDbService $service
     * @param array $settings
     */
    public function __construct( $service = null, $settings = array() )
    {
        parent::__construct( $settings );

        $this->service = $service;
    }

    /**
     * @param string $function
     * @param string $action
     *
     * @throws BadRequestException
     */
    protected function validateStoredFunctionAccess( &$function, $action = null )
    {
        // finally check that the current user has privileges to access this function
        $_resource = static::RESOURCE_NAME;
        $this->service->validateResourceAccess( $_resource, $function, $action );
    }

    /**
     * @return array|bool
     * @throws BadRequestException
     */
    protected function handleGET()
    {
        $options = $this->request->query();
        if ( empty( $this->resource ) )
        {
            $_namesOnly = ArrayUtils::getBool( $options, 'names_only' );
            $_refresh = ArrayUtils::getBool( $options, 'refresh' );
            $_result = $this->listFunctions( $_namesOnly, $_refresh );

            return array( 'resource' => $_result );
        }

        $payload = $this->request->getPayloadData();
        if ( false !== strpos( $this->resource, '(' ) )
        {
            $_inlineParams = strstr( $this->resource, '(' );
            $_name = rtrim( strstr( $this->resource, '(', true ) );
            $_params = ArrayUtils::get( $payload, 'params', trim( $_inlineParams, '()' ) );
        }
        else
        {
            $_name = $this->resource;
            $_params = ArrayUtils::get( $payload, 'params', array() );
        }

        $_returns = ArrayUtils::get( $payload, 'returns' );
        $_wrapper = ArrayUtils::get( $payload, 'wrapper' );
        $_schema = ArrayUtils::get( $payload, 'schema' );

        return $this->callFunction( $_name, $_params, $_returns, $_schema, $_wrapper );
    }

    /**
     * @return array|bool
     * @throws BadRequestException
     */
    protected function handlePOST()
    {
        $payload = $this->request->getPayloadData();
        if ( false !== strpos( $this->resource, '(' ) )
        {
            $_inlineParams = strstr( $this->resource, '(' );
            $_name = rtrim( strstr( $this->resource, '(', true ) );
            $_params = ArrayUtils::get( $payload, 'params', trim( $_inlineParams, '()' ) );
        }
        else
        {
            $_name = $this->resource;
            $_params = ArrayUtils::get( $payload, 'params', array() );
        }

        $_returns = ArrayUtils::get( $payload, 'returns' );
        $_wrapper = ArrayUtils::get( $payload, 'wrapper' );
        $_schema = ArrayUtils::get( $payload, 'schema' );

        return $this->callFunction( $_name, $_params, $_returns, $_schema, $_wrapper );
    }

    /**
     * @param boolean $names_only
     * @param boolean $refresh
     *
     * @throws \Exception
     * @return array
     */
    public function listFunctions( $names_only = false, $refresh = false )
    {
        $_resources = array();

        try
        {
            $_names = $this->service->getConnection()->getSchema()->getFunctionNames( '', $refresh );
            natcasesort( $_names );
            $_result = array_values( $_names );

            foreach ( $_result as $_name )
            {
                $_access = $this->getPermissions( static::RESOURCE_NAME . '/' . $_name );
                if ( !empty( $_access ) )
                {
                    if ( $names_only )
                    {
                        $_resources[] = $_name;
                    }
                    else
                    {
                        $_resources[] = array( 'name' => $_name, 'access' => $_access );
                    }
                }
            }

            return $_resources;
        }
        catch ( RestException $_ex )
        {
            throw $_ex;
        }
        catch ( \Exception $_ex )
        {
            throw new InternalServerErrorException( "Failed to list database stored functions for this service.\n{$_ex->getMessage()}" );
        }
    }

    /**
     * @param string $name
     * @param array  $params
     * @param string $returns
     * @param array  $schema
     * @param string $wrapper
     *
     * @throws \Exception
     * @return array
     */
    public function callFunction( $name, $params = null, $returns = null, $schema = null, $wrapper = null )
    {
        if ( empty( $name ) )
        {
            throw new BadRequestException( 'Stored function name can not be empty.' );
        }

        if ( false === $params = SqlDbUtilities::validateAsArray( $params, ',', true ) )
        {
            $params = array();
        }

        foreach ( $params as $_key => $_param )
        {
            // overcome shortcomings of passed in data
            if ( is_array( $_param ) )
            {
                if ( null === $_pName = ArrayUtils::get( $_param, 'name', null, false, true ) )
                {
                    $params[$_key]['name'] = "p$_key";
                }
            }
            else
            {
                $params[$_key] = array( 'name' => "p$_key", 'value' => $_param );
            }
        }

        try
        {
            $_result = $this->service->getConnection()->getSchema()->callFunction( $name, $params );

            if ( !empty( $returns ) && ( 0 !== strcasecmp( 'TABLE', $returns ) ) )
            {
                // result could be an array of array of one value - i.e. multi-dataset format with just a single value
                if ( is_array( $_result ) )
                {
                    $_result = current( $_result );
                    if ( is_array( $_result ) )
                    {
                        $_result = current( $_result );
                    }
                }
                $_result = SqlDbUtilities::formatValue( $_result, $returns );
            }

            // convert result field values to types according to schema received
            if ( is_array( $schema ) && is_array( $_result ) )
            {
                foreach ( $_result as &$_row )
                {
                    if ( is_array( $_row ) )
                    {
                        if ( isset( $_row[0] ) )
                        {
                            //  Multi-row set, dig a little deeper
                            foreach ( $_row as &$_sub )
                            {
                                if ( is_array( $_sub ) )
                                {
                                    foreach ( $_sub as $_key => $_value )
                                    {
                                        if ( null !== $_type = ArrayUtils::get( $schema, $_key, null, false, true ) )
                                        {
                                            $_sub[$_key] = SqlDbUtilities::formatValue( $_value, $_type );
                                        }
                                    }
                                }
                            }
                        }
                        else
                        {
                            foreach ( $_row as $_key => $_value )
                            {
                                if ( null !== $_type = ArrayUtils::get( $schema, $_key, null, false, true ) )
                                {
                                    $_row[$_key] = SqlDbUtilities::formatValue( $_value, $_type );
                                }
                            }
                        }
                    }
                }
            }

            // wrap the result set if desired
            if ( !empty( $wrapper ) )
            {
                $_result = array( $wrapper => $_result );
            }

            return $_result;
        }
        catch ( \Exception $ex )
        {
            throw new InternalServerErrorException( "Failed to call database stored procedure.\n{$ex->getMessage()}" );
        }
    }
}