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

namespace DreamFactory\Rave\SqlDb\Resources;

use DreamFactory\Library\Utility\ArrayUtils;
use DreamFactory\Rave\Exceptions\BadRequestException;
use DreamFactory\Rave\Exceptions\InternalServerErrorException;
use DreamFactory\Rave\Exceptions\RestException;
use DreamFactory\Rave\Resources\BaseDbResource;
use DreamFactory\Rave\Services\Swagger;
use DreamFactory\Rave\SqlDb\Components\SqlDbResource;
use DreamFactory\Rave\SqlDb\Services\SqlDb;
use DreamFactory\Rave\Resources\BaseRestResource;
use DreamFactory\Rave\Utility\DbUtilities;

class StoredFunction extends BaseDbResource
{
    //*************************************************************************
    //	Traits
    //*************************************************************************

    use SqlDbResource;

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

    //*************************************************************************
    //	Methods
    //*************************************************************************

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

            return [ 'resource' => $_result ];
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
            $_params = ArrayUtils::get( $payload, 'params', [] );
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
            $_params = ArrayUtils::get( $payload, 'params', [] );
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
        $_resources = [];

        try
        {
            $_names = $this->dbConn->getSchema()->getFunctionNames( '', $refresh );
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
                        $_resources[] = [ 'name' => $_name, 'access' => $_access ];
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

        if ( false === $params = DbUtilities::validateAsArray( $params, ',', true ) )
        {
            $params = [];
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
                $params[$_key] = [ 'name' => "p$_key", 'value' => $_param ];
            }
        }

        try
        {
            $_result = $this->dbConn->getSchema()->callFunction( $name, $params );

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
                $_result = DbUtilities::formatValue( $_result, $returns );
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
                                            $_sub[$_key] = DbUtilities::formatValue( $_value, $_type );
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
                                    $_row[$_key] = DbUtilities::formatValue( $_value, $_type );
                                }
                            }
                        }
                    }
                }
            }

            // wrap the result set if desired
            if ( !empty( $wrapper ) )
            {
                $_result = [ $wrapper => $_result ];
            }

            return $_result;
        }
        catch ( \Exception $ex )
        {
            throw new InternalServerErrorException( "Failed to call database stored procedure.\n{$ex->getMessage()}" );
        }
    }

    public function getApiDocInfo()
    {
        $_base = parent::getApiDocInfo();

        $_apis = [
            [
                'path'        => '/{api_name}/' . static::RESOURCE_NAME,
                'operations'  => [
                    [
                        'method'           => 'GET',
                        'summary'          => 'getStoredFuncs() - List callable stored functions.',
                        'nickname'         => 'getStoredFuncs',
                        'notes'            => 'List the names of the available stored functions on this database. ',
                        'type'             => 'Resources',
                        'event_name'       => [ '{api_name}.' . static::RESOURCE_NAME . '.list' ],
                        'parameters'       => [
                            [
                                'name'          => 'names_only',
                                'description'   => 'Return only the names of the functions in an array.',
                                'allowMultiple' => false,
                                'type'          => 'boolean',
                                'paramType'     => 'query',
                                'required'      => false,
                                'default'       => false,
                            ],
                            [
                                'name'          => 'refresh',
                                'description'   => 'Refresh any cached copy of the resource list.',
                                'allowMultiple' => false,
                                'type'          => 'boolean',
                                'paramType'     => 'query',
                                'required'      => false,
                            ],
                        ],
                        'responseMessages' => Swagger::getCommonResponses( [ 400, 401, 500 ] ),
                    ],
                ],
                'description' => 'Operations for retrieving callable stored functions.',
            ],
            [
                'path'        => '/{api_name}/' . static::RESOURCE_NAME . '/{function_name}',
                'operations'  => [
                    [
                        'method'           => 'GET',
                        'summary'          => 'callStoredFunc() - Call a stored function.',
                        'nickname'         => 'callStoredFunc',
                        'notes'            => 'Call a stored function with no parameters. ' . 'Set an optional wrapper for the returned data set. ',
                        'type'             => 'StoredProcResponse',
                        'event_name'       => [
                            '{api_name}.' . static::RESOURCE_NAME . '.{function_name}.call',
                            '{api_name}.' . static::RESOURCE_NAME . '.function_called',
                        ],
                        'parameters'       => [
                            [
                                'name'          => 'function_name',
                                'description'   => 'Name of the stored function to call.',
                                'allowMultiple' => false,
                                'type'          => 'string',
                                'paramType'     => 'path',
                                'required'      => true,
                            ],
                            [
                                'name'          => 'wrapper',
                                'description'   => 'Add this wrapper around the expected data set before returning.',
                                'allowMultiple' => false,
                                'type'          => 'string',
                                'paramType'     => 'query',
                                'required'      => false,
                            ],
                            [
                                'name'          => 'returns',
                                'description'   => 'If returning a single value, use this to set the type of that value.',
                                'allowMultiple' => false,
                                'type'          => 'string',
                                'paramType'     => 'query',
                                'required'      => false,
                            ],
                        ],
                        'responseMessages' => Swagger::getCommonResponses(),
                    ],
                    [
                        'method'           => 'POST',
                        'summary'          => 'callStoredFuncWithParams() - Call a stored function.',
                        'nickname'         => 'callStoredFuncWithParams',
                        'notes'            => 'Call a stored function with parameters. ' . 'Set an optional wrapper and schema for the returned data set. ',
                        'type'             => 'StoredProcResponse',
                        'event_name'       => [
                            '{api_name}.' . static::RESOURCE_NAME . '.{function_name}.call',
                            '{api_name}.' . static::RESOURCE_NAME . '.function_called',
                        ],
                        'parameters'       => [
                            [
                                'name'          => 'function_name',
                                'description'   => 'Name of the stored function to call.',
                                'allowMultiple' => false,
                                'type'          => 'string',
                                'paramType'     => 'path',
                                'required'      => true,
                            ],
                            [
                                'name'          => 'body',
                                'description'   => 'Data containing input parameters to pass to function.',
                                'allowMultiple' => false,
                                'type'          => 'StoredProcRequest',
                                'paramType'     => 'body',
                                'required'      => true,
                            ],
                            [
                                'name'          => 'wrapper',
                                'description'   => 'Add this wrapper around the expected data set before returning.',
                                'allowMultiple' => false,
                                'type'          => 'string',
                                'paramType'     => 'query',
                                'required'      => false,
                            ],
                            [
                                'name'          => 'returns',
                                'description'   => 'If returning a single value, use this to set the type of that value.',
                                'allowMultiple' => false,
                                'type'          => 'string',
                                'paramType'     => 'query',
                                'required'      => false,
                            ],
                        ],
                        'responseMessages' => Swagger::getCommonResponses(),
                    ],
                ],
                'description' => 'Operations for SQL database stored functions.',
            ],
        ];

        $_models = [
            'StoredProcResponse'     => [
                'id'         => 'StoredProcResponse',
                'properties' => [
                    '_wrapper_if_supplied_' => [
                        'type'        => 'Array',
                        'description' => 'Array of returned data.',
                        'items'       => [
                            'type' => 'string'
                        ],
                    ],
                    '_out_param_name_'      => [
                        'type'        => 'string',
                        'description' => 'Name and value of any given output parameter.',
                    ],
                ],
            ],
            'StoredProcRequest'      => [
                'id'         => 'StoredProcRequest',
                'properties' => [
                    'params'  => [
                        'type'        => 'array',
                        'description' => 'Optional array of input and output parameters.',
                        'items'       => [
                            '$ref' => 'StoredProcParam',
                        ],
                    ],
                    'schema'  => [
                        'type'        => 'StoredProcResultSchema',
                        'description' => 'Optional name to type pairs to be applied to returned data.',
                    ],
                    'wrapper' => [
                        'type'        => 'string',
                        'description' => 'Add this wrapper around the expected data set before returning, same as URL parameter.',
                    ],
                    'returns' => [
                        'type'        => 'string',
                        'description' => 'If returning a single value, use this to set the type of that value, same as URL parameter.',
                    ],
                ],
            ],
            'StoredProcParam'        => [
                'id'         => 'StoredProcParam',
                'properties' => [
                    'name'       => [
                        'type'        => 'string',
                        'description' =>
                            'Name of the parameter, required for OUT and INOUT types, ' .
                            'must be the same as the stored procedure\'s parameter name.',
                    ],
                    'param_type' => [
                        'type'        => 'string',
                        'description' => 'Parameter type of IN, OUT, or INOUT, defaults to IN.',
                    ],
                    'value'      => [
                        'type'        => 'string',
                        'description' => 'Value of the parameter, used for the IN and INOUT types, defaults to NULL.',
                    ],
                    'type'       => [
                        'type'        => 'string',
                        'description' =>
                            'For INOUT and OUT parameters, the requested type for the returned value, ' .
                            'i.e. integer, boolean, string, etc. Defaults to value type for INOUT and string for OUT.',
                    ],
                    'length'     => [
                        'type'        => 'integer',
                        'format'      => 'int32',
                        'description' =>
                            'For INOUT and OUT parameters, the requested length for the returned value. ' .
                            'May be required by some database drivers.',
                    ],
                ],
            ],
            'StoredProcResultSchema' => [
                'id'         => 'StoredProcResultSchema',
                'properties' => [
                    '_field_name_' => [
                        'type'        => 'string',
                        'description' =>
                            'The name of the returned element where the value is set to the requested type ' .
                            'for the returned value, i.e. integer, boolean, string, etc.',
                    ],
                ],
            ],
        ];

        $_base['apis'] = array_merge( $_base['apis'], $_apis );
        $_base['models'] = array_merge( $_base['models'], $_models );

        return $_base;
    }
}