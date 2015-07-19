<?php
namespace DreamFactory\Core\SqlDb\Resources;

use DreamFactory\Library\Utility\ArrayUtils;
use DreamFactory\Core\Enums\VerbsMask;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Exceptions\RestException;
use DreamFactory\Core\Resources\BaseDbResource;
use DreamFactory\Core\SqlDb\Components\SqlDbResource;
use DreamFactory\Core\Utility\ApiDocUtilities;
use DreamFactory\Core\Utility\DbUtilities;

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
    protected function validateStoredFunctionAccess(&$function, $action = null)
    {
        // check that the current user has privileges to access this function
        $resource = $function;
        if (!empty($function)) {
            $resource .= rtrim((false !== strpos($function, '(')) ? strstr($function, '(', true) : $function);
        }

        $this->checkPermission($action, $resource);
    }

    /**
     * @return array|bool
     * @throws BadRequestException
     */
    protected function handleGET()
    {
        if (empty($this->resource)) {
            return parent::handleGET();
        }

        $payload = $this->request->getPayloadData();
        if (false !== strpos($this->resource, '(')) {
            $_inlineParams = strstr($this->resource, '(');
            $_name = rtrim(strstr($this->resource, '(', true));
            $_params = ArrayUtils::get($payload, 'params', trim($_inlineParams, '()'));
        } else {
            $_name = $this->resource;
            $_params = ArrayUtils::get($payload, 'params', []);
        }

        $_returns = ArrayUtils::get($payload, 'returns');
        $_wrapper = ArrayUtils::get($payload, 'wrapper');
        $_schema = ArrayUtils::get($payload, 'schema');

        return $this->callFunction($_name, $_params, $_returns, $_schema, $_wrapper);
    }

    /**
     * @return array|bool
     * @throws BadRequestException
     */
    protected function handlePOST()
    {
        $payload = $this->request->getPayloadData();
        if (false !== strpos($this->resource, '(')) {
            $_inlineParams = strstr($this->resource, '(');
            $_name = rtrim(strstr($this->resource, '(', true));
            $_params = ArrayUtils::get($payload, 'params', trim($_inlineParams, '()'));
        } else {
            $_name = $this->resource;
            $_params = ArrayUtils::get($payload, 'params', []);
        }

        $_returns = ArrayUtils::get($payload, 'returns');
        $_wrapper = ArrayUtils::get($payload, 'wrapper');
        $_schema = ArrayUtils::get($payload, 'schema');

        return $this->callFunction($_name, $_params, $_returns, $_schema, $_wrapper);
    }

    /**
     * @param null|string $schema
     * @param bool        $refresh
     *
     * @return array
     * @throws InternalServerErrorException
     * @throws RestException
     * @throws \Exception
     *
     */
    protected function listFunctions($schema = null, $refresh = false)
    {
        try {
            $_names = $this->dbConn->getSchema()->getFunctionNames($schema, $refresh);
            natcasesort($_names);

            return array_values($_names);
        } catch (RestException $_ex) {
            throw $_ex;
        } catch (\Exception $_ex) {
            throw new InternalServerErrorException("Failed to list database stored functions for this service.\n{$_ex->getMessage()}");
        }
    }

    public function getResources($only_handlers = false)
    {
        if ($only_handlers) {
            return [];
        }

        $refresh = $this->request->getParameterAsBool('refresh');
        $schema = $this->request->getParameter('schema', '');

        $result = $this->listFunctions($schema, $refresh);

        $resources = [];
        foreach ($result as $name) {
            $access = $this->getPermissions($name);
            if (!empty($access)) {
                $resources[] = ['name' => $name, 'access' => VerbsMask::maskToArray($access)];
            }
        }

        return $resources;
    }

    public function listAccessComponents($schema = null, $refresh = false)
    {
        $output = [];
        $result = $this->listFunctions($schema, $refresh);
        foreach ($result as $name) {
            $output[] = static::RESOURCE_NAME . '/' . $name;
        }

        return $output;
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
    public function callFunction($name, $params = null, $returns = null, $schema = null, $wrapper = null)
    {
        if (empty($name)) {
            throw new BadRequestException('Stored function name can not be empty.');
        }

        if (false === $params = DbUtilities::validateAsArray($params, ',', true)) {
            $params = [];
        }

        foreach ($params as $_key => $_param) {
            // overcome shortcomings of passed in data
            if (is_array($_param)) {
                if (null === $_pName = ArrayUtils::get($_param, 'name', null, false)) {
                    $params[$_key]['name'] = "p$_key";
                }
            } else {
                $params[$_key] = ['name' => "p$_key", 'value' => $_param];
            }
        }

        try {
            $_result = $this->dbConn->getSchema()->callFunction($name, $params);

            if (!empty($returns) && (0 !== strcasecmp('TABLE', $returns))) {
                // result could be an array of array of one value - i.e. multi-dataset format with just a single value
                if (is_array($_result)) {
                    $_result = current($_result);
                    if (is_array($_result)) {
                        $_result = current($_result);
                    }
                }
                $_result = DbUtilities::formatValue($_result, $returns);
            }

            // convert result field values to types according to schema received
            if (is_array($schema) && is_array($_result)) {
                foreach ($_result as &$_row) {
                    if (is_array($_row)) {
                        if (isset($_row[0])) {
                            //  Multi-row set, dig a little deeper
                            foreach ($_row as &$_sub) {
                                if (is_array($_sub)) {
                                    foreach ($_sub as $_key => $_value) {
                                        if (null !== $_type = ArrayUtils::get($schema, $_key, null, false)) {
                                            $_sub[$_key] = DbUtilities::formatValue($_value, $_type);
                                        }
                                    }
                                }
                            }
                        } else {
                            foreach ($_row as $_key => $_value) {
                                if (null !== $_type = ArrayUtils::get($schema, $_key, null, false)) {
                                    $_row[$_key] = DbUtilities::formatValue($_value, $_type);
                                }
                            }
                        }
                    }
                }
            }

            // wrap the result set if desired
            if (!empty($wrapper)) {
                $_result = [$wrapper => $_result];
            }

            return $_result;
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to call database stored procedure.\n{$ex->getMessage()}");
        }
    }

    public function getApiDocInfo()
    {
        $path = '/' . $this->getServiceName() . '/' . $this->getFullPathName();
        $eventPath = $this->getServiceName() . '.' . $this->getFullPathName('.');
        $_base = parent::getApiDocInfo();

        $_apis = [
            [
                'path'        => $path,
                'operations'  => [
                    [
                        'method'           => 'GET',
                        'summary'          => 'getStoredFuncsList() - List callable stored functions.',
                        'nickname'         => 'getStoredFuncsList',
                        'notes'            => 'List the names of the available stored functions on this database. ',
                        'type'             => 'ResourceList',
                        'event_name'       => [$eventPath . '.list'],
                        'parameters'       => [
                            [
                                'name'          => 'refresh',
                                'description'   => 'Refresh any cached copy of the resource list.',
                                'allowMultiple' => false,
                                'type'          => 'boolean',
                                'paramType'     => 'query',
                                'required'      => false,
                            ],
                        ],
                        'responseMessages' => ApiDocUtilities::getCommonResponses([400, 401, 500]),
                    ],
                    [
                        'method'           => 'GET',
                        'summary'          => 'getStoredFuncs() - List callable stored functions.',
                        'nickname'         => 'getStoredFuncs',
                        'notes'            => 'List the available stored functions on this database. ',
                        'type'             => 'Resources',
                        'event_name'       => [$eventPath . '.list'],
                        'parameters'       => [
                            [
                                'name'          => 'fields',
                                'description'   => 'Return all or specified properties available for each resource.',
                                'allowMultiple' => true,
                                'type'          => 'string',
                                'paramType'     => 'query',
                                'required'      => true,
                                'default'       => '*',
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
                        'responseMessages' => ApiDocUtilities::getCommonResponses([400, 401, 500]),
                    ],
                ],
                'description' => 'Operations for retrieving callable stored functions.',
            ],
            [
                'path'        => $path . '/{function_name}',
                'operations'  => [
                    [
                        'method'           => 'GET',
                        'summary'          => 'callStoredFunc() - Call a stored function.',
                        'nickname'         => 'callStoredFunc',
                        'notes'            =>
                            'Call a stored function with no parameters. ' .
                            'Set an optional wrapper for the returned data set. ',
                        'type'             => 'StoredProcResponse',
                        'event_name'       => [$eventPath . '.{function_name}.call', $eventPath . '.function_called',],
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
                        'responseMessages' => ApiDocUtilities::getCommonResponses(),
                    ],
                    [
                        'method'           => 'POST',
                        'summary'          => 'callStoredFuncWithParams() - Call a stored function.',
                        'nickname'         => 'callStoredFuncWithParams',
                        'notes'            =>
                            'Call a stored function with parameters. ' .
                            'Set an optional wrapper and schema for the returned data set. ',
                        'type'             => 'StoredProcResponse',
                        'event_name'       => [$eventPath . '.{function_name}.call', $eventPath . '.function_called',],
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
                        'responseMessages' => ApiDocUtilities::getCommonResponses(),
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

        $_base['apis'] = array_merge($_base['apis'], $_apis);
        $_base['models'] = array_merge($_base['models'], $_models);

        return $_base;
    }
}