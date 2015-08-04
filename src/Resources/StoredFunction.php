<?php
namespace DreamFactory\Core\SqlDb\Resources;

use DreamFactory\Core\Enums\ApiOptions;
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
            $inlineParams = strstr($this->resource, '(');
            $name = rtrim(strstr($this->resource, '(', true));
            $params = ArrayUtils::get($payload, 'params', trim($inlineParams, '()'));
        } else {
            $name = $this->resource;
            $params = ArrayUtils::get($payload, 'params', []);
        }

        $returns = ArrayUtils::get($payload, 'returns');
        $wrapper = ArrayUtils::get($payload, 'wrapper');
        $schema = ArrayUtils::get($payload, 'schema');

        return $this->callFunction($name, $params, $returns, $schema, $wrapper);
    }

    /**
     * @return array|bool
     * @throws BadRequestException
     */
    protected function handlePOST()
    {
        $payload = $this->request->getPayloadData();
        if (false !== strpos($this->resource, '(')) {
            $inlineParams = strstr($this->resource, '(');
            $name = rtrim(strstr($this->resource, '(', true));
            $params = ArrayUtils::get($payload, 'params', trim($inlineParams, '()'));
        } else {
            $name = $this->resource;
            $params = ArrayUtils::get($payload, 'params', []);
        }

        $returns = ArrayUtils::get($payload, 'returns');
        $wrapper = ArrayUtils::get($payload, 'wrapper');
        $schema = ArrayUtils::get($payload, 'schema');

        return $this->callFunction($name, $params, $returns, $schema, $wrapper);
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
            $names = $this->dbConn->getSchema()->getFunctionNames($schema, $refresh);
            natcasesort($names);

            return array_values($names);
        } catch (RestException $ex) {
            throw $ex;
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to list database stored functions for this service.\n{$ex->getMessage()}");
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

        foreach ($params as $key => $param) {
            // overcome shortcomings of passed in data
            if (is_array($param)) {
                if (null === $pName = ArrayUtils::get($param, 'name', null, false)) {
                    $params[$key]['name'] = "p$key";
                }
            } else {
                $params[$key] = ['name' => "p$key", 'value' => $param];
            }
        }

        try {
            $result = $this->dbConn->getSchema()->callFunction($name, $params);

            if (!empty($returns) && (0 !== strcasecmp('TABLE', $returns))) {
                // result could be an array of array of one value - i.e. multi-dataset format with just a single value
                if (is_array($result)) {
                    $result = current($result);
                    if (is_array($result)) {
                        $result = current($result);
                    }
                }
                $result = DbUtilities::formatValue($result, $returns);
            }

            // convert result field values to types according to schema received
            if (is_array($schema) && is_array($result)) {
                foreach ($result as &$row) {
                    if (is_array($row)) {
                        if (isset($row[0])) {
                            //  Multi-row set, dig a little deeper
                            foreach ($row as &$sub) {
                                if (is_array($sub)) {
                                    foreach ($sub as $key => $value) {
                                        if (null !== $type = ArrayUtils::get($schema, $key, null, false)) {
                                            $sub[$key] = DbUtilities::formatValue($value, $type);
                                        }
                                    }
                                }
                            }
                        } else {
                            foreach ($row as $key => $value) {
                                if (null !== $type = ArrayUtils::get($schema, $key, null, false)) {
                                    $row[$key] = DbUtilities::formatValue($value, $type);
                                }
                            }
                        }
                    }
                }
            }

            // wrap the result set if desired
            if (!empty($wrapper)) {
                $result = [$wrapper => $result];
            }

            return $result;
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to call database stored procedure.\n{$ex->getMessage()}");
        }
    }

    public function getApiDocInfo()
    {
        $path = '/' . $this->getServiceName() . '/' . $this->getFullPathName();
        $eventPath = $this->getServiceName() . '.' . $this->getFullPathName('.');
        $base = parent::getApiDocInfo();

        $apis = [
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
                            ApiOptions::documentOption(ApiOptions::REFRESH),
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
                            ApiOptions::documentOption(ApiOptions::FIELDS),
                            ApiOptions::documentOption(ApiOptions::REFRESH),
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

        $models = [
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

        $base['apis'] = array_merge($base['apis'], $apis);
        $base['models'] = array_merge($base['models'], $models);

        return $base;
    }
}