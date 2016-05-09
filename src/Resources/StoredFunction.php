<?php
namespace DreamFactory\Core\SqlDb\Resources;

use DreamFactory\Core\Components\DataValidator;
use DreamFactory\Core\Enums\VerbsMask;
use DreamFactory\Core\Events\ResourcePostProcess;
use DreamFactory\Core\Events\ResourcePreProcess;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Exceptions\RestException;
use DreamFactory\Core\Resources\BaseDbResource;
use DreamFactory\Core\SqlDb\Components\SqlDbResource;
use DreamFactory\Core\Utility\DataFormatter;
use DreamFactory\Core\Utility\Session;
use DreamFactory\Library\Utility\Inflector;

class StoredFunction extends BaseDbResource
{
    //*************************************************************************
    //	Traits
    //*************************************************************************

    use DataValidator;
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
     * Runs pre process tasks/scripts
     */
    protected function preProcess()
    {
        switch (count($this->resourceArray)) {
            case 0:
                parent::preProcess();
                break;
            case 1:
                // Try the generic table event
                /** @noinspection PhpUnusedLocalVariableInspection */
                $results = \Event::fire(
                    new ResourcePreProcess(
                        $this->getServiceName(), $this->getFullPathName('.') . '.{function_name}', $this->request,
                        $this->resourcePath
                    )
                );
                // Try the actual table name event
                /** @noinspection PhpUnusedLocalVariableInspection */
                $results = \Event::fire(
                    new ResourcePreProcess(
                        $this->getServiceName(), $this->getFullPathName('.') . '.' . $this->resourceArray[0],
                        $this->request,
                        $this->resourcePath
                    )
                );
                break;
            default:
                // Do nothing is all we got?
                break;
        }
    }

    /**
     * Runs post process tasks/scripts
     */
    protected function postProcess()
    {
        switch (count($this->resourceArray)) {
            case 0:
                parent::postProcess();
                break;
            case 1:
                $event = new ResourcePostProcess(
                    $this->getServiceName(), $this->getFullPathName('.') . '.' . $this->resourceArray[0],
                    $this->request,
                    $this->response,
                    $this->resourcePath
                );
                /** @noinspection PhpUnusedLocalVariableInspection */
                $results = \Event::fire($event);
                // copy the event response back to this response
                $this->response = $event->response;

                $event = new ResourcePostProcess(
                    $this->getServiceName(), $this->getFullPathName('.') . '.{function_name}', $this->request,
                    $this->response,
                    $this->resourcePath
                );
                /** @noinspection PhpUnusedLocalVariableInspection */
                $results = \Event::fire($event);

                $this->response = $event->response;
                break;
            default:
                // Do nothing is all we got?
                break;
        }
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
            $params = array_get($payload, 'params', trim($inlineParams, '()'));
        } else {
            $name = $this->resource;
            $params = array_get($payload, 'params', []);
        }

        $returns = array_get($payload, 'returns');
        $wrapper = array_get($payload, 'wrapper');
        $schema = array_get($payload, 'schema');

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
            $params = array_get($payload, 'params', trim($inlineParams, '()'));
        } else {
            $name = $this->resource;
            $params = array_get($payload, 'params', []);
        }

        $returns = array_get($payload, 'returns');
        $wrapper = array_get($payload, 'wrapper');
        $schema = array_get($payload, 'schema');

        return $this->callFunction($name, $params, $returns, $schema, $wrapper);
    }

    /**
     * {@inheritdoc}
     */
    public function getResourceName()
    {
        return static::RESOURCE_NAME;
    }

    /**
     * {@inheritdoc}
     */
    public function listResources($schema = null, $refresh = false)
    {
        try {
            return $this->schema->getFunctionNames($schema, $refresh);
        } catch (RestException $ex) {
            throw $ex;
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to list database stored functions for this service.\n{$ex->getMessage()}");
        }
    }

    /**
     * {@inheritdoc}
     */
    public function getResources($only_handlers = false)
    {
        if ($only_handlers) {
            return [];
        }

        $refresh = $this->request->getParameterAsBool('refresh');
        $schema = $this->request->getParameter('schema', '');

        $result = $this->listResources($schema, $refresh);

        $resources = [];
        foreach ($result as $name) {
            $access = $this->getPermissions($name);
            if (!empty($access)) {
                $resources[] = ['name' => $name, 'access' => VerbsMask::maskToArray($access)];
            }
        }

        return $resources;
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

        if (false === $params = static::validateAsArray($params, ',', true)) {
            $params = [];
        }

        Session::replaceLookups($params);
        foreach ($params as $key => $param) {
            // overcome shortcomings of passed in data
            if (is_array($param)) {
                if (null === $pName = array_get($param, 'name')) {
                    $params[$key]['name'] = "p$key";
                }
            } else {
                $params[$key] = ['name' => "p$key", 'value' => $param];
            }
        }

        try {
            $result = $this->schema->callFunction($name, $params);

            if (!empty($returns) && (0 !== strcasecmp('TABLE', $returns))) {
                // result could be an array of array of one value - i.e. multi-dataset format with just a single value
                if (is_array($result)) {
                    $result = current($result);
                    if (is_array($result)) {
                        $result = current($result);
                    }
                }
                $result = DataFormatter::formatValue($result, $returns);
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
                                        if (null !== $type = array_get($schema, $key)) {
                                            $sub[$key] = DataFormatter::formatValue($value, $type);
                                        }
                                    }
                                }
                            }
                        } else {
                            foreach ($row as $key => $value) {
                                if (null !== $type = array_get($schema, $key)) {
                                    $row[$key] = DataFormatter::formatValue($value, $type);
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

    public static function getApiDocInfo($service, array $resource = [])
    {
        $serviceName = strtolower($service);
        $capitalized = Inflector::camelize($service);
        $class = trim(strrchr(static::class, '\\'), '\\');
        $resourceName = strtolower(array_get($resource, 'name', $class));
        $path = '/' . $serviceName . '/' . $resourceName;
        $eventPath = $serviceName . '.' . $resourceName;
        $base = parent::getApiDocInfo($service, $resource);

        $apis = [
            $path . '/{function_name}' => [
                'get'  => [
                    'tags'              => [$serviceName],
                    'summary'           => 'call' . $capitalized . 'StoredFunction() - Call a stored function.',
                    'operationId'       => 'call' . $capitalized . 'StoredFunction',
                    'description'       =>
                        'Call a stored function with no parameters. ' .
                        'Set an optional wrapper for the returned data set. ',
                    'x-publishedEvents' => [$eventPath . '.{function_name}.call', $eventPath . '.function_called',],
                    'parameters'        => [
                        [
                            'name'        => 'function_name',
                            'description' => 'Name of the stored function to call.',
                            'type'        => 'string',
                            'in'          => 'path',
                            'required'    => true,
                        ],
                        [
                            'name'        => 'wrapper',
                            'description' => 'Add this wrapper around the expected data set before returning.',
                            'type'        => 'string',
                            'in'          => 'query',
                            'required'    => false,
                        ],
                        [
                            'name'        => 'returns',
                            'description' => 'If returning a single value, use this to set the type of that value.',
                            'type'        => 'string',
                            'in'          => 'query',
                            'required'    => false,
                        ],
                    ],
                    'responses'         => [
                        '200'     => [
                            'description' => 'Success',
                            'schema'      => ['$ref' => '#/definitions/StoredFunctionResponse']
                        ],
                        'default' => [
                            'description' => 'Error',
                            'schema'      => ['$ref' => '#/definitions/Error']
                        ]
                    ],
                ],
                'post' => [
                    'tags'              => [$serviceName],
                    'summary'           => 'call' .
                        $capitalized .
                        'StoredFunctionWithParams() - Call a stored function.',
                    'operationId'       => 'call' . $capitalized . 'StoredFunctionWithParams',
                    'description'       =>
                        'Call a stored function with parameters. ' .
                        'Set an optional wrapper and schema for the returned data set. ',
                    'x-publishedEvents' => [$eventPath . '.{function_name}.call', $eventPath . '.function_called',],
                    'parameters'        => [
                        [
                            'name'        => 'function_name',
                            'description' => 'Name of the stored function to call.',
                            'type'        => 'string',
                            'in'          => 'path',
                            'required'    => true,
                        ],
                        [
                            'name'        => 'body',
                            'description' => 'Data containing input parameters to pass to function.',
                            'schema'      => ['$ref' => '#/definitions/StoredFunctionRequest'],
                            'in'          => 'body',
                            'required'    => true,
                        ],
                        [
                            'name'        => 'wrapper',
                            'description' => 'Add this wrapper around the expected data set before returning.',
                            'type'        => 'string',
                            'in'          => 'query',
                            'required'    => false,
                        ],
                        [
                            'name'        => 'returns',
                            'description' => 'If returning a single value, use this to set the type of that value.',
                            'type'        => 'string',
                            'in'          => 'query',
                            'required'    => false,
                        ],
                    ],
                    'responses'         => [
                        '200'     => [
                            'description' => 'Success',
                            'schema'      => ['$ref' => '#/definitions/StoredFunctionResponse']
                        ],
                        'default' => [
                            'description' => 'Error',
                            'schema'      => ['$ref' => '#/definitions/Error']
                        ]
                    ],
                ],
            ],
        ];

        $models = [
            'StoredFunctionResponse'     => [
                'type'       => 'object',
                'properties' => [
                    '_wrapper_if_supplied_' => [
                        'type'        => 'array',
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
            'StoredFunctionRequest'      => [
                'type'       => 'object',
                'properties' => [
                    'params'  => [
                        'type'        => 'array',
                        'description' => 'Optional array of input and output parameters.',
                        'items'       => [
                            '$ref' => '#/definitions/StoredFunctionParam',
                        ],
                    ],
                    'schema'  => [
                        '$ref' => '#/definitions/StoredFunctionResultSchema',
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
            'StoredFunctionParam'        => [
                'type'       => 'object',
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
            'StoredFunctionResultSchema' => [
                'type'       => 'object',
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

        $base['paths'] = array_merge($base['paths'], $apis);
        $base['definitions'] = array_merge($base['definitions'], $models);

        return $base;
    }
}