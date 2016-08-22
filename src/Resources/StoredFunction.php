<?php
namespace DreamFactory\Core\SqlDb\Resources;

use DreamFactory\Core\Components\DataValidator;
use DreamFactory\Core\Database\Schema\FunctionSchema;
use DreamFactory\Core\Enums\ApiOptions;
use DreamFactory\Core\Enums\VerbsMask;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Exceptions\NotFoundException;
use DreamFactory\Core\Exceptions\RestException;
use DreamFactory\Core\Resources\BaseDbResource;
use DreamFactory\Core\SqlDb\Components\SqlDbResource;
use DreamFactory\Core\Utility\DataFormatter;
use DreamFactory\Core\Utility\ResourcesWrapper;
use DreamFactory\Core\Utility\Session;
use DreamFactory\Library\Utility\Enums\Verbs;
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
    /**
     * Replacement tag for dealing with function events
     */
    const EVENT_IDENTIFIER = '{function_name}';

    //*************************************************************************
    //	Members
    //*************************************************************************

    /**
     * @type array Any parameters passed inline, i.e. /_proc/myproc(1, 2, 3)
     */
    protected $inlineParams = [];

    //*************************************************************************
    //	Methods
    //*************************************************************************

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
        /** @type FunctionSchema[] $result */
        $result = $this->schema->getFunctionNames($schema, $refresh);
        $resources = [];
        foreach ($result as $proc) {
            $name = $proc->publicName;
            if (!empty($this->getPermissions($name))) {
                $resources[] = $name;
            }
        }

        return $resources;
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

        /** @type FunctionSchema[] $result */
        $result = $this->schema->getFunctionNames($schema, $refresh);

        $resources = [];
        foreach ($result as $function) {
            $access = $this->getPermissions($function->publicName);
            if (!empty($access)) {
                $temp = $function->toArray();
                $temp['access'] = VerbsMask::maskToArray($access);
                $resources[] = $temp;
            }
        }

        return $resources;
    }

    /**
     * @param string $function
     * @param string $action
     *
     * @throws BadRequestException
     */
    protected function validateStoredFunctionAccess(&$function, $action = null)
    {
        // check that the current user has privileges to access this function
        $this->checkPermission($action, $function);
    }

    protected function setResourceMembers($resourcePath = null)
    {
        if (false !== strpos($resourcePath, '(')) {
            $this->inlineParams = trim(strstr($resourcePath, '('), '()');
            $resourcePath = rtrim(strstr($resourcePath, '(', true));
        }

        return parent::setResourceMembers($resourcePath);
    }

    protected function getEventName()
    {
        $suffix = '';
        switch (count($this->resourceArray)) {
            case 1:
                $suffix = '.' . static::EVENT_IDENTIFIER;
                break;
        }

        return parent::getEventName() . $suffix;
    }


    protected function firePreProcessEvent($name = null, $resource = null)
    {
        // fire default first
        // Try the generic table event
        parent::firePreProcessEvent($name, $resource);

        // also fire more specific event
        // Try the actual table name event
        switch (count($this->resourceArray)) {
            case 1:
                parent::firePreProcessEvent(str_replace(static::EVENT_IDENTIFIER, $this->resourceArray[0],
                    $this->getEventName()), $resource);
                break;
        }
    }

    protected function firePostProcessEvent($name = null, $resource = null)
    {
        // fire default first
        // Try the generic table event
        parent::firePostProcessEvent($name, $resource);

        // also fire more specific event
        // Try the actual table name event
        switch (count($this->resourceArray)) {
            case 1:
                parent::firePostProcessEvent(str_replace(static::EVENT_IDENTIFIER, $this->resourceArray[0],
                    $this->getEventName()), $resource);
                break;
        }
    }

    protected function fireFinalEvent($name = null, $resource = null)
    {
        // fire default first
        // Try the generic table event
        parent::fireFinalEvent($name, $resource);

        // also fire more specific event
        // Try the actual table name event
        switch (count($this->resourceArray)) {
            case 1:
                parent::fireFinalEvent(str_replace(static::EVENT_IDENTIFIER, $this->resourceArray[0],
                    $this->getEventName()), $resource);
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
            $names = $this->request->getParameter(ApiOptions::IDS);
            if (empty($names)) {
                $names = ResourcesWrapper::unwrapResources($this->request->getPayloadData());
            }

            if (!empty($names)) {
                $refresh = $this->request->getParameterAsBool('refresh');
                $result = $this->describeFunctions($names, $refresh);

                return ResourcesWrapper::wrapResources($result);
            } else {
                return parent::handleGET();
            }
        }

        return $this->callFunction();
    }

    /**
     * @return array|bool
     * @throws BadRequestException
     */
    protected function handlePOST()
    {
        if (!empty($this->resource)) {
            return $this->callFunction();
        }

        return parent::handlePOST();
    }

    /**
     * Get multiple functions and their properties
     *
     * @param string | array $names   Function names comma-delimited string or array
     * @param bool           $refresh Force a refresh of the schema from the database
     *
     * @return array
     * @throws \Exception
     */
    public function describeFunctions($names, $refresh = false)
    {
        $names = static::validateAsArray(
            $names,
            ',',
            true,
            'The request contains no valid function names or properties.'
        );

        $out = [];
        foreach ($names as $name) {
            $out[] = $this->describeFunction($name, $refresh);
        }

        return $out;
    }

    /**
     * Get any properties related to the function
     *
     * @param string | array $name    Function name or defining properties
     * @param bool           $refresh Force a refresh of the schema from the database
     *
     * @return array
     * @throws \Exception
     */
    public function describeFunction($name, $refresh = false)
    {
        $name = (is_array($name) ? array_get($name, 'name') : $name);
        if (empty($name)) {
            throw new BadRequestException('Function name can not be empty.');
        }

        $this->checkPermission(Verbs::GET, $name);

        try {
            $procedure = $this->schema->getFunction($name, $refresh);
            if (!$procedure) {
                throw new NotFoundException("Function '$name' does not exist in the database.");
            }

            $result = $procedure->toArray();
            $result['access'] = $this->getPermissions($name);

            return $result;
        } catch (RestException $ex) {
            throw $ex;
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to query database schema.\n{$ex->getMessage()}");
        }
    }

    /**
     * @throws \Exception
     * @return array
     */
    protected function callFunction()
    {
        $payload = $this->request->getPayloadData();
        // check payload first, then inline, then URL param
        $params = array_get($payload, 'params', $this->inlineParams);
        if (empty($params)) {
            $params = $this->request->getParameters();
        }
        if (false === $params = static::validateAsArray($params, ',')) {
            $params = [];
        }

        Session::replaceLookups($params);

        try {
            $result = $this->schema->callFunction($this->resource, $params);
        } catch (RestException $ex) {
            throw $ex;
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to call database stored function.\n{$ex->getMessage()}");
        }

        $returns = array_get($payload, 'returns', $this->request->getParameter('returns'));
        $schema = array_get($payload, 'schema');

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
        if (is_array($schema) && !empty($result)) {
            if (is_array($result)) {
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
            } elseif (is_array($result)) {
                foreach ($result as $key => $value) {
                    if (null !== $type = array_get($schema, $key)) {
                        $result[$key] = DataFormatter::formatValue($value, $type);
                    }
                }
            }
        }

        return $result;
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
                    'description'       => 'Call a stored function with no parameters. ',
                    'parameters'        => [
                        [
                            'name'        => 'function_name',
                            'description' => 'Name of the stored function to call.',
                            'type'        => 'string',
                            'in'          => 'path',
                            'required'    => true,
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
                    'description'       => 'Call a stored function with parameters. ',
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
                    '_out_param_name_' => [
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
                    'returns' => [
                        'type'        => 'string',
                        'description' => 'If returning a single value, use this to set the type of that value, same as URL parameter.',
                    ],
                ],
            ],
            'StoredFunctionParam'        => [
                'type'       => 'object',
                'properties' => [
                    'name'  => [
                        'type'        => 'string',
                        'description' =>
                            'Name of the parameter, required for OUT and INOUT types, ' .
                            'must be the same as the stored procedure\'s parameter name.',
                    ],
                    'value' => [
                        'type'        => 'string',
                        'description' => 'Value of the parameter, used for the IN and INOUT types, defaults to NULL.',
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