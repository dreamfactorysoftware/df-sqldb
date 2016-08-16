<?php
namespace DreamFactory\Core\SqlDb\Resources;

use DreamFactory\Core\Components\DataValidator;
use DreamFactory\Core\Database\Schema\ProcedureSchema;
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

class StoredProcedure extends BaseDbResource
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
    const RESOURCE_NAME = '_proc';
    /**
     * Replacement tag for dealing with procedure events
     */
    const EVENT_IDENTIFIER = '{procedure_name}';

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
        /** @type ProcedureSchema[] $result */
        $result = $this->schema->getProcedureNames($schema, $refresh);
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

        /** @type ProcedureSchema[] $result */
        $result = $this->schema->getProcedureNames($schema, $refresh);
        $resources = [];
        foreach ($result as $procedure) {
            $access = $this->getPermissions($procedure->publicName);
            if (!empty($access)) {
                $temp = $procedure->toArray();
                $temp['access'] = VerbsMask::maskToArray($access);
                $resources[] = $temp;
            }
        }

        return $resources;
    }

    /**
     * @param string $procedure
     * @param string $action
     *
     * @throws BadRequestException
     */
    protected function validateStoredProcedureAccess(&$procedure, $action = null)
    {
        // check that the current user has privileges to access this function
        $this->checkPermission($action, $procedure);
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
                $result = $this->describeProcedures($names, $refresh);

                return ResourcesWrapper::wrapResources($result);
            } else {
                return parent::handleGET();
            }
        }

        return $this->callProcedure();
    }

    /**
     * @return array|bool
     * @throws BadRequestException
     */
    protected function handlePOST()
    {
        if (!empty($this->resource)) {
            return $this->callProcedure();
        }

        return parent::handlePOST();
    }

    /**
     * Get multiple procedures and their properties
     *
     * @param string | array $names   Procedure names comma-delimited string or array
     * @param bool           $refresh Force a refresh of the schema from the database
     *
     * @return array
     * @throws \Exception
     */
    protected function describeProcedures($names, $refresh = false)
    {
        $names = static::validateAsArray(
            $names,
            ',',
            true,
            'The request contains no valid procedure names or properties.'
        );

        $out = [];
        foreach ($names as $name) {
            $out[] = $this->describeProcedure($name, $refresh);
        }

        return $out;
    }

    /**
     * Get any properties related to the procedure
     *
     * @param string | array $name    Procedure name or defining properties
     * @param bool           $refresh Force a refresh of the schema from the database
     *
     * @return array
     * @throws \Exception
     */
    protected function describeProcedure($name, $refresh = false)
    {
        $name = (is_array($name) ? array_get($name, 'name') : $name);
        if (empty($name)) {
            throw new BadRequestException('Procedure name can not be empty.');
        }

        $this->checkPermission(Verbs::GET, $name);

        try {
            $procedure = $this->schema->getProcedure($name, $refresh);
            if (!$procedure) {
                throw new NotFoundException("Procedure '$name' does not exist in the database.");
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
    protected function callProcedure()
    {
        $payload = $this->request->getPayloadData();
        $params = array_get($payload, 'params', $this->inlineParams);
        if (empty($params)) {
            $params = $this->request->getParameters();
        }
        if (false === $params = static::validateAsArray($params, ',')) {
            $params = [];
        }

        Session::replaceLookups($params);

        $outParams = [];
        try {
            $result = $this->schema->callProcedure($this->resource, $params, $outParams);
        } catch (RestException $ex) {
            throw $ex;
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to call database stored procedure.\n{$ex->getMessage()}");
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
            } else {
                foreach ($result as $key => $value) {
                    if (null !== $type = array_get($schema, $key)) {
                        $result[$key] = DataFormatter::formatValue($value, $type);
                    }
                }
            }
        }

        // wrap the result set if desired
        if (!empty($outParams)) {
            foreach ($outParams as $key => $value) {
                if (null !== $type = array_get($schema, $key)) {
                    $outParams[$key] = DataFormatter::formatValue($value, $type);
                }
            }
            if (!empty($result)) {
                // must be wrapped
                $wrapper =
                    array_get($payload, 'wrapper',
                        $this->request->getParameter('wrapper', config('resources_wrapper', 'resource')));
                $result = [$wrapper => $result];
            }
            $result = array_merge($result, $outParams);
        } elseif (!empty($result)) {
            // want it wrapped?
            if (!empty($wrapper = array_get($payload, 'wrapper', $this->request->getParameter('wrapper')))) {
                $result = [$wrapper => $result];
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
            $path . '/{procedure_name}' => [
                'get'  => [
                    'tags'              => [$serviceName],
                    'summary'           => 'call' . $capitalized . 'StoredProcedure() - Call a stored procedure.',
                    'operationId'       => 'call' . $capitalized . 'StoredProcedure',
                    'description'       =>
                        'Call a stored procedure with no parameters. ' .
                        'Set an optional wrapper for the returned data set. ',
                    'x-publishedEvents' => [
                        $eventPath . '.{procedure_name}.call',
                        $eventPath . '.procedure_called',
                    ],
                    'parameters'        => [
                        [
                            'name'        => 'procedure_name',
                            'description' => 'Name of the stored procedure to call.',
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
                            'schema'      => ['$ref' => '#/definitions/StoredProcedureResponse']
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
                        'StoredProcedureWithParams() - Call a stored procedure.',
                    'operationId'       => 'call' . $capitalized . 'StoredProcedureWithParams',
                    'description'       =>
                        'Call a stored procedure with parameters. ' .
                        'Set an optional wrapper and schema for the returned data set. ',
                    'x-publishedEvents' => [
                        $eventPath . '.{procedure_name}.call',
                        $eventPath . '.procedure_called',
                    ],
                    'parameters'        => [
                        [
                            'name'        => 'procedure_name',
                            'description' => 'Name of the stored procedure to call.',
                            'type'        => 'string',
                            'in'          => 'path',
                            'required'    => true,
                        ],
                        [
                            'name'        => 'body',
                            'description' => 'Data containing in and out parameters to pass to procedure.',
                            'schema'      => ['$ref' => '#/definitions/StoredProcedureRequest'],
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
                            'schema'      => ['$ref' => '#/definitions/StoredProcedureResponse']
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
            'StoredProcedureResponse'     => [
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
            'StoredProcedureRequest'      => [
                'type'       => 'object',
                'properties' => [
                    'params'  => [
                        'type'        => 'array',
                        'description' => 'Optional array of input and output parameters.',
                        'items'       => [
                            '$ref' => '#/definitions/StoredProcedureParam',
                        ],
                    ],
                    'schema'  => [
                        '$ref' => '#/definitions/StoredProcedureResultSchema',
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
            'StoredProcedureParam'        => [
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
            'StoredProcedureResultSchema' => [
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