<?php

namespace DreamFactory\Core\SqlDb\Database\Schema;

use DreamFactory\Core\Database\Components\DataReader;
use DreamFactory\Core\Database\Components\Schema;
use DreamFactory\Core\Database\Enums\DbFunctionUses;
use DreamFactory\Core\Database\Enums\FunctionTypes;
use DreamFactory\Core\Database\Schema\ColumnSchema;
use DreamFactory\Core\Database\Schema\FunctionSchema;
use DreamFactory\Core\Database\Schema\ParameterSchema;
use DreamFactory\Core\Database\Schema\ProcedureSchema;
use DreamFactory\Core\Database\Schema\RelationSchema;
use DreamFactory\Core\Database\Schema\RoutineSchema;
use DreamFactory\Core\Database\Schema\TableSchema;
use DreamFactory\Core\Enums\DbResourceTypes;
use DreamFactory\Core\Enums\DbSimpleTypes;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Exceptions\NotFoundException;
use DreamFactory\Core\Models\Service;

/**
 * Schema is the base class for retrieving metadata information.
 *
 */
class SqlSchema extends Schema
{
    /**
     * @const string Quoting characters
     */
    const LEFT_QUOTE_CHARACTER = '"';

    /**
     * @const string Quoting characters
     */
    const RIGHT_QUOTE_CHARACTER = '"';

    /**
     * Default fetch mode for procedures and functions
     */
    const ROUTINE_FETCH_MODE = \PDO::FETCH_NAMED;

    /**
     * @return string|null
     */
    public function getUserSchema()
    {
        return $this->userSchema;
    }

    /**
     * @param string|null $schema
     */
    public function setUserSchema($schema)
    {
        $this->userSchema = $schema;
    }

    /**
     * @return boolean
     */
    public function isDefaultSchemaOnly()
    {
        return $this->defaultSchemaOnly;
    }

    /**
     * @param boolean $defaultSchemaOnly
     */
    public function setDefaultSchemaOnly($defaultSchemaOnly)
    {
        $this->defaultSchemaOnly = $defaultSchemaOnly;
    }

    /**
     * @return mixed
     */
    public function getUserName()
    {
        return $this->connection->getConfig('username');
    }

    /**
     * @param       $query
     * @param array $bindings
     * @param null  $column
     *
     * @return array
     */
    public function selectColumn($query, $bindings = [], $column = null)
    {
        $rows = $this->connection->select($query, $bindings);
        foreach ($rows as $key => $row) {
            if (!empty($column)) {
                $rows[$key] = data_get($row, $column);
            } else {
                $row = (array)$row;
                $rows[$key] = reset($row);
            }
        }

        return $rows;
    }

    /**
     * @param       $query
     * @param array $bindings
     * @param null  $column
     *
     * @return mixed|null
     */
    public function selectValue($query, $bindings = [], $column = null)
    {
        if (null !== $row = $this->connection->selectOne($query, $bindings)) {
            if (!empty($column)) {
                return data_get($row, $column);
            } else {
                $row = (array)$row;

                return reset($row);
            }
        }

        return null;
    }

    /**
     * Quotes a string value for use in a query.
     *
     * @param string $str string to be quoted
     *
     * @return string the properly quoted string
     * @see http://www.php.net/manual/en/function.PDO-quote.php
     */
    public function quoteValue($str)
    {
        if (is_int($str) || is_float($str)) {
            return $str;
        }

        if (($value = $this->connection->getPdo()->quote($str)) !== false) {
            return $value;
        } else  // the driver doesn't support quote (e.g. oci)
        {
            return "'" . addcslashes(str_replace("'", "''", $str), "\000\n\r\\\032") . "'";
        }
    }

    public function getNamingSchema()
    {
        switch (strtolower($this->userSchema)) {
            case null:
            case '':
            case 'all':
            case 'default':
                return $this->getDefaultSchema();
            default:
                return $this->userSchema;
        }
    }

    /**
     * Returns the default schema name for the connection.
     *
     * @param boolean $refresh if we need to refresh schema cache.
     *
     * @return string default schema.
     */
    public function getDefaultSchema($refresh = false)
    {
        if (!$refresh) {
            if (!empty($this->defaultSchema)) {
                return $this->defaultSchema;
            } elseif (null !== $this->defaultSchema = $this->getFromCache('default_schema')) {
                return $this->defaultSchema;
            }
        }

        $this->defaultSchema = $this->findDefaultSchema();
        $this->addToCache('default_schema', $this->defaultSchema, true);

        return $this->defaultSchema;
    }

    /**
     * Return an array of supported schema resource types.
     * @return array
     */
    public function getSupportedResourceTypes()
    {
        return [
            DbResourceTypes::TYPE_SCHEMA,
            DbResourceTypes::TYPE_TABLE,
            DbResourceTypes::TYPE_TABLE_FIELD,
            DbResourceTypes::TYPE_TABLE_RELATIONSHIP,
            DbResourceTypes::TYPE_VIEW,
            DbResourceTypes::TYPE_FUNCTION,
            DbResourceTypes::TYPE_PROCEDURE,
        ];
    }

    /**
     * @param \DreamFactory\Core\Database\Schema\TableSchema $table
     * @param                                                $constraints
     */
    protected function buildTableRelations(TableSchema $table, $constraints)
    {
        $schema = (!empty($table->schemaName)) ? $table->schemaName : $this->getDefaultSchema();
        $defaultSchema = $this->getNamingSchema();
        $constraints2 = $constraints;

        foreach ($constraints as $key => $constraint) {
            $constraint = array_change_key_case((array)$constraint, CASE_LOWER);
            $ts = $constraint['table_schema'];
            $tn = $constraint['table_name'];
            $cn = $constraint['column_name'];
            $rts = $constraint['referenced_table_schema'];
            $rtn = $constraint['referenced_table_name'];
            $rcn = $constraint['referenced_column_name'];
            if ((0 == strcasecmp($tn, $table->resourceName)) && (0 == strcasecmp($ts, $schema))) {
                $name = ($rts == $defaultSchema) ? $rtn : $rts . '.' . $rtn;
                $column = $table->getColumn($cn);
                $table->foreignKeys[strtolower($cn)] = [$name, $rcn];
                if (isset($column)) {
                    $column->isForeignKey = true;
                    $column->refTable = $name;
                    $column->refField = $rcn;
                    if (DbSimpleTypes::TYPE_INTEGER === $column->type) {
                        $column->type = DbSimpleTypes::TYPE_REF;
                    }
                    $table->addColumn($column);
                }

                // Add it to our foreign references as well
                $relation =
                    new RelationSchema([
                        'type'           => RelationSchema::BELONGS_TO,
                        'field'          => $cn,
                        'ref_service_id' => $this->getServiceId(),
                        'ref_table'      => $name,
                        'ref_field'      => $rcn,
                    ]);

                $table->addRelation($relation);
            } elseif ((0 == strcasecmp($rtn, $table->resourceName)) && (0 == strcasecmp($rts, $schema))) {
                $name = ($ts == $defaultSchema) ? $tn : $ts . '.' . $tn;
                $relation =
                    new RelationSchema([
                        'type'           => RelationSchema::HAS_MANY,
                        'field'          => $rcn,
                        'ref_service_id' => $this->getServiceId(),
                        'ref_table'      => $name,
                        'ref_field'      => $cn,
                    ]);

                $table->addRelation($relation);

                // if other has foreign keys to other tables, we can say these are related as well
                foreach ($constraints2 as $key2 => $constraint2) {
                    if (0 != strcasecmp($key, $key2)) // not same key
                    {
                        $constraint2 = array_change_key_case((array)$constraint2, CASE_LOWER);
                        $ts2 = $constraint2['table_schema'];
                        $tn2 = $constraint2['table_name'];
                        $cn2 = $constraint2['column_name'];
                        if ((0 == strcasecmp($ts2, $ts)) && (0 == strcasecmp($tn2, $tn))
                        ) {
                            $rts2 = $constraint2['referenced_table_schema'];
                            $rtn2 = $constraint2['referenced_table_name'];
                            $rcn2 = $constraint2['referenced_column_name'];
                            if ((0 != strcasecmp($rts2, $schema)) || (0 != strcasecmp($rtn2, $table->resourceName))
                            ) {
                                $name2 = ($rts2 == $schema) ? $rtn2 : $rts2 . '.' . $rtn2;
                                // not same as parent, i.e. via reference back to self
                                // not the same key
                                $relation =
                                    new RelationSchema([
                                        'type'                => RelationSchema::MANY_MANY,
                                        'field'               => $rcn,
                                        'ref_service_id'      => $this->getServiceId(),
                                        'ref_table'           => $name2,
                                        'ref_field'           => $rcn2,
                                        'junction_service_id' => $this->getServiceId(),
                                        'junction_table'      => $name,
                                        'junction_field'      => $cn,
                                        'junction_ref_field'  => $cn2
                                    ]);

                                $table->addRelation($relation);
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Loads the metadata for the specified table.
     *
     * @param TableSchema $table Any already known info about the table
     */
    protected function loadTable(TableSchema $table)
    {
        $this->loadFields($table);

        $this->loadRelated($table);
    }

    /**
     * Loads the column metadata for the specified table.
     *
     * @param TableSchema $table Any already known info about the table
     */
    protected function loadFields(TableSchema $table)
    {
        if (!empty($columns = $this->findColumns($table))) {
            foreach ($columns as $column) {
                $column = array_change_key_case((array)$column, CASE_LOWER);
                $c = $this->createColumn($column);

                if ($c->isPrimaryKey) {
                    if ($c->autoIncrement) {
                        $table->sequenceName = array_get($column, 'sequence', $c->name);
                        if ((DbSimpleTypes::TYPE_INTEGER === $c->type)) {
                            $c->type = DbSimpleTypes::TYPE_ID;
                        }
                    }
                    if ($table->primaryKey === null) {
                        $table->primaryKey = $c->name;
                    } elseif (is_string($table->primaryKey)) {
                        $table->primaryKey = [$table->primaryKey, $c->name];
                    } else {
                        $table->primaryKey[] = $c->name;
                    }
                }
                $table->addColumn($c);
            }
        }

        // merge db extras
        if (!empty($extras = $this->getSchemaExtrasForFields($table->name))) {
            foreach ($extras as $extra) {
                if (!empty($columnName = array_get($extra, 'field'))) {
                    unset($extra['field']);
                    if (!empty($type = array_get($extra, 'extra_type'))) {
                        $extra['type'] = $type;
                        // upgrade old entries
                        if ('virtual' === $type) {
                            $extra['is_virtual'] = true;
                            if (!empty($functionInfo = array_get($extra, 'db_function'))) {
                                $type = $extra['type'] = array_get($functionInfo, 'type', DbSimpleTypes::TYPE_STRING);
                                if ($function = array_get($functionInfo, 'function')) {
                                    $extra['db_function'] = [
                                        [
                                            'use'           => [DbFunctionUses::SELECT],
                                            'function'      => $function,
                                            'function_type' => FunctionTypes::DATABASE,
                                        ]
                                    ];
                                }
                                if ($aggregate = array_get($functionInfo, 'aggregate')) {
                                    $extra['is_aggregate'] = $aggregate;
                                }
                            }
                        }
                    }
                    unset($extra['extra_type']);

                    if (!empty($alias = array_get($extra, 'alias'))) {
                        $extra['quotedAlias'] = $this->quoteColumnName($alias);
                    }

                    if (null !== $c = $table->getColumn($columnName)) {
                        $c->fill($extra);
                    } elseif (!empty($type) && (array_get($extra, 'is_virtual') ||
                            !$this->supportsResourceType(DbResourceTypes::TYPE_TABLE_FIELD))
                    ) {
                        $extra['name'] = $columnName;
                        $c = new ColumnSchema($extra);
                        $c->quotedName = $this->quoteColumnName($c->name);
                        $table->addColumn($c);
                    }
                }
            }
        }
    }

    /**
     * Creates a table column.
     *
     * @param array $column column metadata
     *
     * @return ColumnSchema normalized column metadata
     */
    protected function createColumn($column)
    {
        $c = new ColumnSchema($column);
        $c->quotedName = $this->quoteColumnName($c->name);

        return $c;
    }

    /**
     * Finds the column metadata from the database for the specified table.
     *
     * @param TableSchema $table Any already known info about the table
     * @return array
     */
    protected function findColumns(
        /** @noinspection PhpUnusedParameterInspection */
        TableSchema $table
    ) {
        return [];
    }

    /**
     * Loads the relationship metadata for the specified table.
     *
     * @param TableSchema $table Any already known info about the table
     */
    protected function loadRelated(TableSchema $table)
    {
        $references = $this->getTableReferences();

        $this->buildTableRelations($table, $references);

        // merge db extras
        if (!empty($extras = $this->getSchemaVirtualRelationships($table->name))) {
            foreach ($extras as $extra) {
                $refService = null;
                $junctionService = null;
                $si = array_get($extra, 'ref_service_id');
                if ($this->getServiceId() !== $si) {
                    $refService = Service::getCachedNameById($si);
                }
                $si = array_get($extra, 'junction_service_id');
                if (!empty($si) && ($this->getServiceId() !== $si)) {
                    $junctionService = Service::getCachedNameById($si);
                }
                $extra['name'] = RelationSchema::buildName(
                    array_get($extra, 'type'),
                    array_get($extra, 'field'),
                    $refService,
                    array_get($extra, 'ref_table'),
                    array_get($extra, 'ref_field'),
                    $junctionService,
                    array_get($extra, 'junction_table')
                );
                $relation = new RelationSchema($extra);
                $relation->isVirtual = true;
                $table->addRelation($relation);
            }
        }
        if (!empty($extras = $this->getSchemaExtrasForRelated($table->name))) {
            foreach ($extras as $extra) {
                if (!empty($relatedName = array_get($extra, 'relationship'))) {
                    if (null !== $relationship = $table->getRelation($relatedName)) {
                        $relationship->fill($extra);
                        if (isset($extra['always_fetch']) && $extra['always_fetch']) {
                            $table->fetchRequiresRelations = true;
                        }
                    }
                }
            }
        }
    }

    /**
     * Obtains the metadata for the named stored procedure.
     *
     * @param string  $name    stored procedure name
     * @param boolean $refresh if we need to refresh schema cache for a stored procedure.
     *
     * @return ProcedureSchema stored procedure metadata. Null if the named stored procedure does not exist.
     */
    public function getProcedure($name, $refresh = false)
    {
        $ndx = strtolower($name);
        if (!$refresh) {
            if (isset($this->procedures[$ndx]) && $this->procedures[$ndx]->discoveryCompleted) {
                return $this->procedures[$ndx];
            } else {
                if (is_null($this->procedures)) {
                    $this->getCachedProcedureNames();
                }
                if (empty($this->procedures[$ndx])) {
                    return null;
                }
                if (null !== $procedure = $this->getFromCache('procedure:' . $ndx)) {
                    $this->procedures[$ndx] = $procedure;

                    return $this->procedures[$ndx];
                }
            }
        }

        // check if know anything about this procedure already
        if (is_null($this->procedures)) {
            $this->getCachedProcedureNames();
        }
        if (empty($procedure = array_get($this->procedures, $ndx))) {
            return null;
        }

        $this->loadProcedure($procedure);
        $this->procedures[$ndx] = $procedure;
        $this->addToCache('procedure:' . $ndx, $procedure, true);

        return $procedure;
    }

    /**
     * Loads the metadata for the specified stored procedure.
     *
     * @param ProcedureSchema $procedure procedure
     *
     * @throws \Exception
     */
    protected function loadProcedure(ProcedureSchema $procedure)
    {
        $this->loadParameters($procedure);
    }

    /**
     * Returns the metadata for all stored procedures in the database.
     *
     * @param string $schema the schema of the procedures. Defaults to empty string, meaning the current or default
     *                       schema.
     * @param bool   $refresh
     *
     * @return array the metadata for all stored procedures in the database.
     * Each array element is an instance of {@link ProcedureSchema} (or its child class).
     * The array keys are procedure names.
     */
    public function getProcedures($schema = '', $refresh = false)
    {
        $procedures = [];
        /** @type ProcedureSchema $procNameSchema */
        foreach ($this->getProcedureNames($schema) as $procNameSchema) {
            if (($procedure = $this->getProcedure($procNameSchema->name, $refresh)) !== null) {
                $procedures[$procNameSchema->name] = $procedure;
            }
        }

        return $procedures;
    }

    /**
     * Returns all stored procedure names in the database.
     *
     * @param string $schema the schema of the procedures. Defaults to empty string, meaning the current or default
     *                       schema. If not empty, the returned procedure names will be prefixed with the schema name.
     * @param bool   $refresh
     *
     * @return array all procedure names in the database.
     */
    public function getProcedureNames($schema = '', $refresh = false)
    {
        // go ahead and reset all schemas
        $this->getCachedProcedureNames($refresh);
        if (empty($schema)) {
            // return all
            return $this->procedures;
        } else {
            $names = [];
            foreach ($this->procedures as $key => $value) {
                if ($value->schemaName === $schema) {
                    $names[$key] = $value;
                }
            }

            return $names;
        }
    }

    /**
     * @param bool $refresh
     *
     * @throws \Exception
     */
    protected function getCachedProcedureNames($refresh = false)
    {
        if ($refresh ||
            (is_null($this->procedures) && (null === $this->procedures = $this->getFromCache('procedures')))
        ) {
            $names = [];
            foreach ($this->getSchemas($refresh) as $temp) {
                $names = array_merge($names, $this->findProcedureNames($temp));
            }
            ksort($names, SORT_NATURAL); // sort alphabetically
            $this->procedures = $names;
            $this->addToCache('procedures', $this->procedures, true);
        }
    }

    /**
     * Returns all stored procedure names in the database.
     * This method should be overridden by child classes in order to support this feature
     * because the default implementation simply throws an exception.
     *
     * @param string $schema the schema of the stored procedure. Defaults to empty string, meaning the current or
     *                       default schema. If not empty, the returned stored procedure names will be prefixed with
     *                       the schema name.
     *
     * @throws \Exception if current schema does not support fetching all stored procedure names
     * @return array all stored procedure names in the database.
     */
    protected function findProcedureNames($schema = '')
    {
        return $this->findRoutineNames('PROCEDURE', $schema);
    }

    /**
     * Returns all routines in the database.
     *
     * @param string $type   "procedure" or "function"
     * @param string $schema the schema of the routine. Defaults to empty string, meaning the current or
     *                       default schema. If not empty, the returned stored function names will be prefixed with the
     *                       schema name.
     *
     * @throws \InvalidArgumentException
     * @return array all stored function names in the database.
     */
    protected function findRoutineNames($type, $schema = '')
    {
        $bindings = [':type' => $type];
        $where = 'ROUTINE_TYPE = :type';
        if (!empty($schema)) {
            $where .= ' AND ROUTINE_SCHEMA = :schema';
            $bindings[':schema'] = $schema;
        }

        $sql = <<<MYSQL
SELECT ROUTINE_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.ROUTINES WHERE {$where}
MYSQL;

        $rows = $this->connection->select($sql, $bindings);

        $defaultSchema = $this->getNamingSchema();
        $addSchema = (!empty($schema) && ($defaultSchema !== $schema));

        $names = [];
        foreach ($rows as $row) {
            $row = array_change_key_case((array)$row, CASE_UPPER);
            $resourceName = array_get($row, 'ROUTINE_NAME');
            $schemaName = $schema;
            $internalName = $schemaName . '.' . $resourceName;
            $name = ($addSchema) ? $internalName : $resourceName;
            $quotedName = $this->quoteTableName($schemaName) . '.' . $this->quoteTableName($resourceName);
            $returnType = array_get($row, 'DATA_TYPE');
            if (!empty($returnType) && (0 !== strcasecmp('void', $returnType))) {
                $returnType = static::extractSimpleType($returnType);
            }
            $settings = compact('schemaName', 'resourceName', 'name', 'internalName', 'quotedName', 'returnType');
            $names[strtolower($name)] =
                ('PROCEDURE' === $type) ? new ProcedureSchema($settings) : new FunctionSchema($settings);
        }

        return $names;
    }

    /**
     * Obtains the metadata for the named stored function.
     *
     * @param string  $name    stored function name
     * @param boolean $refresh if we need to refresh schema cache for a stored function.
     *
     * @return FunctionSchema stored function metadata. Null if the named stored function does not exist.
     */
    public function getFunction($name, $refresh = false)
    {
        $ndx = strtolower($name);
        if (!$refresh) {
            if (isset($this->functions[$ndx]) && $this->functions[$ndx]->discoveryCompleted) {
                return $this->functions[$ndx];
            } else {
                if (is_null($this->functions)) {
                    $this->getCachedFunctionNames();
                }
                if (empty($this->functions[$ndx])) {
                    return null;
                }
                if (null !== $function = $this->getFromCache('function:' . $ndx)) {
                    $this->functions[$ndx] = $function;

                    return $this->functions[$ndx];
                }
            }
        }

        // check if know anything about this function already
        if (is_null($this->functions)) {
            $this->getCachedFunctionNames();
        }
        if (empty($function = array_get($this->functions, $ndx))) {
            return null;
        }

        $this->loadFunction($function);
        $this->functions[$ndx] = $function;
        $this->addToCache('function:' . $ndx, $function, true);

        return $function;
    }

    /**
     * Loads the metadata for the specified stored function.
     *
     * @param FunctionSchema $function
     */
    protected function loadFunction(FunctionSchema $function)
    {
        $this->loadParameters($function);
    }

    /**
     * Loads the parameter metadata for the specified stored procedure or function.
     *
     * @param RoutineSchema $holder
     */
    protected function loadParameters(RoutineSchema $holder)
    {
        $sql = <<<MYSQL
SELECT 
    p.ORDINAL_POSITION, p.PARAMETER_MODE, p.PARAMETER_NAME, p.DATA_TYPE, p.CHARACTER_MAXIMUM_LENGTH, p.NUMERIC_PRECISION, p.NUMERIC_SCALE
FROM 
    INFORMATION_SCHEMA.PARAMETERS AS p JOIN INFORMATION_SCHEMA.ROUTINES AS r ON r.SPECIFIC_NAME = p.SPECIFIC_NAME
WHERE 
    r.ROUTINE_NAME = '{$holder->name}' AND r.ROUTINE_SCHEMA = '{$holder->schemaName}'
MYSQL;

        foreach ($this->connection->select($sql) as $row) {
            $row = array_change_key_case((array)$row, CASE_UPPER);
            $name = ltrim(array_get($row, 'PARAMETER_NAME'), '@'); // added on by some drivers, i.e. @name
            $pos = intval(array_get($row, 'ORDINAL_POSITION'));
            $simpleType = static::extractSimpleType(array_get($row, 'DATA_TYPE'));
            if (0 === $pos) {
                $holder->returnType = $simpleType;
            } else {
                $holder->addParameter(new ParameterSchema(
                    [
                        'name'       => $name,
                        'position'   => $pos,
                        'param_type' => array_get($row, 'PARAMETER_MODE'),
                        'type'       => $simpleType,
                        'db_type'    => array_get($row, 'DATA_TYPE'),
                        'length'     => (isset($row['CHARACTER_MAXIMUM_LENGTH']) ? intval(array_get($row,
                            'CHARACTER_MAXIMUM_LENGTH')) : null),
                        'precision'  => (isset($row['NUMERIC_PRECISION']) ? intval(array_get($row, 'NUMERIC_PRECISION'))
                            : null),
                        'scale'      => (isset($row['NUMERIC_SCALE']) ? intval(array_get($row, 'NUMERIC_SCALE'))
                            : null),
                    ]
                ));
            }
        }
    }

    /**
     * Returns the metadata for all stored functions in the database.
     *
     * @param string $schema the schema of the functions. Defaults to empty string, meaning the current or default
     *                       schema.
     *
     * @return array the metadata for all stored functions in the database.
     * Each array element is an instance of {@link FunctionSchema} (or its child class).
     * The array keys are functions names.
     */
    public function getFunctions($schema = '')
    {
        $functions = [];
        /** @type FunctionSchema $funcNameSchema */
        foreach ($this->getFunctionNames($schema) as $funcNameSchema) {
            if (($procedure = $this->getFunction($funcNameSchema->name)) !== null) {
                $functions[$funcNameSchema->name] = $procedure;
            }
        }

        return $functions;
    }

    /**
     * Returns all stored functions names in the database.
     *
     * @param string $schema the schema of the functions. Defaults to empty string, meaning the current or default
     *                       schema. If not empty, the returned functions names will be prefixed with the schema name.
     *
     * @param bool   $refresh
     *
     * @return array all stored functions names in the database.
     */
    public function getFunctionNames($schema = '', $refresh = false)
    {
        // go ahead and reset all schemas
        $this->getCachedFunctionNames($refresh);
        if (empty($schema)) {
            // return all
            return $this->functions;
        } else {
            $names = [];
            foreach ($this->functions as $key => $value) {
                if ($value->schemaName === $schema) {
                    $names[$key] = $value;
                }
            }

            return $names;
        }
    }

    /**
     * @param bool $refresh
     *
     * @throws \Exception
     */
    protected function getCachedFunctionNames($refresh = false)
    {
        if ($refresh ||
            (is_null($this->functions) && (null === $this->functions = $this->getFromCache('functions')))
        ) {
            $names = [];
            foreach ($this->getSchemas($refresh) as $temp) {
                $names = array_merge($names, $this->findFunctionNames($temp));
            }
            ksort($names, SORT_NATURAL); // sort alphabetically
            $this->functions = $names;
            $this->addToCache('functions', $this->functions, true);
        }
    }

    /**
     * Returns all stored function names in the database.
     * This method should be overridden by child classes in order to support this feature
     * because the default implementation simply throws an exception.
     *
     * @param string $schema the schema of the stored function. Defaults to empty string, meaning the current or
     *                       default schema. If not empty, the returned stored function names will be prefixed with the
     *                       schema name.
     *
     * @throws \Exception if current schema does not support fetching all stored function names
     * @return array all stored function names in the database.
     */
    protected function findFunctionNames($schema = '')
    {
        return $this->findRoutineNames('FUNCTION', $schema);
//        throw new NotImplementedException("Database or driver does not support fetching all stored function names.");
    }

    /**
     * Resets the sequence value of a table's primary key.
     * The sequence will be reset such that the primary key of the next new row inserted
     * will have the specified value or max value of a primary key plus one (i.e. sequence trimming).
     *
     * @param TableSchema  $table   the table schema whose primary key sequence will be reset
     * @param integer|null $value   the value for the primary key of the next new row inserted.
     *                              If this is not set, the next new row's primary key will have the max value of a
     *                              primary key plus one (i.e. sequence trimming).
     */
    public function resetSequence($table, $value = null)
    {
    }

    /**
     * Enables or disables integrity check.
     *
     * @param boolean $check  whether to turn on or off the integrity check.
     * @param string  $schema the schema of the tables. Defaults to empty string, meaning the current or default schema.
     */
    public function checkIntegrity($check = true, $schema = '')
    {
    }

    /**
     * @param      $tables
     * @param bool $allow_merge
     * @param bool $allow_delete
     * @param bool $rollback
     *
     * @return array
     * @throws \Exception
     */
    public function updateSchema($tables, $allow_merge = false, $allow_delete = false, $rollback = false)
    {
        if (!is_array($tables) || empty($tables)) {
            throw new \Exception('There are no table sets in the request.');
        }

        if (!isset($tables[0])) {
            // single record possibly passed in without wrapper array
            $tables = [$tables];
        }

        $created = [];
        $references = [];
        $indexes = [];
        $out = [];
        $tableExtras = [];
        $fieldExtras = [];
        $fieldDrops = [];
        $relatedExtras = [];
        $relatedDrops = [];
        $virtualRelations = [];
        $virtualRelationDrops = [];
        $count = 0;
        $singleTable = (1 == count($tables));

        foreach ($tables as $table) {
            try {
                if (empty($tableName = array_get($table, 'name'))) {
                    throw new \Exception('Table name missing from schema.');
                }

                //	Does it already exist
                if (!$tableSchema = $this->getResource(DbResourceTypes::TYPE_TABLE, $tableName)) {
                    // until views get their own resource
                    if ($this->supportsResourceType(DbResourceTypes::TYPE_VIEW)) {
                        $tableSchema = $this->getResource(DbResourceTypes::TYPE_VIEW, $tableName);
                    }
                }
                if ($tableSchema) {
                    if (!$allow_merge) {
                        throw new \Exception("A table with name '$tableName' already exist in the database.");
                    }

                    \Log::debug('Schema update: ' . $tableName);

                    $results = [];
                    if (!empty($fields = array_get($table, 'field'))) {
                        $results = $this->buildTableFields($tableSchema, $fields, true, $allow_delete);
                    }
                    if (!empty($related = array_get($table, 'related'))) {
                        $related = $this->buildTableRelated($tableSchema, $related, true, $allow_delete);
                        $results = array_merge($results, $related);
                    }

                    $this->updateTable($tableSchema, array_merge($table, $results));
                } else {
                    \Log::debug('Creating table: ' . $tableName);

                    $results = [];
                    if (!empty($fields = array_get($table, 'field'))) {
                        $results = $this->createTableFields($tableName, $fields);
                    }
                    if (!empty($related = array_get($table, 'related'))) {
                        $temp = $this->createTableRelated($tableName, $related);
                        $results = array_merge($results, $temp);
                    }

                    $this->createTable($table, $results);

                    if (!$singleTable && $rollback) {
                        $created[] = $tableName;
                    }
                }

                if (!empty($results['commands'])) {
                    foreach ($results['commands'] as $extraCommand) {
                        try {
                            $this->connection->statement($extraCommand);
                        } catch (\Exception $ex) {
                            // oh well, we tried.
                        }
                    }
                }

                // add table extras
                $extras = array_only($table, ['label', 'plural', 'alias', 'description', 'name_field']);
                if (!empty($extras)) {
                    $extras['table'] = $tableName;
                    $tableExtras[] = $extras;
                }

                $fieldExtras = array_merge($fieldExtras, (array)array_get($results, 'extras'));
                $fieldDrops = array_merge($fieldDrops, (array)array_get($results, 'drop_extras'));
                $references = array_merge($references, (array)array_get($results, 'references'));
                $indexes = array_merge($indexes, (array)array_get($results, 'indexes'));
                $relatedExtras = array_merge($relatedExtras, (array)array_get($results, 'related_extras'));
                $relatedDrops = array_merge($relatedDrops, (array)array_get($results, 'drop_related_extras'));
                $virtualRelations = array_merge($virtualRelations, (array)array_get($results, 'virtual_relations'));
                $virtualRelationDrops = array_merge($virtualRelationDrops,
                    (array)array_get($results, 'drop_virtual_relations'));

                $out[$count] = ['name' => $tableName];
            } catch (\Exception $ex) {
                if ($rollback || $singleTable) {
                    //  Delete any created tables
                    throw $ex;
                }

                $out[$count] = [
                    'error' => [
                        'message' => $ex->getMessage(),
                        'code'    => $ex->getCode()
                    ]
                ];
            }

            $count++;
        }

        if (!empty($references)) {
            $this->createFieldReferences($references);
        }
        if (!empty($indexes)) {
            $this->createFieldIndexes($indexes);
        }
        if (!empty($tableExtras)) {
            $this->setSchemaTableExtras($tableExtras);
        }
        if (!empty($fieldExtras)) {
            $this->setSchemaFieldExtras($fieldExtras);
        }
        if (!empty($fieldDrops)) {
            foreach ($fieldDrops as $table => $dropped) {
                $this->removeSchemaExtrasForFields($table, $dropped);
            }
        }
        if (!empty($relatedExtras)) {
            $this->setSchemaRelatedExtras($relatedExtras);
        }
        if (!empty($relatedDrops)) {
            foreach ($relatedDrops as $table => $dropped) {
                $this->removeSchemaExtrasForRelated($table, $dropped);
            }
        }
        if (!empty($virtualRelations)) {
            $this->setSchemaVirtualRelationships($virtualRelations);
        }
        if (!empty($virtualRelationDrops)) {
            foreach ($virtualRelationDrops as $table => $dropped) {
                $this->removeSchemaVirtualRelationships($table, $dropped);
            }
        }

        return $out;
    }

    /**
     * @param string $table_name
     * @param array  $fields
     *
     * @throws \Exception
     * @return array
     */
    protected function createTableFields($table_name, $fields)
    {
        if (!is_array($fields) || empty($fields)) {
            throw new \Exception('There are no fields in the requested schema.');
        }

        if (!isset($fields[0])) {
            // single record possibly passed in without wrapper array
            $fields = [$fields];
        }

        $internalTableName = $table_name;
        if ((false === strpos($table_name, '.')) && !empty($namingSchema = $this->getNamingSchema())) {
            $internalTableName = $namingSchema . '.' . $table_name;
        }
        $columns = [];
        $references = [];
        $indexes = [];
        $extras = [];
        $commands = [];
        foreach ($fields as $field) {
            $this->cleanClientField($field);
            $name = array_get($field, 'name');

            // clean out extras
            $extraNew = array_only($field, $this->fieldExtras);
            $field = array_except($field, $this->fieldExtras);

            $type = strtolower((string)array_get($field, 'type'));
            if (!$this->supportsResourceType(DbResourceTypes::TYPE_TABLE_FIELD) ||
                array_get($extraNew, 'is_virtual', false)
            ) {
                // no need to build what the db doesn't support, use extras and bail
                $extraNew['extra_type'] = $type;
            } else {
                if ($this->isUndiscoverableType($type)) {
                    $extraNew['extra_type'] = $type;
                }

                $result = $this->buildTableField($internalTableName, $field);
                $commands = array_merge($commands, (array)array_get($result, 'commands'));
                $references = array_merge($references, (array)array_get($result, 'references'));
                $indexes = array_merge($indexes, (array)array_get($result, 'indexes'));

                $columns[$name] = $field;
            }

            if (!empty($extraNew)) {
                $extraNew['table'] = $table_name;
                $extraNew['field'] = $name;
                $extras[] = $extraNew;
            }
        }

        return [
            'columns'    => $columns,
            'references' => $references,
            'indexes'    => $indexes,
            'extras'     => $extras,
            'commands'   => $commands,
        ];
    }

    /**
     * @param TableSchema $table_schema
     * @param array       $fields
     * @param bool        $allow_update
     * @param bool        $allow_delete
     *
     * @throws \Exception
     * @return array
     */
    protected function buildTableFields(
        $table_schema,
        $fields,
        $allow_update = false,
        $allow_delete = false
    ) {
        if (!is_array($fields) || empty($fields)) {
            throw new \Exception('There are no fields in the requested schema.');
        }

        if (!isset($fields[0])) {
            // single record possibly passed in without wrapper array
            $fields = [$fields];
        }

        $columns = [];
        $alterColumns = [];
        $dropColumns = [];
        $references = [];
        $indexes = [];
        $extras = [];
        $dropExtras = [];
        $commands = [];
        $internalTableName = $table_schema->internalName;
        foreach ($fields as $field) {
            $this->cleanClientField($field);
            $name = array_get($field, 'name');

            /** @type ColumnSchema $oldField */
            if ($oldField = $table_schema->getColumn($name)) {
                // UPDATE
                if (!$allow_update) {
                    throw new \Exception("Field '$name' already exists in table '{$table_schema->name}'.");
                }

                $oldArray = $oldField->toArray();
                $diffFields = array_diff($this->fieldExtras, ['picklist', 'validation', 'db_function']);
                $extraNew = array_diff_assoc(array_only($field, $diffFields), array_only($oldArray, $diffFields));

                if (array_key_exists('picklist', $field)) {
                    $picklist = (array)array_get($field, 'picklist');
                    $oldPicklist = (array)$oldField->picklist;
                    if ((count($picklist) !== count($oldPicklist)) ||
                        !empty(array_diff($picklist, $oldPicklist)) ||
                        !empty(array_diff($oldPicklist, $picklist))
                    ) {
                        $extraNew['picklist'] = $picklist;
                    }
                }

                if (array_key_exists('validation', $field)) {
                    $validation = (array)array_get($field, 'validation');
                    $oldValidation = (array)$oldField->validation;
                    if (json_encode($validation) !== json_encode($oldValidation)) {
                        $extraNew['validation'] = $validation;
                    }
                }

                if (array_key_exists('db_function', $field)) {
                    $dbFunction = (array)array_get($field, 'db_function');
                    $oldFunction = (array)$oldField->dbFunction;
                    if (json_encode($dbFunction) !== json_encode($oldFunction)) {
                        $extraNew['db_function'] = $dbFunction;
                    }
                }

                // clean out extras
                $noDiff = array_merge($this->fieldExtras, ['default', 'native']);
                $settingsNew = array_diff_assoc(array_except($field, $noDiff), array_except($oldArray, $noDiff));

                // may be an array due to expressions
                if (array_key_exists('default', $settingsNew)) {
                    $default = $settingsNew['default'];
                    if ($default !== $oldField->defaultValue) {
                        $settingsNew['default'] = $default;
                    }
                }
                if (array_key_exists('native', $settingsNew)) {
                    $native = $settingsNew['native'];
                    if ($native !== $oldField->native) {
                        $settingsNew['native'] = $native;
                    }
                }

                // if empty, nothing to do here, check extras
                if (empty($settingsNew)) {
                    if (!empty($extraNew)) {
                        $extraNew['table'] = $table_schema->name;
                        $extraNew['field'] = $name;
                        $extras[] = $extraNew;
                    }

                    continue;
                }

                $type = strtolower((string)array_get($field, 'type'));
                if (!$this->supportsResourceType(DbResourceTypes::TYPE_TABLE_FIELD) ||
                    array_get($extraNew, 'is_virtual', false) || $oldField->isVirtual
                ) {
                    if (!$oldField->isVirtual) {
                        throw new \Exception("Field '$name' already exists as non-virtual in table '{$table_schema->name}'.");
                    }
                    // no need to build what the db doesn't support, use extras and bail
                    $extraNew['extra_type'] = $type;
                } else {
                    if ($this->isUndiscoverableType($type)) {
                        $extraNew['extra_type'] = $type;
                    }

                    $result = $this->buildTableField($internalTableName, $field, true, $oldField);
                    $commands = array_merge($commands, (array)array_get($result, 'commands'));
                    $references = array_merge($references, (array)array_get($result, 'references'));
                    $indexes = array_merge($indexes, (array)array_get($result, 'indexes'));

                    $alterColumns[$name] = $field;
                }
            } else {
                // CREATE

                // clean out extras
                $extraNew = array_only($field, $this->fieldExtras);
                $field = array_except($field, $this->fieldExtras);

                $type = strtolower((string)array_get($field, 'type'));
                if (!$this->supportsResourceType(DbResourceTypes::TYPE_TABLE_FIELD) ||
                    array_get($extraNew, 'is_virtual', false)
                ) {
                    // no need to build what the db doesn't support, use extras and bail
                    $extraNew['extra_type'] = $type;
                } else {
                    if ($this->isUndiscoverableType($type)) {
                        $extraNew['extra_type'] = $type;
                    }

                    $result = $this->buildTableField($internalTableName, $field, true);
                    $commands = array_merge($commands, (array)array_get($result, 'commands'));
                    $references = array_merge($references, (array)array_get($result, 'references'));
                    $indexes = array_merge($indexes, (array)array_get($result, 'indexes'));

                    $columns[$name] = $field;
                }
            }

            if (!empty($extraNew)) {
                $extraNew['table'] = $table_schema->name;
                $extraNew['field'] = $name;
                $extras[] = $extraNew;
            }
        }

        if ($allow_delete) {
            // check for columns to drop
            /** @type  ColumnSchema $oldField */
            foreach ($table_schema->getColumns() as $oldField) {
                $found = false;
                foreach ($fields as $field) {
                    $field = array_change_key_case($field, CASE_LOWER);
                    if (array_get($field, 'name') === $oldField->name) {
                        $found = true;
                    }
                }
                if (!$found) {
                    if ($this->supportsResourceType(DbResourceTypes::TYPE_TABLE_FIELD) && !$oldField->isVirtual) {
                        $dropColumns[] = $oldField->name;
                    }
                    $dropExtras[$table_schema->name][] = $oldField->name;
                }
            }
        }

        return [
            'columns'       => $columns,
            'alter_columns' => $alterColumns,
            'drop_columns'  => $dropColumns,
            'references'    => $references,
            'indexes'       => $indexes,
            'extras'        => $extras,
            'drop_extras'   => $dropExtras,
            'commands'      => $commands,
        ];
    }

    /**
     * @param string       $tableName
     * @param array        $field
     * @param bool         $oldTable
     * @param ColumnSchema $oldField
     *
     * @return array
     * @throws \Exception
     */
    protected function buildTableField($tableName, $field, $oldTable = false, $oldField = null)
    {
        $name = array_get($field, 'name');
        $type = strtolower((string)array_get($field, 'type'));
        $commands = [];
        $indexes = [];
        $references = [];
        switch ($type) {
            case DbSimpleTypes::TYPE_ID:
                $pkExtras = $this->getPrimaryKeyCommands($tableName, $name);
                $commands = array_merge($commands, $pkExtras);
                break;
        }

        if (((DbSimpleTypes::TYPE_REF == $type) || array_get($field, 'is_foreign_key'))) {
            // special case for references because the table referenced may not be created yet
            if (empty($refTable = array_get($field, 'ref_table'))) {
                throw new \Exception("Invalid schema detected - no table element for reference type of $name.");
            }

            if ((false === strpos($refTable, '.')) && !empty($namingSchema = $this->getNamingSchema())) {
                $refTable = $namingSchema . '.' . $refTable;
            }
            $refColumns = array_get($field, 'ref_field', array_get($field, 'ref_fields'));
            $refOnDelete = array_get($field, 'ref_on_delete');
            $refOnUpdate = array_get($field, 'ref_on_update');

            if ($this->allowsSeparateForeignConstraint()) {
                if (!isset($oldField) || !$oldField->isForeignKey) {
                    // will get to it later, $refTable may not be there
                    $keyName = $this->makeConstraintName('fk', $tableName, $name);
                    $references[] = [
                        'name'      => $keyName,
                        'table'     => $tableName,
                        'column'    => $name,
                        'ref_table' => $refTable,
                        'ref_field' => $refColumns,
                        'delete'    => $refOnDelete,
                        'update'    => $refOnUpdate,
                    ];
                }
            }
        }

        // regardless of type
        if (array_get($field, 'is_unique')) {
            if ($this->requiresCreateIndex(true, !$oldTable)) {
                // will get to it later, create after table built
                $keyName = $this->makeConstraintName('undx', $tableName, $name);
                $indexes[] = [
                    'name'   => $keyName,
                    'table'  => $tableName,
                    'column' => $name,
                    'unique' => true,
                    'drop'   => isset($oldField),
                ];
            }
        } elseif (array_get($field, 'is_index')) {
            if ($this->requiresCreateIndex($oldTable, !$oldTable)) {
                // will get to it later, create after table built
                $keyName = $this->makeConstraintName('ndx', $tableName, $name);
                $indexes[] = [
                    'name'   => $keyName,
                    'table'  => $tableName,
                    'column' => $name,
                    'drop'   => isset($oldField),
                ];
            }
        }

        return ['field' => $field, 'commands' => $commands, 'references' => $references, 'indexes' => $indexes];
    }

    /**
     * @param string $table_name
     * @param array  $related
     *
     * @throws \Exception
     * @return array
     */
    public function createTableRelated($table_name, $related)
    {
        if (!is_array($related) || empty($related)) {
            return [];
        }

        if (!isset($related[0])) {
            // single record possibly passed in without wrapper array
            $related = [$related];
        }

        $extra = [];
        $virtual = [];
        foreach ($related as $relation) {
            $this->cleanClientRelation($relation);
            // clean out extras
            $extraNew = array_only($relation, $this->relatedExtras);
            if (!empty($extraNew['alias']) || !empty($extraNew['label']) || !empty($extraNew['description']) ||
                !empty($extraNew['always_fetch']) || !empty($extraNew['flatten']) ||
                !empty($extraNew['flatten_drop_prefix'])
            ) {
                $extraNew['table'] = $table_name;
                $extraNew['relationship'] = array_get($relation, 'name');
                $extra[] = $extraNew;
            }

            // only virtual
            if (boolval(array_get($relation, 'is_virtual'))) {
                $relation = array_except($relation, $this->relatedExtras);
                $relation['table'] = $table_name;
                $virtual[] = $relation;
            } else {
                // todo create foreign keys here eventually as well?
            }
        }

        return [
            'related_extras'    => $extra,
            'virtual_relations' => $virtual,
        ];
    }

    /**
     * @param TableSchema $table_schema
     * @param array       $related
     * @param bool        $allow_update
     * @param bool        $allow_delete
     *
     * @throws \Exception
     * @return array
     */
    public function buildTableRelated(
        $table_schema,
        $related,
        $allow_update = false,
        $allow_delete = false
    ) {
        if (!is_array($related) || empty($related)) {
            throw new \Exception('There are no related elements in the requested schema.');
        }

        if (!isset($related[0])) {
            // single record possibly passed in without wrapper array
            $related = [$related];
        }

        $extras = [];
        $dropExtras = [];
        $virtuals = [];
        $dropVirtuals = [];
        foreach ($related as $relation) {
            $this->cleanClientRelation($relation);
            $name = array_get($relation, 'name');

            /** @type RelationSchema $oldRelation */
            if ($oldRelation = $table_schema->getRelation($name)) {
                // UPDATE
                if (!$allow_update) {
                    throw new \Exception("Relation '$name' already exists in table '{$table_schema->name}'.");
                }

                $oldArray = $oldRelation->toArray();
                $extraNew = array_only($relation, $this->relatedExtras);
                $extraOld = array_only($oldArray, $this->relatedExtras);
                if (!empty($extraNew = array_diff_assoc($extraNew, $extraOld))) {
                    // if all empty, delete the extras entry, otherwise update
                    $combined = array_merge($extraOld, $extraNew);
                    if (!empty($combined['alias']) || !empty($combined['label']) || !empty($combined['description']) ||
                        !empty($combined['always_fetch']) || !empty($combined['flatten']) ||
                        !empty($combined['flatten_drop_prefix'])
                    ) {
                        $extraNew['table'] = $table_schema->name;
                        $extraNew['relationship'] = array_get($relation, 'name');
                        $extras[] = $extraNew;
                    } else {
                        $dropExtras[$table_schema->name][] = $oldRelation->name;
                    }
                }

                // only virtual
                if (boolval(array_get($relation, 'is_virtual'))) {
                    // clean out extras
                    $noDiff = array_merge($this->relatedExtras, ['native']);
                    $relation = array_except($relation, $noDiff);
                    if (!empty(array_diff_assoc($relation, array_except($oldArray, $noDiff)))) {
                        $relation['table'] = $table_schema->name;
                        $virtuals[] = $relation;
                    }
                }
            } else {
                // CREATE
                // clean out extras
                $extraNew = array_only($relation, $this->relatedExtras);
                if (!empty($extraNew['alias']) || !empty($extraNew['label']) || !empty($extraNew['description']) ||
                    !empty($extraNew['always_fetch']) || !empty($extraNew['flatten']) ||
                    !empty($extraNew['flatten_drop_prefix'])
                ) {
                    $extraNew['table'] = $table_schema->name;
                    $extraNew['relationship'] = array_get($relation, 'name');
                    $extras[] = $extraNew;
                }

                // only virtual
                if (boolval(array_get($relation, 'is_virtual'))) {
                    $relation = array_except($relation, $this->relatedExtras);
                    $relation['table'] = $table_schema->name;
                    $virtuals[] = $relation;
                }
            }
        }

        if ($allow_delete && isset($oldSchema)) {
            // check for relations to drop
            /** @type RelationSchema $oldField */
            foreach ($oldSchema->getRelations() as $oldRelation) {
                $found = false;
                foreach ($related as $relation) {
                    $relation = array_change_key_case($relation, CASE_LOWER);
                    if (array_get($relation, 'name') === $oldRelation->name) {
                        $found = true;
                    }
                }
                if (!$found) {
                    if ($oldRelation->isVirtual) {
                        $dropVirtuals[$table_schema->name][] = $oldRelation->toArray();
                    } else {
                        $dropExtras[$table_schema->name][] = $oldRelation->name;
                    }
                }
            }
        }

        return [
            'related_extras'         => $extras,
            'drop_related_extras'    => $dropExtras,
            'virtual_relations'      => $virtuals,
            'drop_virtual_relations' => $dropVirtuals,
        ];
    }

    protected function cleanClientField(array &$field)
    {
        $field = array_change_key_case($field, CASE_LOWER);
        if (empty($name = array_get($field, 'name'))) {
            throw new \Exception("Invalid schema detected - no name element.");
        }
        if (!empty($label = array_get($field, 'label'))) {
            if ($label === camelize($name, '_', true)) {
                unset($field['label']); // no need to create an entry just for the same label
            }
        }

        $picklist = array_get($field, 'picklist');
        if (!empty($picklist) && !is_array($picklist)) {
            // accept comma delimited from client side
            $field['picklist'] = array_map('trim', explode(',', trim($picklist, ',')));
        }

        // make sure we have boolean values, not integers or strings
        $booleanFieldNames = [
            'allow_null',
            'fixed_length',
            'supports_multibyte',
            'auto_increment',
            'is_unique',
            'is_index',
            'is_primary_key',
            'is_foreign_key',
            'is_virtual',
            'is_aggregate',
        ];
        foreach ($booleanFieldNames as $name) {
            if (isset($field[$name])) {
                $field[$name] = boolval($field[$name]);
            }
        }

        // tighten up type info
        if (isset($field['type'])) {
            $type = strtolower((string)array_get($field, 'type'));
            switch ($type) {
                case 'pk':
                    $type = DbSimpleTypes::TYPE_ID;
                    break;
                case 'fk':
                    $type = DbSimpleTypes::TYPE_REF;
                    break;
                case 'virtual':
                    // upgrade old virtual field definitions
                    $field['is_virtual'] = true;
                    if (!empty($functionInfo = array_get($field, 'db_function'))) {
                        $type = array_get($functionInfo, 'type', DbSimpleTypes::TYPE_STRING);
                        if ($function = array_get($functionInfo, 'function')) {
                            $field['db_function'] = [
                                [
                                    'use'           => [DbFunctionUses::SELECT],
                                    'function'      => $function,
                                    'function_type' => FunctionTypes::DATABASE,
                                ]
                            ];
                        }
                        if ($aggregate = array_get($functionInfo, 'aggregate')) {
                            $field['is_aggregate'] = $aggregate;
                        }
                    }
                    break;
            }
            $field['type'] = $type;
        }
    }

    protected function cleanClientRelation(array &$relation)
    {
        $relation = array_change_key_case($relation, CASE_LOWER);
        // make sure we have boolean values, not integers or strings
        $booleanFieldNames = [
            'is_virtual',
            'always_fetch',
            'flatten',
            'flatten_drop_prefix',
        ];
        foreach ($booleanFieldNames as $name) {
            if (isset($relation[$name])) {
                $relation[$name] = boolval($relation[$name]);
            }
        }

        if (boolval(array_get($relation, 'is_virtual'))) {
            // tighten up type info
            if (isset($relation['type'])) {
                $type = strtolower((string)array_get($relation, 'type', ''));
                switch ($type) {
                    case RelationSchema::BELONGS_TO:
                    case RelationSchema::HAS_MANY:
                    case RelationSchema::MANY_MANY:
                        $relation['type'] = $type;
                        break;
                    default:
                        throw new \Exception("Invalid schema detected - invalid or missing type element.");
                        break;
                }
            }
        } else {
            if (empty(array_get($relation, 'name'))) {
                throw new \Exception("Invalid schema detected - no name element.");
            }
        }
    }

    protected static function isUndiscoverableType($type)
    {
        switch ($type) {
            // keep our type extensions
            case DbSimpleTypes::TYPE_USER_ID:
            case DbSimpleTypes::TYPE_USER_ID_ON_CREATE:
            case DbSimpleTypes::TYPE_USER_ID_ON_UPDATE:
            case DbSimpleTypes::TYPE_TIMESTAMP_ON_CREATE:
            case DbSimpleTypes::TYPE_TIMESTAMP_ON_UPDATE:
                return true;
        }

        return false;
    }

    /**
     * @param array $info
     */
    protected function translateSimpleColumnTypes(array &$info)
    {
    }

    /**
     * @param array $info
     */
    protected function validateColumnSettings(array &$info)
    {
    }

    /**
     * @param array $info
     *
     * @return string
     * @throws \Exception
     */
    protected function buildColumnDefinition(array $info)
    {
        // This works for most except Oracle
        $type = (isset($info['type'])) ? $info['type'] : null;
        $typeExtras = (isset($info['type_extras'])) ? $info['type_extras'] : null;

        $definition = $type . $typeExtras;

        $allowNull = (isset($info['allow_null'])) ? $info['allow_null'] : null;
        $definition .= ($allowNull) ? ' NULL' : ' NOT NULL';

        $default = (isset($info['db_type'])) ? $info['db_type'] : null;
        if (isset($default)) {
            if (is_array($default)) {
                $expression = (isset($default['expression'])) ? $default['expression'] : null;
                if (null !== $expression) {
                    $definition .= ' DEFAULT ' . $expression;
                }
            } else {
                $default = $this->quoteValue($default);
                $definition .= ' DEFAULT ' . $default;
            }
        }

        $isUniqueKey = (isset($info['is_unique'])) ? filter_var($info['is_unique'], FILTER_VALIDATE_BOOLEAN) : false;
        $isPrimaryKey =
            (isset($info['is_primary_key'])) ? filter_var($info['is_primary_key'], FILTER_VALIDATE_BOOLEAN) : false;
        if ($isPrimaryKey && $isUniqueKey) {
            throw new \Exception('Unique and Primary designations not allowed simultaneously.');
        }

        if ($isUniqueKey) {
            $definition .= ' UNIQUE KEY';
        } elseif ($isPrimaryKey) {
            $definition .= ' PRIMARY KEY';
        }

        return $definition;
    }

    /**
     * Converts an abstract column type into a physical column type.
     * The conversion is done using the type map specified in {@link columnTypes}.
     * These abstract column types are supported (using MySQL as example to explain the corresponding
     * physical types):
     * <ul>
     * <li>pk: an auto-incremental primary key type, will be converted into "int(11) NOT NULL AUTO_INCREMENT PRIMARY
     * KEY"</li>
     * <li>string: string type, will be converted into "varchar(255)"</li>
     * <li>text: a long string type, will be converted into "text"</li>
     * <li>integer: integer type, will be converted into "int(11)"</li>
     * <li>boolean: boolean type, will be converted into "tinyint(1)"</li>
     * <li>float: float number type, will be converted into "float"</li>
     * <li>decimal: decimal number type, will be converted into "decimal"</li>
     * <li>datetime: datetime type, will be converted into "datetime"</li>
     * <li>timestamp: timestamp type, will be converted into "timestamp"</li>
     * <li>time: time type, will be converted into "time"</li>
     * <li>date: date type, will be converted into "date"</li>
     * <li>binary: binary data type, will be converted into "blob"</li>
     * </ul>
     *
     * If the abstract type contains two or more parts separated by spaces or '(' (e.g. "string NOT NULL" or
     * "decimal(10,2)"), then only the first part will be converted, and the rest of the parts will be appended to the
     * conversion result. For example, 'string NOT NULL' is converted to 'varchar(255) NOT NULL'.
     *
     * @param string $info abstract column type
     *
     * @return string physical column type including arguments, null designation and defaults.
     * @throws \Exception
     */
    protected function getColumnType($info)
    {
        $out = [];
        $type = '';
        if (is_string($info)) {
            $type = trim($info); // cleanup
        } elseif (is_array($info)) {
            $sql = (isset($info['sql'])) ? $info['sql'] : null;
            if (!empty($sql)) {
                return $sql; // raw SQL statement given, pass it on.
            }

            $out = $info;
            $type = (isset($info['type'])) ? $info['type'] : null;
            if (empty($type)) {
                $type = (isset($info['db_type'])) ? $info['db_type'] : null;
                if (empty($type)) {
                    throw new \Exception("Invalid schema detected - no type or db_type element.");
                }
            }
            $type = trim($type); // cleanup
        }

        if (empty($type)) {
            throw new \Exception("Invalid schema detected - no type definition.");
        }

        //  If there are extras, then pass it on through
        if ((false !== strpos($type, ' ')) || (false !== strpos($type, '('))) {
            return $type;
        }

        $out['type'] = $type;
        $this->translateSimpleColumnTypes($out);
        $this->validateColumnSettings($out);

        return $this->buildColumnDefinition($out);
    }

    /**
     * Builds a SQL statement for renaming a DB table.
     *
     * @param string $table   the table to be renamed. The name will be properly quoted by the method.
     * @param string $newName the new table name. The name will be properly quoted by the method.
     *
     * @return string the SQL statement for renaming a DB table.
     */
    public function renameTable($table, $newName)
    {
        return 'RENAME TABLE ' . $this->quoteTableName($table) . ' TO ' . $this->quoteTableName($newName);
    }

    /**
     * Builds a SQL statement for truncating a DB table.
     *
     * @param string $table the table to be truncated. The name will be properly quoted by the method.
     *
     * @return string the SQL statement for truncating a DB table.
     */
    public function truncateTable($table)
    {
        return "TRUNCATE TABLE " . $this->quoteTableName($table);
    }

    /**
     * Builds a SQL statement for adding a new DB column.
     *
     * @param string $table  The quoted table that the new column will be added to.
     * @param string $column The name of the new column. The name will be properly quoted by the method.
     * @param string $type   The column type. The {@link getColumnType} method will be invoked to convert abstract
     *                       column type (if any) into the physical one. Anything that is not recognized as abstract
     *                       type will be kept in the generated SQL. For example, 'string' will be turned into
     *                       'varchar(255)', while 'string not null' will become 'varchar(255) not null'.
     *
     * @return string the SQL statement for adding a new column.
     */
    public function addColumn($table, $column, $type)
    {
        return <<<MYSQL
ALTER TABLE $table ADD COLUMN {$this->quoteColumnName($column)} {$this->getColumnType($type)};
MYSQL;
    }

    /**
     * Builds a SQL statement for renaming a column.
     *
     * @param string $table   the table whose column is to be renamed. The name will be properly quoted by the method.
     * @param string $name    the old name of the column. The name will be properly quoted by the method.
     * @param string $newName the new name of the column. The name will be properly quoted by the method.
     *
     * @return string the SQL statement for renaming a DB column.
     */
    public function renameColumn($table, $name, $newName)
    {
        return <<<MYSQL
ALTER TABLE $table RENAME COLUMN {$this->quoteColumnName($name)} TO {$this->quoteColumnName($newName)};
MYSQL;
    }

    /**
     * Builds a SQL statement for changing the definition of a column.
     *
     * @param string $table      the table whose column is to be changed. The table name will be properly quoted by the
     *                           method.
     * @param string $column     the name of the column to be changed. The name will be properly quoted by the method.
     * @param string $definition the new column type. The {@link getColumnType} method will be invoked to convert
     *                           abstract column type (if any) into the physical one. Anything that is not recognized
     *                           as abstract type will be kept in the generated SQL. For example, 'string' will be
     *                           turned into 'varchar(255)', while 'string not null' will become 'varchar(255) not
     *                           null'.
     *
     * @return string the SQL statement for changing the definition of a column.
     */
    public function alterColumn($table, $column, $definition)
    {
        return <<<MYSQL
ALTER TABLE $table CHANGE {$this->quoteColumnName($column)} {$this->quoteColumnName($column)} {$this->getColumnType($definition)};
MYSQL;
    }

    /**
     * @param string      $prefix
     * @param string      $table
     * @param string|null $column
     *
     * @return string
     */
    public function makeConstraintName($prefix, $table, $column = null)
    {
        $temp = $prefix . '_' . str_replace('.', '_', $table);
        if (!empty($column)) {
            $temp .= '_' . $column;
        }

        return $temp;
    }

    /**
     * Builds a SQL statement for adding a foreign key constraint to an existing table.
     * The method will properly quote the table and column names.
     *
     * @param string $name       the name of the foreign key constraint.
     * @param string $table      the table that the foreign key constraint will be added to.
     * @param string $columns    the name of the column to that the constraint will be added on. If there are multiple
     *                           columns, separate them with commas.
     * @param string $refTable   the table that the foreign key references to.
     * @param string $refColumns the name of the column that the foreign key references to. If there are multiple
     *                           columns, separate them with commas.
     * @param string $delete     the ON DELETE option. Most DBMS support these options: RESTRICT, CASCADE, NO ACTION,
     *                           SET DEFAULT, SET NULL
     * @param string $update     the ON UPDATE option. Most DBMS support these options: RESTRICT, CASCADE, NO ACTION,
     *                           SET DEFAULT, SET NULL
     *
     * @return string the SQL statement for adding a foreign key constraint to an existing table.
     */
    public function addForeignKey($name, $table, $columns, $refTable, $refColumns, $delete = null, $update = null)
    {
        $columns = preg_split('/\s*,\s*/', $columns, -1, PREG_SPLIT_NO_EMPTY);
        foreach ($columns as $i => $col) {
            $columns[$i] = $this->quoteColumnName($col);
        }
        $refColumns = preg_split('/\s*,\s*/', $refColumns, -1, PREG_SPLIT_NO_EMPTY);
        foreach ($refColumns as $i => $col) {
            $refColumns[$i] = $this->quoteColumnName($col);
        }
        $sql =
            'ALTER TABLE ' .
            $this->quoteTableName($table) .
            ' ADD CONSTRAINT ' .
            $this->quoteColumnName($name) .
            ' FOREIGN KEY (' .
            implode(', ', $columns) .
            ')' .
            ' REFERENCES ' .
            $this->quoteTableName($refTable) .
            ' (' .
            implode(', ', $refColumns) .
            ')';
        if ($delete !== null) {
            $sql .= ' ON DELETE ' . $delete;
        }
        if ($update !== null) {
            $sql .= ' ON UPDATE ' . $update;
        }

        return $sql;
    }

    /**
     * Builds a SQL statement for dropping a foreign key constraint.
     *
     * @param string $name  the name of the foreign key constraint to be dropped. The name will be properly quoted by
     *                      the method.
     * @param string $table the table whose foreign is to be dropped. The name will be properly quoted by the method.
     *
     * @return string the SQL statement for dropping a foreign key constraint.
     */
    public function dropForeignKey($name, $table)
    {
        return 'ALTER TABLE ' . $this->quoteTableName($table) . ' DROP CONSTRAINT ' . $this->quoteColumnName($name);
    }

    /**
     * @param bool $unique
     * @param bool $on_create_table
     *
     * @return bool
     */
    public function requiresCreateIndex($unique = false, $on_create_table = false)
    {
        return true;
    }

    /**
     * @return bool
     */
    public function allowsSeparateForeignConstraint()
    {
        return true;
    }

    /**
     * Builds a SQL statement for creating a new index.
     *
     * @param string  $name   the name of the index. The name will be properly quoted by the method.
     * @param string  $table  the table that the new index will be created for. The table name will be properly quoted
     *                        by the method.
     * @param string  $column the column(s) that should be included in the index. If there are multiple columns, please
     *                        separate them by commas. Each column name will be properly quoted by the method, unless a
     *                        parenthesis is found in the name.
     * @param boolean $unique whether to add UNIQUE constraint on the created index.
     *
     * @return string the SQL statement for creating a new index.
     */
    public function createIndex($name, $table, $column, $unique = false)
    {
        $cols = [];
        $columns = preg_split('/\s*,\s*/', $column, -1, PREG_SPLIT_NO_EMPTY);
        foreach ($columns as $col) {
            if (strpos($col, '(') !== false) {
                $cols[] = $col;
            } else {
                $cols[] = $this->quoteColumnName($col);
            }
        }

        return
            ($unique ? 'CREATE UNIQUE INDEX ' : 'CREATE INDEX ') .
            $this->quoteTableName($name) .
            ' ON ' .
            $this->quoteTableName($table) .
            ' (' .
            implode(', ', $cols) .
            ')';
    }

    /**
     * Builds a SQL statement for dropping an index.
     *
     * @param string $name  the name of the index to be dropped. The name will be properly quoted by the method.
     * @param string $table the table whose index is to be dropped. The name will be properly quoted by the method.
     *
     * @return string the SQL statement for dropping an index.
     */
    public function dropIndex($name, $table)
    {
        return 'DROP INDEX ' . $this->quoteTableName($name) . ' ON ' . $this->quoteTableName($table);
    }

    /**
     * Builds a SQL statement for adding a primary key constraint to an existing table.
     *
     * @param string       $name    the name of the primary key constraint.
     * @param string       $table   the table that the primary key constraint will be added to.
     * @param string|array $columns comma separated string or array of columns that the primary key will consist of.
     *                              Array value can be passed.
     *
     * @return string the SQL statement for adding a primary key constraint to an existing table.
     */
    public function addPrimaryKey($name, $table, $columns)
    {
        if (is_string($columns)) {
            $columns = preg_split('/\s*,\s*/', $columns, -1, PREG_SPLIT_NO_EMPTY);
        }
        foreach ($columns as $i => $col) {
            $columns[$i] = $this->quoteColumnName($col);
        }

        return
            'ALTER TABLE ' .
            $this->quoteTableName($table) .
            ' ADD CONSTRAINT ' .
            $this->quoteColumnName($name) .
            '  PRIMARY KEY (' .
            implode(', ', $columns) .
            ' )';
    }

    /**
     * Builds a SQL statement for removing a primary key constraint to an existing table.
     *
     * @param string $name  the name of the primary key constraint to be removed.
     * @param string $table the table that the primary key constraint will be removed from.
     *
     * @return string the SQL statement for removing a primary key constraint from an existing table.
     */
    public function dropPrimaryKey($name, $table)
    {
        return 'ALTER TABLE ' . $this->quoteTableName($table) . ' DROP CONSTRAINT ' . $this->quoteColumnName($name);
    }

    /**
     * @param $table
     * @param $column
     *
     * @return array
     */
    public function getPrimaryKeyCommands($table, $column)
    {
        return [];
    }

    /**
     * @return mixed
     */
    public function getTimestampForSet()
    {
        return $this->connection->raw('(NOW())');
    }

    /**
     * Builds a SQL statement for creating a new DB view of an existing table.
     *
     *
     * @param string $table   the name of the view to be created. The name will be properly quoted by the method.
     * @param array  $columns optional mapping to the columns in the select of the new view.
     * @param string $select  SQL statement defining the view.
     * @param string $options additional SQL fragment that will be appended to the generated SQL.
     *
     * @return string the SQL statement for creating a new DB table.
     */
    public function createView($table, $columns, $select, $options = null)
    {
        $sql = "CREATE VIEW " . $this->quoteTableName($table);
        if (!empty($columns)) {
            if (is_array($columns)) {
                foreach ($columns as &$name) {
                    $name = $this->quoteColumnName($name);
                }
                $columns = implode(',', $columns);
            }
            $sql .= " ($columns)";
        }
        $sql .= " AS " . $select;

        return $sql;
    }

    /**
     * Builds a SQL statement for dropping a DB view.
     *
     * @param string $table the view to be dropped. The name will be properly quoted by the method.
     *
     * @return string the SQL statement for dropping a DB view.
     */
    public function dropView($table)
    {
        return "DROP VIEW " . $this->quoteTableName($table);
    }

    /**
     * Builds and executes a SQL statement for creating a new DB table.
     *
     * The columns in the new table should be specified as name-definition pairs (e.g. 'name'=>'string'),
     * where name stands for a column name which will be properly quoted by the method, and definition
     * stands for the column type which can contain an abstract DB type.
     * The {@link getColumnType} method will be invoked to convert any abstract type into a physical one.
     *
     * If a column is specified with definition only (e.g. 'PRIMARY KEY (name, type)'), it will be directly
     * inserted into the generated SQL.
     *
     * @param array $table   the whole schema of the table to be created. The name will be properly quoted by the
     *                       method.
     * @param array $options the options for the new table, including columns.
     *
     * @return int 0 is always returned. See <a
     *             href='http://php.net/manual/en/pdostatement.rowcount.php'>http://php.net/manual/en/pdostatement.rowcount.php</a>
     *             for more for more information.
     * @throws \Exception
     */
    protected function createTable($table, $options)
    {
        if (empty($tableName = array_get($table, 'name'))) {
            throw new \Exception("No valid name exist in the received table schema.");
        }

        if (empty($columns = array_get($options, 'columns'))) {
            throw new \Exception("No valid fields exist in the received table schema.");
        }

        $cols = [];
        foreach ($columns as $name => $type) {
            if (is_string($name)) {
                $cols[] = "\t" . $this->quoteColumnName($name) . ' ' . $this->getColumnType($type);
            } else {
                $cols[] = "\t" . $type;
            }
        }
        if ((false === strpos($tableName, '.')) && !empty($namingSchema = $this->getNamingSchema())) {
            $tableName = $namingSchema . '.' . $tableName;
        }
        $sql = "CREATE TABLE {$this->quoteTableName($tableName)} (\n" . implode(",\n", $cols) . "\n)";

        // string additional SQL fragment that will be appended to the generated SQL
        if (!empty($addOn = array_get($table, 'options'))) {
            $sql .= ' ' . $addOn;
        }

        return $this->connection->statement($sql);
    }

    /**
     * @param TableSchema $tableSchema
     * @param array       $changes
     *
     * @throws \Exception
     */
    protected function updateTable($tableSchema, $changes)
    {
        //  Is there a name update
        if (!empty($changes['new_name'])) {
            // todo change table name, has issue with references
        }

        // update column types
        if (isset($changes['columns']) && is_array($changes['columns'])) {
            foreach ($changes['columns'] as $name => $definition) {
                $this->connection->statement($this->addColumn($tableSchema->quotedName, $name, $definition));
            }
        }
        if (isset($changes['alter_columns']) && is_array($changes['alter_columns'])) {
            foreach ($changes['alter_columns'] as $name => $definition) {
                $this->connection->statement($this->alterColumn($tableSchema->quotedName, $name, $definition));
            }
        }
        if (isset($changes['drop_columns']) && is_array($changes['drop_columns']) && !empty($changes['drop_columns'])) {
            $this->connection->statement($this->dropColumns($tableSchema->quotedName, $changes['drop_columns']));
        }
    }

    /**
     * Builds and executes a SQL statement for dropping a DB table.
     *
     * @param string $table The internal table name to be dropped.
     *
     * @return integer 0 is always returned. See {@link http://php.net/manual/en/pdostatement.rowcount.php} for more
     *                 information.
     */
    public function dropTable($table)
    {
        return $this->connection->statement("DROP TABLE $table");
    }

    /**
     * @param string       $table
     * @param string|array $columns
     *
     * @return bool|int
     */
    public function dropColumns($table, $columns)
    {
        $commands = [];
        foreach ((array)$columns as $column) {
            if (!empty($column)) {
                $commands[] = "DROP COLUMN " . $column;
            }
        }

        if (!empty($commands)) {
            return $this->connection->statement("ALTER TABLE $table " . implode(',', $commands));
        }

        return false;
    }

    /**
     * @param string $table
     * @param        $relationship
     *
     * @return bool|int
     */
    public function dropRelationship($table, $relationship)
    {
        // todo anything we can do for database foreign keys here?
        return false;
    }

    /**
     * @param array $references
     *
     */
    protected function createFieldReferences($references)
    {
        if (!empty($references)) {
            foreach ($references as $reference) {
                $name = $reference['name'];
                $table = $reference['table'];
                $drop = (isset($reference['drop'])) ? boolval($reference['drop']) : false;
                if ($drop) {
                    try {
                        $this->connection->statement($this->dropForeignKey($name, $table));
                    } catch (\Exception $ex) {
                        \Log::debug($ex->getMessage());
                    }
                }
                // add new reference
                $refTable = (isset($reference['ref_table'])) ? $reference['ref_table'] : null;
                if (!empty($refTable)) {
                    $this->connection->statement($this->addForeignKey(
                        $name,
                        $table,
                        $reference['column'],
                        $refTable,
                        $reference['ref_field'],
                        $reference['delete'],
                        $reference['update']
                    ));
                }
            }
        }
    }

    /**
     * @param array $indexes
     *
     */
    protected function createFieldIndexes($indexes)
    {
        if (!empty($indexes)) {
            foreach ($indexes as $index) {
                $name = $index['name'];
                $table = $index['table'];
                $drop = (isset($index['drop'])) ? boolval($index['drop']) : false;
                if ($drop) {
                    try {
                        $this->connection->statement($this->dropIndex($name, $table));
                    } catch (\Exception $ex) {
                        \Log::debug($ex->getMessage());
                    }
                }
                $unique = (isset($index['unique'])) ? boolval($index['unique']) : false;

                $this->connection->statement($this->createIndex($name, $table, $index['column'], $unique));
            }
        }
    }

    /**
     * @param string $name
     * @param array  $in_params
     *
     * @throws \Exception
     * @return mixed
     */
    public function callFunction($name, array $in_params)
    {
        if (!$this->supportsResourceType(DbResourceTypes::TYPE_FUNCTION)) {
            throw new \Exception('Stored Functions are not supported by this database connection.');
        }

        if (null === $function = $this->getFunction($name)) {
            throw new NotFoundException("Function '$name' can not be found.");
        }

        $paramSchemas = $function->getParameters();
        $values = $this->determineRoutineValues($paramSchemas, $in_params);

        $sql = $this->getFunctionStatement($function, $paramSchemas, $values);
        /** @type \PDOStatement $statement */
        if (!$statement = $this->connection->getPdo()->prepare($sql)) {
            throw new InternalServerErrorException('Failed to prepare statement: ' . $sql);
        }

        // do binding
        $this->doRoutineBinding($statement, $paramSchemas, $values);

        // support multiple result sets
        $result = [];
        try {
            $statement->execute();
            $reader = new DataReader($statement);
            $reader->setFetchMode(static::ROUTINE_FETCH_MODE);
            do {
                $temp = $reader->readAll();
                if (!empty($temp)) {
                    $result[] = $temp;
                }
            } while ($reader->nextResult());
        } catch (\Exception $ex) {
            if (!$this->handleRoutineException($ex)) {
                $errorInfo = $ex instanceof \PDOException ? $ex : null;
                $message = $ex->getMessage();
                throw new \Exception($message, (int)$ex->getCode(), $errorInfo);
            }
        }

        // if there is only one data set, just return it
        if (1 == count($result)) {
            $result = $result[0];
            // if there is only one data set, search for an output
            if (1 == count($result)) {
                $result = current($result);
                if (array_key_exists('output', $result)) {
                    $value = $result['output'];

                    return $this->typecastToClient($value, $function->returnType);
                } elseif (array_key_exists($function->name, $result)) {
                    // some vendors return the results as the function's name
                    $value = $result[$function->name];

                    return $this->typecastToClient($value, $function->returnType);
                }
            }
        }

        return $result;
    }

    /**
     * @param array $param_schemas
     * @param array $values
     *
     * @return string
     */
    protected function getRoutineParamString(array $param_schemas, array &$values)
    {
        $paramStr = '';
        foreach ($param_schemas as $key => $paramSchema) {
            switch ($paramSchema->paramType) {
                case 'IN':
                case 'INOUT':
                case 'OUT':
                    $pName = ':' . $paramSchema->name;
                    $paramStr .= (empty($paramStr)) ? $pName : ", $pName";
                    break;
                default:
                    break;
            }
        }

        return $paramStr;
    }

    /**
     * @param \DreamFactory\Core\Database\Schema\RoutineSchema $routine
     * @param array                                            $param_schemas
     * @param array                                            $values
     *
     * @return string
     */
    protected function getFunctionStatement(RoutineSchema $routine, array $param_schemas, array &$values)
    {
        $paramStr = $this->getRoutineParamString($param_schemas, $values);

        return "SELECT {$routine->quotedName}($paramStr) AS " . $this->quoteColumnName('output');
    }

    /**
     * @param \Exception $ex
     *
     * @return bool
     */
    protected function handleRoutineException(
        /** @noinspection PhpUnusedParameterInspection */
        \Exception $ex
    ) {
        return false;
    }

    /**
     * @param string $name
     * @param array  $in_params
     * @param array  $out_params
     *
     * @throws \Exception
     * @return mixed
     */
    public function callProcedure($name, array $in_params, array &$out_params)
    {
        if (!$this->supportsResourceType(DbResourceTypes::TYPE_PROCEDURE)) {
            throw new BadRequestException('Stored Procedures are not supported by this database connection.');
        }

        if (null === $procedure = $this->getProcedure($name)) {
            throw new NotFoundException("Procedure '$name' can not be found.");
        }

        $paramSchemas = $procedure->getParameters();
        $values = $this->determineRoutineValues($paramSchemas, $in_params);

        $sql = $this->getProcedureStatement($procedure, $paramSchemas, $values);

        /** @type \PDOStatement $statement */
        if (!$statement = $this->connection->getPdo()->prepare($sql)) {
            throw new InternalServerErrorException('Failed to prepare statement: ' . $sql);
        }

        // do binding
        $this->doRoutineBinding($statement, $paramSchemas, $values);

        // support multiple result sets
        $result = [];
        try {
            $statement->execute();
            $reader = new DataReader($statement);
            $reader->setFetchMode(static::ROUTINE_FETCH_MODE);
            do {
                try {
                    if (0 < $reader->getColumnCount()) {
                        $temp = $reader->readAll();
                    }
                } catch (\Exception $ex) {
                    // latest oracle driver seems to kick this back for all OUT params even though it works, ignore for now
                    if (false === stripos($ex->getMessage(),
                            'ORA-24374: define not done before fetch or execute and fetch')
                    ) {
                        throw $ex;
                    }
                }
                if (!empty($temp)) {
                    $keep = true;
                    if (1 == count($temp)) {
                        $check = array_change_key_case(current($temp), CASE_LOWER);
                        foreach ($paramSchemas as $key => $paramSchema) {
                            switch ($paramSchema->paramType) {
                                case 'OUT':
                                case 'INOUT':
                                    if (array_key_exists($key, $check)) {
                                        $values[$paramSchema->name] = $check[$key];
                                        // todo problem here if the result contains field name = param name!
                                        $keep = false;
                                    }
                                    break;
                            }
                        }
                    }
                    if ($keep) {
                        $result[] = $temp;
                    }
                }
            } while ($reader->nextResult());
        } catch (\Exception $ex) {
            if (!$this->handleRoutineException($ex)) {
                $errorInfo = $ex instanceof \PDOException ? $ex : null;
                $message = $ex->getMessage();
                throw new \Exception($message, (int)$ex->getCode(), $errorInfo);
            }
        }

        // if there is only one data set, just return it
        if (1 == count($result)) {
            $result = $result[0];
        }

        // any post op?
        $this->postProcedureCall($paramSchemas, $values);

        $values = array_change_key_case($values, CASE_LOWER);
        foreach ($paramSchemas as $key => $paramSchema) {
            switch ($paramSchema->paramType) {
                case 'OUT':
                case 'INOUT':
                    if (array_key_exists($key, $values)) {
                        $value = $values[$key];
                        $out_params[$paramSchema->name] = $this->typecastToClient($value, $paramSchema);
                    }
                    break;
            }
        }

        return $result;
    }

    protected static function cleanParameters(array $param_schemas, array $in_params)
    {
        $out = [];
        foreach ($in_params as $key => $value) {
            if (is_string($key)) {
                // $key is name, check if we have array with value
                if (is_array($value)) {
                    $value = array_get(array_change_key_case($value, CASE_LOWER), 'value');
                }
                $out[strtolower($key)] = $value;
            } else {
                if (is_array($value)) {
                    $param = array_change_key_case($value, CASE_LOWER);
                    if (array_key_exists('name', $param)) {
                        $out[strtolower($param['name'])] = array_get($param, 'value');
                    }
                } else {
                    if ($name = array_get(array_keys($param_schemas), $key)) {
                        $out[$name] = $value;
                    }
                }
            }
        }

        return $out;
    }

    /**
     * @param array $param_schemas
     * @param array $in_params
     *
     * @return array
     * @throws \DreamFactory\Core\Exceptions\BadRequestException
     */
    protected function determineRoutineValues(array $param_schemas, array $in_params)
    {
        $in_params = static::cleanParameters($param_schemas, $in_params);
        $values = [];
        $index = -1;
        // key is lowercase index
        foreach ($param_schemas as $key => $paramSchema) {
            $index++;
            switch ($paramSchema->paramType) {
                case 'IN':
                case 'INOUT':
                    if (array_key_exists($key, $in_params)) {
                        $value = $in_params[$key];
                    } else {
                        $value = $paramSchema->defaultValue;
                    }
                    $values[$key] = $this->typecastToClient($value, $paramSchema);
                    break;
                case 'OUT':
                    $values[$key] = null;
                    break;
                default:
                    break;
            }
        }

        return $values;
    }

    /**
     * @param       $statement
     * @param array $paramSchemas
     * @param array $values
     */
    protected function doRoutineBinding($statement, array $paramSchemas, array &$values)
    {
        // do binding
        foreach ($paramSchemas as $key => $paramSchema) {
            switch ($paramSchema->paramType) {
                case 'IN':
                    $this->bindValue($statement, ':' . $paramSchema->name, array_get($values, $key));
                    break;
                case 'INOUT':
                case 'OUT':
                    $pdoType = $this->extractPdoType($paramSchema->type);
//                    $values[$key] = $this->formatValue($values[$key], $paramSchema->type);
                    $this->bindParam(
                        $statement, ':' . $paramSchema->name,
                        $values[$key],
                        $pdoType | \PDO::PARAM_INPUT_OUTPUT,
                        $paramSchema->length
                    );
                    break;
            }
        }
    }

    /**
     * @param RoutineSchema $routine
     * @param array         $param_schemas
     * @param array         $values
     *
     * @return string
     */
    protected function getProcedureStatement(RoutineSchema $routine, array $param_schemas, array &$values)
    {
        $paramStr = $this->getRoutineParamString($param_schemas, $values);

        return "CALL {$routine->quotedName}($paramStr)";
    }

    /**
     * @param array $param_schemas
     * @param array $values
     */
    protected function postProcedureCall(array $param_schemas, array &$values)
    {
    }

    /**
     * @param \PDOStatement $statement
     * @param               $name
     * @param               $value
     * @param null          $dataType
     * @param null          $length
     * @param null          $driverOptions
     */
    public function bindParam($statement, $name, &$value, $dataType = null, $length = null, $driverOptions = null)
    {
        if ($dataType === null) {
            $statement->bindParam($name, $value, $this->getPdoType(gettype($value)));
        } elseif ($length === null) {
            $statement->bindParam($name, $value, $dataType);
        } elseif ($driverOptions === null) {
            $statement->bindParam($name, $value, $dataType, $length);
        } else {
            $statement->bindParam($name, $value, $dataType, $length, $driverOptions);
        }
    }

    /**
     * Binds a value to a parameter.
     *
     * @param \PDOStatement $statement
     * @param mixed         $name     Parameter identifier. For a prepared statement
     *                                using named placeholders, this will be a parameter name of
     *                                the form :name. For a prepared statement using question mark
     *                                placeholders, this will be the 1-indexed position of the parameter.
     * @param mixed         $value    The value to bind to the parameter
     * @param integer       $dataType SQL data type of the parameter. If null, the type is determined by the PHP type
     *                                of the value.
     *
     * @see http://www.php.net/manual/en/function.PDOStatement-bindValue.php
     */
    public function bindValue($statement, $name, $value, $dataType = null)
    {
        if ($dataType === null) {
            $statement->bindValue($name, $value, $this->getPdoType(gettype($value)));
        } else {
            $statement->bindValue($name, $value, $dataType);
        }
    }

    /**
     * Binds a list of values to the corresponding parameters.
     * This is similar to {@link bindValue} except that it binds multiple values.
     * Note that the SQL data type of each value is determined by its PHP type.
     *
     * @param \PDOStatement $statement
     * @param array         $values the values to be bound. This must be given in terms of an associative
     *                              array with array keys being the parameter names, and array values the corresponding
     *                              parameter values. For example, <code>array(':name'=>'John', ':age'=>25)</code>.
     */
    public function bindValues($statement, $values)
    {
        foreach ($values as $name => $value) {
            $statement->bindValue($name, $value, $this->getPdoType(gettype($value)));
        }
    }

    /**
     * Extracts the PHP PDO type from DF type.
     *
     * @param string $type DF type
     *
     * @return int|null
     */
    public static function extractPdoType($type)
    {
        switch ($type) {
            case DbSimpleTypes::TYPE_BINARY:
                return \PDO::PARAM_LOB;
            default:
                switch (static::extractPhpType($type)) {
                    case 'boolean':
                        return \PDO::PARAM_BOOL;
                    case 'int':
                        return \PDO::PARAM_INT;
                    case 'string':
                        return \PDO::PARAM_STR;
                }
        }

        return null;
    }

    /**
     * Determines the PDO type for the specified PHP type.
     *
     * @param string $type The PHP type (obtained by gettype() call).
     *
     * @return integer the corresponding PDO type
     */
    public function getPdoType($type)
    {
        static $map = [
            'boolean'  => \PDO::PARAM_BOOL,
            'integer'  => \PDO::PARAM_INT,
            'string'   => \PDO::PARAM_STR,
            'resource' => \PDO::PARAM_LOB,
            'NULL'     => \PDO::PARAM_NULL,
        ];

        return isset($map[$type]) ? $map[$type] : \PDO::PARAM_STR;
    }

    /**
     * @param      $type
     * @param null $size
     * @param null $scale
     *
     * @return string
     */
    public function extractSimpleType($type, $size = null, $scale = null)
    {
        switch (strtolower($type)) {
            case 'bit':
            case (false !== strpos($type, 'bool')):
                $value = DbSimpleTypes::TYPE_BOOLEAN;
                break;

            case 'number': // Oracle for boolean, integers and decimals
                if ($size == 1) {
                    $value = DbSimpleTypes::TYPE_BOOLEAN;
                } elseif (empty($scale)) {
                    $value = DbSimpleTypes::TYPE_INTEGER;
                } else {
                    $value = DbSimpleTypes::TYPE_DECIMAL;
                }
                break;

            case 'decimal':
            case 'numeric':
            case 'percent':
                $value = DbSimpleTypes::TYPE_DECIMAL;
                break;

            case (false !== strpos($type, 'double')):
                $value = DbSimpleTypes::TYPE_DOUBLE;
                break;

            case 'real':
            case (false !== strpos($type, 'float')):
                if ($size == 53) {
                    $value = DbSimpleTypes::TYPE_DOUBLE;
                } else {
                    $value = DbSimpleTypes::TYPE_FLOAT;
                }
                break;

            case (false !== strpos($type, 'money')):
                $value = DbSimpleTypes::TYPE_MONEY;
                break;

            case 'binary_integer': // oracle integer
            case 'tinyint':
            case 'smallint':
            case 'mediumint':
            case 'int':
            case 'integer':
            case 'serial': // Informix
                // watch out for point here!
                if ($size == 1) {
                    $value = DbSimpleTypes::TYPE_BOOLEAN;
                } else {
                    $value = DbSimpleTypes::TYPE_INTEGER;
                }
                break;

            case 'varint': // java type used in cassandra, possibly others, can be really big
            case 'bigint':
            case 'bigserial': // Informix
            case 'serial8': // Informix
                // bigint too big to represent as number in php
                $value = DbSimpleTypes::TYPE_BIG_INT;
                break;

            case (false !== strpos($type, 'timestamp')):
            case 'datetimeoffset': //  MSSQL
                $value = DbSimpleTypes::TYPE_TIMESTAMP;
                break;

            case (false !== strpos($type, 'datetime')):
                $value = DbSimpleTypes::TYPE_DATETIME;
                break;

            case 'date':
                $value = DbSimpleTypes::TYPE_DATE;
                break;

            case 'timeuuid': // type 1 time-based UUID
                $value = DbSimpleTypes::TYPE_TIME_UUID;
                break;

            case (false !== strpos($type, 'time')):
                $value = DbSimpleTypes::TYPE_TIME;
                break;

            case (false !== strpos($type, 'binary')):
            case (false !== strpos($type, 'blob')):
                $value = DbSimpleTypes::TYPE_BINARY;
                break;

            //	String types
            case (false !== strpos($type, 'clob')):
            case (false !== strpos($type, 'text')):
            case 'lvarchar': // informix
                $value = DbSimpleTypes::TYPE_TEXT;
                break;

            case 'varchar':
                if ($size == -1) {
                    $value = DbSimpleTypes::TYPE_TEXT; // varchar(max) in MSSQL
                } else {
                    $value = DbSimpleTypes::TYPE_STRING;
                }
                break;

            case 'uuid':
                $value = DbSimpleTypes::TYPE_UUID;
                break;

            // common routine return types
            case 'ref cursor':
                $value = DbSimpleTypes::TYPE_REF_CURSOR;
                break;

            case 'table':
                $value = DbSimpleTypes::TYPE_TABLE;
                break;

            case 'array':
                $value = DbSimpleTypes::TYPE_ARRAY;
                break;

            case 'column':
                $value = DbSimpleTypes::TYPE_COLUMN;
                break;

            case 'row':
                $value = DbSimpleTypes::TYPE_ROW;
                break;

            case 'string':
            case (false !== strpos($type, 'char')):
            default:
                $value = DbSimpleTypes::TYPE_STRING; // default to string to handle anything
                break;
        }

        return $value;
    }

    /**
     * Extracts the DreamFactory simple type from DB type.
     *
     * @param ColumnSchema $column
     * @param string       $dbType DB type
     */
    public function extractType(ColumnSchema $column, $dbType)
    {
        $simpleType = strstr($dbType, '(', true);
        $dbType = strtolower($simpleType ?: $dbType);

        $column->type = static::extractSimpleType($dbType, $column->size, $column->scale);
    }

    /**
     * @param $dbType
     *
     * @return bool
     */
    public function extractMultiByteSupport($dbType)
    {
        switch ($dbType) {
            case (false !== strpos($dbType, 'national')):
            case (false !== strpos($dbType, 'nchar')):
            case (false !== strpos($dbType, 'nvarchar')):
                return true;
        }

        return false;
    }

    /**
     * @param $dbType
     *
     * @return bool
     */
    public function extractFixedLength($dbType)
    {
        switch ($dbType) {
            case ((false !== strpos($dbType, 'char')) && (false === strpos($dbType, 'var'))):
            case 'binary':
                return true;
        }

        return false;
    }

    /**
     * Extracts size, precision and scale information from column's DB type.
     *
     * @param ColumnSchema $field
     * @param string       $dbType the column's DB type
     */
    public function extractLimit(ColumnSchema $field, $dbType)
    {
        if (strpos($dbType, '(') && preg_match('/\((.*)\)/', $dbType, $matches)) {
            $values = explode(',', $matches[1]);
            $field->size = (int)$values[0];
            if (isset($values[1])) {
                $field->precision = (int)$values[0];
                $field->scale = (int)$values[1];
            }
        }
    }
}
