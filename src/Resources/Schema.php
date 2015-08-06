<?php
namespace DreamFactory\Core\SqlDb\Resources;

use DreamFactory\Core\Enums\ApiOptions;
use DreamFactory\Library\Utility\ArrayUtils;
use DreamFactory\Library\Utility\Inflector;
use DreamFactory\Core\Enums\VerbsMask;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Exceptions\NotFoundException;
use DreamFactory\Core\Exceptions\RestException;
use DreamFactory\Core\Resources\BaseDbSchemaResource;
use DreamFactory\Core\SqlDb\Components\SqlDbResource;
use DreamFactory\Core\SqlDb\Components\TableDescriber;
use DreamFactory\Core\SqlDbCore\Command;
use DreamFactory\Core\SqlDbCore\ColumnSchema;
use DreamFactory\Core\SqlDbCore\TableSchema;
use DreamFactory\Core\Utility\DbUtilities;

class Schema extends BaseDbSchemaResource
{
    //*************************************************************************
    //	Traits
    //*************************************************************************

    use SqlDbResource;
    use TableDescriber;

    //*************************************************************************
    //	Methods
    //*************************************************************************

    /**
     * {@inheritdoc}
     */
    public function listTables($schema = null, $refresh = false)
    {
        return $this->dbConn->getSchema()->getTableNames($schema, true, $refresh);
    }

    /**
     * {@inheritdoc}
     */
    public function getResources($only_handlers = false)
    {
        if ($only_handlers) {
            return [];
        }

        $refresh = $this->request->getParameterAsBool(ApiOptions::REFRESH);
        $schema = $this->request->getParameter(ApiOptions::SCHEMA, '');

        $result = $this->listTables($schema, $refresh);

        $extras = $this->parent->getSchemaExtrasForTables($result, false, 'table,label,plural');

        $resources = [];
        foreach ($result as $name) {
            $access = $this->getPermissions($name);
            if (!empty($access)) {
                $label = '';
                $plural = '';
                foreach ($extras as $each) {
                    if (0 == strcasecmp($name, ArrayUtils::get($each, 'table', ''))) {
                        $label = ArrayUtils::get($each, 'label');
                        $plural = ArrayUtils::get($each, 'plural');
                        break;
                    }
                }

                if (empty($label)) {
                    $label = Inflector::camelize($name, ['_', '.'], true);
                }

                if (empty($plural)) {
                    $plural = Inflector::pluralize($label);
                }

                $resources[] =
                    [
                        'name'   => $name,
                        'label'  => $label,
                        'plural' => $plural,
                        'access' => VerbsMask::maskToArray($access)
                    ];
            }
        }

        return $resources;
    }

    public function listAccessComponents($schema = null, $refresh = false)
    {
        $output = [];
        $result = $this->listTables($schema, $refresh);
        foreach ($result as $name) {
            $output[] = static::RESOURCE_NAME . '/' . $name;
        }

        return $output;
    }

    /**
     * {@inheritdoc}
     */
    public function describeTable($name, $refresh = false)
    {
        if (empty($name)) {
            throw new BadRequestException('Table name can not be empty.');
        }

        $this->correctTableName($name);
        try {
            $table = $this->dbConn->getSchema()->getTable($name, $refresh);
            if (!$table) {
                throw new NotFoundException("Table '$name' does not exist in the database.");
            }

            $extras = $this->parent->getSchemaExtrasForTables($name);
            $extras = DbUtilities::reformatFieldLabelArray($extras);
            $result = static::mergeTableExtras($table->toArray(), $extras);
            $result['access'] = $this->getPermissions($name);

            return $result;
        } catch (RestException $ex) {
            throw $ex;
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to query database schema.\n{$ex->getMessage()}");
        }
    }

    /**
     * {@inheritdoc}
     */
    public function describeField($table, $field, $refresh = false)
    {
        if (empty($table)) {
            throw new BadRequestException('Table name can not be empty.');
        }

        try {
            $result = $this->describeTableFields($table, $field);

            return ArrayUtils::get($result, 0);
        } catch (RestException $ex) {
            throw $ex;
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Error describing database table '$table' field '$field'.\n" .
                $ex->getMessage(), $ex->getCode());
        }
    }

    /**
     * {@inheritdoc}
     */
    public function createTables($tables, $check_exist = false, $return_schema = false)
    {
        $tables = DbUtilities::validateAsArray($tables, null, true, 'There are no table sets in the request.');

        // check for system tables and deny
        foreach ($tables as $table) {
            if (null === ($name = ArrayUtils::get($table, 'name'))) {
                throw new BadRequestException("Table schema received does not have a valid name.");
            }
        }

        $result = $this->updateTablesInternal($tables);

        //  Any changes here should refresh cached schema
        static::refreshCachedTables();

        if ($return_schema) {
            return $this->describeTables($tables);
        }

        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function createTable($table, $properties = [], $check_exist = false, $return_schema = false)
    {
        $properties = ArrayUtils::clean($properties);
        $properties['name'] = $table;

        $tables = DbUtilities::validateAsArray($properties, null, true, 'Bad data format in request.');
        $result = $this->updateTablesInternal($tables);
        $result = ArrayUtils::get($result, 0, []);

        //  Any changes here should refresh cached schema
        static::refreshCachedTables();

        if ($return_schema) {
            return $this->describeTable($table);
        }

        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function createField($table, $field, $properties = [], $check_exist = false, $return_schema = false)
    {
        $properties = ArrayUtils::clean($properties);
        $properties['name'] = $field;

        $fields = DbUtilities::validateAsArray($properties, null, true, 'Bad data format in request.');

        $result = $this->updateFieldsInternal($table, $fields);

        //  Any changes here should refresh cached schema
        static::refreshCachedTables();

        if ($return_schema) {
            return $this->describeField($table, $field);
        }

        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function updateTables($tables, $allow_delete_fields = false, $return_schema = false)
    {
        $tables = DbUtilities::validateAsArray($tables, null, true, 'There are no table sets in the request.');

        foreach ($tables as $table) {
            if (null === ($name = ArrayUtils::get($table, 'name'))) {
                throw new BadRequestException("Table schema received does not have a valid name.");
            }
        }

        $result = $this->updateTablesInternal($tables, true, $allow_delete_fields);

        //  Any changes here should refresh cached schema
        static::refreshCachedTables();

        if ($return_schema) {
            return $this->describeTables($tables);
        }

        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function updateTable($table, $properties, $allow_delete_fields = false, $return_schema = false)
    {
        $properties = ArrayUtils::clean($properties);
        $properties['name'] = $table;

        $tables = DbUtilities::validateAsArray($properties, null, true, 'Bad data format in request.');

        $result = $this->updateTablesInternal($tables, true, $allow_delete_fields);
        $result = ArrayUtils::get($result, 0, []);

        //  Any changes here should refresh cached schema
        static::refreshCachedTables();

        if ($return_schema) {
            return $this->describeTable($table);
        }

        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function updateField($table, $field, $properties = [], $allow_delete_parts = false, $return_schema = false)
    {
        if (empty($table)) {
            throw new BadRequestException('Table name can not be empty.');
        }

        $properties = ArrayUtils::clean($properties);
        $properties['name'] = $field;

        $fields = DbUtilities::validateAsArray($properties, null, true, 'Bad data format in request.');

        $result = $this->updateFieldsInternal($table, $fields, true);

        //  Any changes here should refresh cached schema
        static::refreshCachedTables();

        if ($return_schema) {
            return $this->describeField($table, $field);
        }

        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function deleteTable($table, $check_empty = false)
    {
        if (empty($table)) {
            throw new BadRequestException('Table name can not be empty.');
        }

        //  Any changes here should refresh cached schema
        static::refreshCachedTables();

        //  Does it exist
        if (!$this->doesTableExist($table)) {
            throw new NotFoundException('Table "' . $table . '" not found.');
        }
        try {
            $this->dbConn->createCommand()->dropTable($table);
        } catch (\Exception $ex) {
            \Log::error('Exception dropping table: ' . $ex->getMessage());

            throw $ex;
        }

        //  Any changes here should refresh cached schema
        static::refreshCachedTables();

        DbUtilities::removeSchemaExtrasForTables($this->serviceId, $table);
    }

    /**
     * {@inheritdoc}
     */
    public function deleteField($table, $field)
    {
        if (empty($table)) {
            throw new BadRequestException('Table name can not be empty.');
        }

        // does it already exist
        if (!$this->doesTableExist($table)) {
            throw new NotFoundException("A table with name '$table' does not exist in the database.");
        }
        try {
            $this->dbConn->createCommand()->dropColumn($table, $field);
        } catch (\Exception $ex) {
            error_log($ex->getMessage());
            throw $ex;
        }

        //  Any changes here should refresh cached schema
        static::refreshCachedTables();

        DbUtilities::removeSchemaExtrasForFields($this->serviceId, $table, $field);
    }

    /**
     * @param string $name
     *
     * @throws BadRequestException
     * @throws NotFoundException
     * @return string
     */
    public function correctTableName(&$name)
    {
        if (false !== ($table = $this->doesTableExist($name, true))) {
            $name = $table;
        } else {
            throw new NotFoundException('Table "' . $name . '" does not exist in the database.');
        }
    }

    /**
     * @param string $name       The name of the table to check
     * @param bool   $returnName If true, the table name is returned instead of TRUE
     *
     * @throws \InvalidArgumentException
     * @return bool
     */
    public function doesTableExist($name, $returnName = false)
    {
        if (empty($name)) {
            throw new \InvalidArgumentException('Table name cannot be empty.');
        }

        //  Build the lower-cased table array
        $tables = $this->dbConn->getSchema()->getTableNames();

        //	Search normal, return real name
        if (false !== array_search($name, $tables)) {
            return $returnName ? $name : true;
        }

        if (false !== $key = array_search(strtolower($name), array_map('strtolower', $tables))) {
            return $returnName ? $tables[$key] : true;
        }

        return false;
    }

    /**
     * @param string                $table_name
     * @param null | string | array $field_names
     * @param bool                  $refresh
     *
     * @throws NotFoundException
     * @throws InternalServerErrorException
     * @return array
     */
    public function describeTableFields($table_name, $field_names = null, $refresh = false)
    {
        $this->correctTableName($table_name);
        $table = $this->dbConn->getSchema()->getTable($table_name, $refresh);
        if (!$table) {
            throw new NotFoundException("Table '$table_name' does not exist in the database.");
        }

        if (!empty($field_names)) {
            $field_names = DbUtilities::validateAsArray($field_names, ',', true, 'No valid field names given.');
            $extras = $this->parent->getSchemaExtrasForFields($table_name, $field_names);
        } else {
            $extras = $this->parent->getSchemaExtrasForTables($table_name);
        }

        $extras = DbUtilities::reformatFieldLabelArray($extras);

        $out = [];
        try {
            /** @var ColumnSchema $column */
            foreach ($table->columns as $column) {

                if (empty($field_names) || (false !== array_search($column->name, $field_names))) {
                    $info = ArrayUtils::get($extras, $column->name, []);
                    $out[] = static::mergeFieldExtras($column->toArray(), $info);
                }
            }
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to query table field schema.\n{$ex->getMessage()}");
        }

        if (empty($out)) {
            throw new NotFoundException("No requested fields found in table '$table_name'.");
        }

        return $out;
    }

    /**
     * @param string $table_name
     * @param array  $fields
     * @param bool   $allow_update
     * @param bool   $allow_delete
     *
     * @return array
     * @throws \Exception
     */
    public function updateFieldsInternal($table_name, $fields, $allow_update = false, $allow_delete = false)
    {
        if (empty($table_name)) {
            throw new BadRequestException("Table schema received does not have a valid name.");
        }

        // does it already exist
        if (!$this->doesTableExist($table_name)) {
            throw new NotFoundException("Update schema called on a table with name '$table_name' that does not exist in the database.");
        }

        $schema = $this->describeTable($table_name);

        try {
            $names = [];
            $results = $this->buildTableFields($table_name, $fields, $schema, $allow_update, $allow_delete);
            /** @var Command $command */
            $command = $this->dbConn->createCommand();
            $columns = ArrayUtils::get($results, 'columns', []);
            foreach ($columns as $name => $definition) {
                $command->reset();
                $command->addColumn($table_name, $name, $definition);
                $names[] = $name;
            }
            $columns = ArrayUtils::get($results, 'alter_columns', []);
            foreach ($columns as $name => $definition) {
                $command->reset();
                $command->alterColumn($table_name, $name, $definition);
                $names[] = $name;
            }
            $columns = ArrayUtils::get($results, 'drop_columns', []);
            foreach ($columns as $name) {
                $command->reset();
                $command->dropColumn($table_name, $name);
                $names[] = $name;
            }
            static::createFieldExtras($results);

            $labels = ArrayUtils::get($results, 'labels', null, true);
            if (!empty($labels)) {
                DbUtilities::setSchemaExtras($this->serviceId, $labels);
            }

            return ['names' => $names];
        } catch (\Exception $ex) {
            \Log::error('Exception updating fields: ' . $ex->getMessage());
            throw $ex;
        }
    }

    /**
     * @param array $tables
     * @param bool  $allow_merge
     * @param bool  $allow_delete
     * @param bool  $rollback
     *
     * @throws \Exception
     * @return array
     */
    public function updateTablesInternal($tables, $allow_merge = false, $allow_delete = false, $rollback = false)
    {
        $tables = DbUtilities::validateAsArray($tables, null, true, 'There are no table sets in the request.');

        $created = $references = $indexes = $labels = $out = [];
        $count = 0;
        $singleTable = (1 == count($tables));

        foreach ($tables as $table) {
            try {
                if (null === ($tableName = ArrayUtils::get($table, 'name'))) {
                    throw new BadRequestException('Table name missing from schema.');
                }

                //	Does it already exist
                if ($this->doesTableExist($tableName)) {
                    if (!$allow_merge) {
                        throw new BadRequestException("A table with name '$tableName' already exist in the database.");
                    }

                    \Log::debug('Schema update: ' . $tableName);

                    $oldSchema = $this->describeTable($tableName);

                    $results = $this->updateTableInternal($tableName, $table, $oldSchema, $allow_delete);
                } else {
                    \Log::debug('Creating table: ' . $tableName);

                    $results = $this->createTableInternal($tableName, $table, false);

                    if (!$singleTable && $rollback) {
                        $created[] = $tableName;
                    }
                }

                $labels = array_merge($labels, ArrayUtils::get($results, 'labels', []));
                $references = array_merge($references, ArrayUtils::get($results, 'references', []));
                $indexes = array_merge($indexes, ArrayUtils::get($results, 'indexes', []));
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

        $results = ['references' => $references, 'indexes' => $indexes];
        static::createFieldExtras($results);

        if (!empty($labels)) {
            DbUtilities::setSchemaExtras($this->serviceId, $labels);
        }

        return $out;
    }

    /**
     * @param $type
     *
     * @return int | null
     */
    public static function determinePdoBindingType($type)
    {
        switch ($type) {
            case 'boolean':
                return \PDO::PARAM_BOOL;

            case 'integer':
            case 'id':
            case 'reference':
            case 'user_id':
            case 'user_id_on_create':
            case 'user_id_on_update':
                return \PDO::PARAM_INT;

            case 'string':
                return \PDO::PARAM_STR;
                break;
        }

        return null;
    }

    /**
     * @param string           $table_name
     * @param array            $fields
     * @param null|TableSchema $schema
     * @param bool             $allow_update
     * @param bool             $allow_delete
     *
     * @throws \Exception
     * @return string
     */
    protected function buildTableFields(
        $table_name,
        $fields,
        $schema = null,
        $allow_update = false,
        $allow_delete = false
    ){
        $fields =
            DbUtilities::validateAsArray($fields, null, true, "No valid fields exist in the received table schema.");

        $columns = [];
        $alterColumns = [];
        $references = [];
        $indexes = [];
        $labels = [];
        $dropColumns = [];
        $oldFields = [];
        $extraCommands = [];
        if ($schema) {
            foreach (ArrayUtils::clean(ArrayUtils::get($schema, 'field')) as $old) {
                $old = array_change_key_case($old, CASE_LOWER);

                $oldFields[ArrayUtils::get($old, 'name')] = $old;
            }
        }
        $newFields = [];
        foreach ($fields as $field) {
            $newFields[ArrayUtils::get($field, 'name')] = array_change_key_case($field, CASE_LOWER);
        }

        if ($allow_delete && !empty($oldFields)) {
            // check for columns to drop
            foreach ($oldFields as $oldName => $oldField) {
                if (!isset($newFields[$oldName])) {
                    $dropColumns[] = $oldName;
                }
            }
        }

        foreach ($newFields as $name => $field) {
            if (empty($name)) {
                throw new BadRequestException("Invalid schema detected - no name element.");
            }

            $isAlter = isset($oldFields[$name]);
            if ($isAlter && !$allow_update) {
                throw new BadRequestException("Field '$name' already exists in table '$table_name'.");
            }

            $oldField = ($isAlter) ? $oldFields[$name] : [];
            $oldForeignKey = ArrayUtils::get($oldField, 'is_foreign_key', false);
            $temp = [];

            // if same as old, don't bother
            if (!empty($oldField)) {
                $same = true;

                foreach ($field as $key => $value) {
                    switch (strtolower($key)) {
                        case 'label':
                        case 'value':
                        case 'validation':
                            break;
                        default:
                            if (isset($oldField[$key])) // could be extra stuff from client
                            {
                                if ($value != $oldField[$key]) {
                                    $same = false;
                                    break 2;
                                }
                            }
                            break;
                    }
                }

                if ($same) {
                    continue;
                }
            }

            $type = strtolower(ArrayUtils::get($field, 'type', ''));

            switch ($type) {
                case 'user_id':
                    $field['type'] = 'int';
                    $temp['user_id'] = true;
                    break;
                case 'user_id_on_create':
                    $field['type'] = 'int';
                    $temp['user_id_on_update'] = false;
                    break;
                case 'user_id_on_update':
                    $field['type'] = 'int';
                    $temp['user_id_on_update'] = true;
                    break;
                case 'timestamp_on_create':
                    $temp['timestamp_on_update'] = false;
                    break;
                case 'timestamp_on_update':
                    $temp['timestamp_on_update'] = true;
                    break;
                case 'id':
                case 'pk':
                    $pkExtras = $this->dbConn->getSchema()->getPrimaryKeyCommands($table_name, $name);
                    $extraCommands = array_merge($extraCommands, $pkExtras);
                    break;
            }

            if (('reference' == $type) || ArrayUtils::getBool($field, 'is_foreign_key')) {
                // special case for references because the table referenced may not be created yet
                $refTable = ArrayUtils::get($field, 'ref_table');
                if (empty($refTable)) {
                    throw new BadRequestException("Invalid schema detected - no table element for reference type of $name.");
                }
                $refColumns = ArrayUtils::get($field, 'ref_fields', 'id');
                $refOnDelete = ArrayUtils::get($field, 'ref_on_delete');
                $refOnUpdate = ArrayUtils::get($field, 'ref_on_update');

                // will get to it later, $refTable may not be there
                $keyName = $this->dbConn->getSchema()->makeConstraintName('fk', $table_name, $name);
                if (!$isAlter || !$oldForeignKey) {
                    $references[] = [
                        'name'       => $keyName,
                        'table'      => $table_name,
                        'column'     => $name,
                        'ref_table'  => $refTable,
                        'ref_fields' => $refColumns,
                        'delete'     => $refOnDelete,
                        'update'     => $refOnUpdate
                    ];
                }
            }

            // regardless of type
            if (ArrayUtils::getBool($field, 'is_unique')) {
                // will get to it later, create after table built
                $keyName = $this->dbConn->getSchema()->makeConstraintName('undx', $table_name, $name);
                $indexes[] = [
                    'name'   => $keyName,
                    'table'  => $table_name,
                    'column' => $name,
                    'unique' => true,
                    'drop'   => $isAlter
                ];
            } elseif (ArrayUtils::get($field, 'is_index')) {
                // will get to it later, create after table built
                $keyName = $this->dbConn->getSchema()->makeConstraintName('ndx', $table_name, $name);
                $indexes[] = [
                    'name'   => $keyName,
                    'table'  => $table_name,
                    'column' => $name,
                    'drop'   => $isAlter
                ];
            }

            $values = ArrayUtils::get($field, 'value');
            if (empty($values)) {
                $values = ArrayUtils::getDeep($field, 'values', 'value', []);
            }
            if (!is_array($values)) {
                $values = array_map('trim', explode(',', trim($values, ',')));
            }
            if (!empty($values) && ($values != ArrayUtils::get($oldField, 'value'))) {
                $picklist = '';
                foreach ($values as $value) {
                    if (!empty($picklist)) {
                        $picklist .= "\r";
                    }
                    $picklist .= $value;
                }
                if (!empty($picklist)) {
                    $temp['picklist'] = $picklist;
                }
            }

            // labels
            $label = ArrayUtils::get($field, 'label');
            if (!empty($label) && ($label != ArrayUtils::get($oldField, 'label'))) {
                $temp['label'] = $label;
            }

            $validation = ArrayUtils::get($field, 'validation');
            if (!empty($validation) && ($validation != ArrayUtils::get($oldField, 'validation'))) {
                $temp['validation'] = json_encode($validation);
            }

            if (!empty($temp)) {
                $temp['table'] = $table_name;
                $temp['field'] = $name;
                $labels[] = $temp;
            }

            if ($isAlter) {
                $alterColumns[$name] = $field;
            } else {
                $columns[$name] = $field;
            }
        }

        return [
            'columns'       => $columns,
            'alter_columns' => $alterColumns,
            'drop_columns'  => $dropColumns,
            'references'    => $references,
            'indexes'       => $indexes,
            'labels'        => $labels,
            'extras'        => $extraCommands
        ];
    }

    /**
     * @param array $extras
     *
     * @return array
     */
    protected function createFieldExtras($extras)
    {
        /** @var Command $command */
        $command = $this->dbConn->createCommand();
        $references = ArrayUtils::get($extras, 'references', []);
        if (!empty($references)) {
            foreach ($references as $reference) {
                $command->reset();
                $name = $reference['name'];
                $table = $reference['table'];
                $drop = ArrayUtils::getBool($reference, 'drop');
                if ($drop) {
                    try {
                        $command->dropForeignKey($name, $table);
                    } catch (\Exception $ex) {
                        \Log::debug($ex->getMessage());
                    }
                }
                // add new reference
                $refTable = ArrayUtils::get($reference, 'ref_table');
                if (!empty($refTable)) {
                    /** @noinspection PhpUnusedLocalVariableInspection */
                    $rows = $command->addForeignKey(
                        $name,
                        $table,
                        $reference['column'],
                        $refTable,
                        $reference['ref_fields'],
                        $reference['delete'],
                        $reference['update']
                    );
                }
            }
        }
        $indexes = ArrayUtils::get($extras, 'indexes', []);
        if (!empty($indexes)) {
            foreach ($indexes as $index) {
                $command->reset();
                $name = $index['name'];
                $table = $index['table'];
                $drop = ArrayUtils::getBool($index, 'drop');
                if ($drop) {
                    try {
                        $command->dropIndex($name, $table);
                    } catch (\Exception $ex) {
                        \Log::debug($ex->getMessage());
                    }
                }
                $unique = ArrayUtils::getBool($index, 'unique');

                /** @noinspection PhpUnusedLocalVariableInspection */
                $rows = $command->createIndex($name, $table, $index['column'], $unique);
            }
        }
    }

    /**
     * @param string $table_name
     * @param array  $data
     * @param bool   $checkExist
     *
     * @throws BadRequestException
     * @throws \Exception
     * @return array
     */
    protected function createTableInternal($table_name, $data, $checkExist = true)
    {
        if (empty($table_name)) {
            throw new BadRequestException("Table schema received does not have a valid name.");
        }

        // does it already exist
        if (true === $checkExist && $this->doesTableExist($table_name)) {
            throw new BadRequestException("A table with name '$table_name' already exist in the database.");
        }

        $data = array_change_key_case($data, CASE_LOWER);
        $fields = ArrayUtils::get($data, 'field');
        try {
            $results = $this->buildTableFields($table_name, $fields);
            $columns = ArrayUtils::get($results, 'columns');

            if (empty($columns)) {
                throw new BadRequestException("No valid fields exist in the received table schema.");
            }

            $this->dbConn->createCommand()->createTable($table_name, $columns);

            $extras = ArrayUtils::get($results, 'extras', null, true);
            if (!empty($extras)) {
                foreach ($extras as $extraCommand) {
                    try {
                        $this->dbConn->createCommand()->setText($extraCommand)->execute();
                    } catch (\Exception $ex) {
                        // oh well, we tried.
                    }
                }
            }

            $labels = ArrayUtils::get($results, 'labels', []);
            // add table labels
            $label = ArrayUtils::get($data, 'label');
            $plural = ArrayUtils::get($data, 'plural');
            if (!empty($label) || !empty($plural)) {
                $labels[] = [
                    'table'  => $table_name,
                    'field'  => '',
                    'label'  => $label,
                    'plural' => $plural
                ];
            }
            $results['labels'] = $labels;

            return $results;
        } catch (\Exception $ex) {
            \Log::error('Exception creating table: ' . $ex->getMessage());
            throw $ex;
        }
    }

    /**
     * @param string $table_name
     * @param array  $data
     * @param array  $old_schema
     * @param bool   $allow_delete
     *
     * @throws \Exception
     * @return array
     */
    protected function updateTableInternal($table_name, $data, $old_schema, $allow_delete = false)
    {
        if (empty($table_name)) {
            throw new BadRequestException("Table schema received does not have a valid name.");
        }
        // does it already exist
        if (!$this->doesTableExist($table_name)) {
            throw new BadRequestException("Update schema called on a table with name '$table_name' that does not exist in the database.");
        }

        //  Is there a name update
        $newName = ArrayUtils::get($data, 'new_name');

        if (!empty($newName)) {
            // todo change table name, has issue with references
        }

        // update column types

        $labels = [];
        $references = [];
        $indexes = [];
        $fields = ArrayUtils::get($data, 'field');
        if (!empty($fields)) {
            try {
                /** @var Command $command */
                $command = $this->dbConn->createCommand();
                $results = $this->buildTableFields($table_name, $fields, $old_schema, true, $allow_delete);
                $columns = ArrayUtils::get($results, 'columns', []);
                foreach ($columns as $name => $definition) {
                    $command->reset();
                    $command->addColumn($table_name, $name, $definition);
                }
                $columns = ArrayUtils::get($results, 'alter_columns', []);
                foreach ($columns as $name => $definition) {
                    $command->reset();
                    $command->alterColumn($table_name, $name, $definition);
                }
                $columns = ArrayUtils::get($results, 'drop_columns', []);
                foreach ($columns as $name) {
                    $command->reset();
                    $command->dropColumn($table_name, $name);
                }
            } catch (\Exception $ex) {
                \Log::error('Exception updating table: ' . $ex->getMessage());
                throw $ex;
            }

            $labels = ArrayUtils::get($results, 'labels', []);
            $references = ArrayUtils::get($results, 'references', []);
            $indexes = ArrayUtils::get($results, 'indexes', []);
        }

        // add table labels
        $label = ArrayUtils::get($data, 'label');
        $plural = ArrayUtils::get($data, 'plural');
        if (!is_null($label) || !is_null($plural)) {
            if (($label != ArrayUtils::get($old_schema, 'label')) &&
                ($plural != ArrayUtils::get($old_schema, 'plural'))
            ) {
                $labels[] = [
                    'table'  => $table_name,
                    'field'  => '',
                    'label'  => $label,
                    'plural' => $plural
                ];
            }
        }

        $results = ['references' => $references, 'indexes' => $indexes, 'labels' => $labels];

        return $results;
    }

    /**
     * Refreshes all schema associated with this db connection:
     *
     * @return array
     */
    public function refreshCachedTables()
    {
        $this->dbConn->getSchema()->refresh();
    }

    public function getApiDocInfo()
    {
        $base = parent::getApiDocInfo();

        $base['models'] = array_merge($base['models'], static::getApiDocCommonModels());

        return $base;
    }
}