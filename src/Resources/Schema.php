<?php
namespace DreamFactory\Core\SqlDb\Resources;

use DreamFactory\Core\Database\Schema\ColumnSchema;
use DreamFactory\Core\Database\Schema\RelationSchema;
use DreamFactory\Core\Database\Schema\TableSchema;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Exceptions\NotFoundException;
use DreamFactory\Core\Exceptions\RestException;
use DreamFactory\Core\Resources\BaseDbSchemaResource;
use DreamFactory\Core\SqlDb\Components\SqlDbResource;
use DreamFactory\Core\SqlDb\Components\TableDescriber;

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
    public function describeTable($name, $refresh = false)
    {
        $name = (is_array($name) ? array_get($name, 'name') :  $name);
        if (empty($name)) {
            throw new BadRequestException('Table name can not be empty.');
        }

        try {
            $table = $this->schema->getTable($name, $refresh);
            if (!$table) {
                throw new NotFoundException("Table '$name' does not exist in the database.");
            }

            $result = $table->toArray();
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

            return array_get($result, 0);
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
    public function describeRelationship($table, $relationship, $refresh = false)
    {
        if (empty($table)) {
            throw new BadRequestException('Table name can not be empty.');
        }

        try {
            $result = $this->describeTableRelationships($table, $relationship);

            return array_get($result, 0);
        } catch (RestException $ex) {
            throw $ex;
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Error describing database table '$table' relationship '$relationship'.\n" .
                $ex->getMessage(), $ex->getCode());
        }
    }

    /**
     * {@inheritdoc}
     */
    public function createTables($tables, $check_exist = false, $return_schema = false)
    {
        $tables = static::validateAsArray($tables, null, true, 'There are no table sets in the request.');

        foreach ($tables as $table) {
            if (null === ($name = array_get($table, 'name'))) {
                throw new BadRequestException("Table schema received does not have a valid name.");
            }
        }

        $result = $this->schema->updateSchema($tables);

        //  Any changes here should refresh cached schema
        $this->refreshCachedTables();

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
        $properties = (is_array($properties) ? $properties : []);
        $properties['name'] = $table;

        $tables = static::validateAsArray($properties, null, true, 'Bad data format in request.');
        $result = $this->schema->updateSchema($tables);
        $result = array_get($result, 0, []);

        //  Any changes here should refresh cached schema
        $this->refreshCachedTables();

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
        $properties = (is_array($properties) ? $properties : []);
        $properties['name'] = $field;

        $fields = static::validateAsArray($properties, null, true, 'Bad data format in request.');

        $tables = [['name' => $table, 'field' => $fields]];
        $result = $this->schema->updateSchema($tables, true);
        $result = array_get(array_get($result, 0, []), 'field', []);

        //  Any changes here should refresh cached schema
        $this->refreshCachedTables();

        if ($return_schema) {
            return $this->describeField($table, $field);
        }

        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function createRelationship($table, $relationship, $properties = [], $check_exist = false, $return_schema = false)
    {
        $properties = (is_array($properties) ? $properties : []);
        $properties['name'] = $relationship;

        $fields = static::validateAsArray($properties, null, true, 'Bad data format in request.');

        $tables = [['name' => $table, 'related' => $fields]];
        $result = $this->schema->updateSchema($tables, true);
        $result = array_get(array_get($result, 0, []), 'related', []);

        //  Any changes here should refresh cached schema
        $this->refreshCachedTables();

        if ($return_schema) {
            return $this->describeRelationship($table, $relationship);
        }

        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function updateTables($tables, $allow_delete_fields = false, $return_schema = false)
    {
        $tables = static::validateAsArray($tables, null, true, 'There are no table sets in the request.');

        foreach ($tables as $table) {
            if (null === ($name = array_get($table, 'name'))) {
                throw new BadRequestException("Table schema received does not have a valid name.");
            }
        }

        $result = $this->schema->updateSchema($tables, true, $allow_delete_fields);

        //  Any changes here should refresh cached schema
        $this->refreshCachedTables();

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
        $properties = (is_array($properties) ? $properties : []);
        $properties['name'] = $table;

        $tables = static::validateAsArray($properties, null, true, 'Bad data format in request.');

        $result = $this->schema->updateSchema($tables, true, $allow_delete_fields);
        $result = array_get($result, 0, []);

        //  Any changes here should refresh cached schema
        $this->refreshCachedTables();

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

        $properties = (is_array($properties) ? $properties : []);
        $properties['name'] = $field;

        $fields = static::validateAsArray($properties, null, true, 'Bad data format in request.');

        $tables = [['name' => $table, 'field' => $fields]];
        $result = $this->schema->updateSchema($tables, true);
        $result = array_get(array_get($result, 0, []), 'field', []);

        //  Any changes here should refresh cached schema
        $this->refreshCachedTables();

        if ($return_schema) {
            return $this->describeField($table, $field);
        }

        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function updateRelationship($table, $relationship, $properties = [], $allow_delete_parts = false, $return_schema = false)
    {
        if (empty($table)) {
            throw new BadRequestException('Table name can not be empty.');
        }

        $properties = (is_array($properties) ? $properties : []);
        $properties['name'] = $relationship;

        $fields = static::validateAsArray($properties, null, true, 'Bad data format in request.');

        $tables = [['name' => $table, 'related' => $fields]];
        $result = $this->schema->updateSchema($tables, true);
        $result = array_get(array_get($result, 0, []), 'related', []);

        //  Any changes here should refresh cached schema
        $this->refreshCachedTables();

        if ($return_schema) {
            return $this->describeRelationship($table, $relationship);
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

        //  Does it exist
        if (!$this->doesTableExist($table)) {
            throw new NotFoundException("Table '$table' not found.");
        }

        try {
            $this->schema->dropTable($table);
        } catch (\Exception $ex) {
            \Log::error('Exception dropping table: ' . $ex->getMessage());

            throw $ex;
        }

        //  Any changes here should refresh cached schema
        $this->refreshCachedTables();
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
            $this->schema->dropColumn($table, $field);
        } catch (\Exception $ex) {
            error_log($ex->getMessage());
            throw $ex;
        }

        //  Any changes here should refresh cached schema
        $this->refreshCachedTables();
    }

    /**
     * {@inheritdoc}
     */
    public function deleteRelationship($table, $relationship)
    {
        if (empty($table)) {
            throw new BadRequestException('Table name can not be empty.');
        }

        // does it already exist
        if (!$this->doesTableExist($table)) {
            throw new NotFoundException("A table with name '$table' does not exist in the database.");
        }

        try {
            $this->schema->dropRelationship($table, $relationship);
        } catch (\Exception $ex) {
            error_log($ex->getMessage());
            throw $ex;
        }

        //  Any changes here should refresh cached schema
        $this->refreshCachedTables();
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

        return $name;
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
        return $this->schema->doesTableExist($name, $returnName);
    }

    /**
     * @param string                $table_name
     * @param null | string | array $fields
     * @param bool                  $refresh
     *
     * @throws NotFoundException
     * @throws InternalServerErrorException
     * @return array
     */
    public function describeTableFields($table_name, $fields = null, $refresh = false)
    {
        $table = $this->schema->getTable($table_name, $refresh);
        if (!$table) {
            throw new NotFoundException("Table '$table_name' does not exist in the database.");
        }

        if (!empty($fields)) {
            $fields = static::validateAsArray($fields, ',', true, 'No valid field names given.');
        }

        $out = [];
        try {
            /** @var ColumnSchema $column */
            foreach ($table->columns as $column) {
                if (empty($fields) || (false !== array_search($column->name, $fields))) {
                    $out[] = $column->toArray();
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
     * @param string                $table_name
     * @param null | string | array $relationships
     * @param bool                  $refresh
     *
     * @throws NotFoundException
     * @throws InternalServerErrorException
     * @return array
     */
    public function describeTableRelationships($table_name, $relationships = null, $refresh = false)
    {
        /** @var TableSchema $table */
        $table = $this->schema->getTable($table_name, $refresh);
        if (!$table) {
            throw new NotFoundException("Table '$table_name' does not exist in the database.");
        }

        if (!empty($relationships)) {
            $relationships = static::validateAsArray($relationships, ',', true, 'No valid relationship names given.');
        }

        $out = [];
        try {
            /** @var RelationSchema $relation */
            foreach ($table->relations as $relation) {
                if (empty($relationships) || (false !== array_search($relation->name, $relationships))) {
                    $out[] = $relation->toArray();
                }
            }
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to query table relationship schema.\n{$ex->getMessage()}");
        }

        if (empty($out)) {
            throw new NotFoundException("No requested relationships found in table '$table_name'.");
        }

        return $out;
    }

    public static function getApiDocInfo($service, array $resource = [])
    {
        $base = parent::getApiDocInfo($service, $resource);

        $base['definitions'] = array_merge($base['definitions'], static::getApiDocCommonModels());

        return $base;
    }
}