<?php
namespace DreamFactory\Core\SqlDb\Resources;

use DreamFactory\Core\Database\ColumnSchema;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Exceptions\NotFoundException;
use DreamFactory\Core\Exceptions\RestException;
use DreamFactory\Core\Resources\BaseDbSchemaResource;
use DreamFactory\Core\SqlDb\Components\SqlDbResource;
use DreamFactory\Core\SqlDb\Components\TableDescriber;
use DreamFactory\Core\Utility\DbUtilities;
use DreamFactory\Library\Utility\ArrayUtils;

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
        if (empty($name)) {
            throw new BadRequestException('Table name can not be empty.');
        }

        try {
            $table = $this->dbConn->getSchema()->getTable($name, $refresh);
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

        foreach ($tables as $table) {
            if (null === ($name = ArrayUtils::get($table, 'name'))) {
                throw new BadRequestException("Table schema received does not have a valid name.");
            }
        }

        $result = $this->dbConn->updateSchema($tables);

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
        $properties = ArrayUtils::clean($properties);
        $properties['name'] = $table;

        $tables = DbUtilities::validateAsArray($properties, null, true, 'Bad data format in request.');
        $result = $this->dbConn->updateSchema($tables);
        $result = ArrayUtils::get($result, 0, []);

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
        $properties = ArrayUtils::clean($properties);
        $properties['name'] = $field;

        $fields = DbUtilities::validateAsArray($properties, null, true, 'Bad data format in request.');

        $result = $this->dbConn->updateFields($table, $fields);

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
    public function updateTables($tables, $allow_delete_fields = false, $return_schema = false)
    {
        $tables = DbUtilities::validateAsArray($tables, null, true, 'There are no table sets in the request.');

        foreach ($tables as $table) {
            if (null === ($name = ArrayUtils::get($table, 'name'))) {
                throw new BadRequestException("Table schema received does not have a valid name.");
            }
        }

        $result = $this->dbConn->updateSchema($tables, true, $allow_delete_fields);

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
        $properties = ArrayUtils::clean($properties);
        $properties['name'] = $table;

        $tables = DbUtilities::validateAsArray($properties, null, true, 'Bad data format in request.');

        $result = $this->dbConn->updateSchema($tables, true, $allow_delete_fields);
        $result = ArrayUtils::get($result, 0, []);

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

        $properties = ArrayUtils::clean($properties);
        $properties['name'] = $field;

        $fields = DbUtilities::validateAsArray($properties, null, true, 'Bad data format in request.');

        $result = $this->dbConn->updateFields($table, $fields, true);

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
    public function deleteTable($table, $check_empty = false)
    {
        if (empty($table)) {
            throw new BadRequestException('Table name can not be empty.');
        }

        //  Does it exist
        if (!$this->doesTableExist($table)) {
            throw new NotFoundException('Table "' . $table . '" not found.');
        }

        try {
            $this->dbConn->dropTable($table);
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
            $this->dbConn->dropColumn($table, $field);
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
        return $this->dbConn->getSchema()->doesTableExist($name, $returnName);
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
        $table = $this->dbConn->getSchema()->getTable($table_name, $refresh);
        if (!$table) {
            throw new NotFoundException("Table '$table_name' does not exist in the database.");
        }

        if (!empty($field_names)) {
            $field_names = DbUtilities::validateAsArray($field_names, ',', true, 'No valid field names given.');
        }

        $out = [];
        try {
            /** @var ColumnSchema $column */
            foreach ($table->columns as $column) {
                if (empty($field_names) || (false !== array_search($column->name, $field_names))) {
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

    public function getApiDocInfo()
    {
        $base = parent::getApiDocInfo();

        $base['models'] = array_merge($base['models'], static::getApiDocCommonModels());

        return $base;
    }
}