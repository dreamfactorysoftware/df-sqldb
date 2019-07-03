<?php

namespace DreamFactory\Core\MySqlDb\Database\Schema;

use DreamFactory\Core\SqlDb\Database\Schema\SqlSchema;
use DreamFactory\Core\Database\Schema\ColumnSchema;
use DreamFactory\Core\Database\Schema\FunctionSchema;
use DreamFactory\Core\Database\Schema\ParameterSchema;
use DreamFactory\Core\Database\Schema\ProcedureSchema;
use DreamFactory\Core\Database\Schema\RoutineSchema;
use DreamFactory\Core\Database\Schema\TableSchema;
use DreamFactory\Core\Enums\DbSimpleTypes;

/**
 * Schema is the class for retrieving metadata information from a MySQL database (version 4.1.x and 5.x).
 */
class MySqlSchema extends SqlSchema
{
    /**
     * @const string Quoting characters
     */
    const LEFT_QUOTE_CHARACTER = '`';

    const RIGHT_QUOTE_CHARACTER = '`';

    public static function getNativeDateTimeFormat($field_info)
    {
        $type = DbSimpleTypes::TYPE_STRING;
        if (is_string($field_info)) {
            $type = $field_info;
        } elseif ($field_info instanceof ColumnSchema) {
            $type = $field_info->type;
        } elseif ($field_info instanceof ParameterSchema) {
            $type = $field_info->type;
        }
        switch (strtolower(strval($type))) {
            case DbSimpleTypes::TYPE_DATE:
                return 'Y-m-d';

            case DbSimpleTypes::TYPE_DATETIME:
            case DbSimpleTypes::TYPE_DATETIME_TZ:
                return 'Y-m-d H:i:s.u';

            case DbSimpleTypes::TYPE_TIME:
            case DbSimpleTypes::TYPE_TIME_TZ:
                return 'H:i:s.u';

            case DbSimpleTypes::TYPE_TIMESTAMP:
            case DbSimpleTypes::TYPE_TIMESTAMP_TZ:
            case DbSimpleTypes::TYPE_TIMESTAMP_ON_CREATE:
            case DbSimpleTypes::TYPE_TIMESTAMP_ON_UPDATE:
                return 'Y-m-d H:i:s.u'; // No support for timezone being passed in
        }

        return null;
    }

    /**
     * @inheritdoc
     */
    protected function translateSimpleColumnTypes(array &$info)
    {
        // override this in each schema class
        $type = (isset($info['type'])) ? $info['type'] : null;
        switch ($type) {
            // some types need massaging, some need other required properties
            case 'pk':
            case DbSimpleTypes::TYPE_ID:
                $info['type'] = 'int';
                $info['type_extras'] = '(11)';
                $info['allow_null'] = false;
                $info['auto_increment'] = true;
                $info['is_primary_key'] = true;
                break;

            case 'fk':
            case DbSimpleTypes::TYPE_REF:
                $info['type'] = 'int';
                $info['type_extras'] = '(11)';
                $info['is_foreign_key'] = true;
                // check foreign tables
                break;

            case DbSimpleTypes::TYPE_TIMESTAMP_ON_CREATE:
            case DbSimpleTypes::TYPE_TIMESTAMP_ON_UPDATE:
                $info['type'] = 'timestamp';
                $default = (isset($info['default'])) ? $info['default'] : null;
                if (!isset($default)) {
                    $default = 'CURRENT_TIMESTAMP';
                    if (DbSimpleTypes::TYPE_TIMESTAMP_ON_UPDATE === $type) {
                        $default .= ' ON UPDATE CURRENT_TIMESTAMP';
                    }
                    $info['default'] = ['expression' => $default];
                }
                break;

            case DbSimpleTypes::TYPE_USER_ID:
            case DbSimpleTypes::TYPE_USER_ID_ON_CREATE:
            case DbSimpleTypes::TYPE_USER_ID_ON_UPDATE:
                $info['type'] = 'int';
                $info['type_extras'] = '(11)';
                break;

            case DbSimpleTypes::TYPE_BOOLEAN:
                $info['type'] = 'tinyint';
                $info['type_extras'] = '(1)';
                $default = (isset($info['default'])) ? $info['default'] : null;
                if (isset($default)) {
                    // convert to bit 0 or 1, where necessary
                    $info['default'] = (int)filter_var($default, FILTER_VALIDATE_BOOLEAN);
                }
                break;

            case DbSimpleTypes::TYPE_MONEY:
                $info['type'] = 'decimal';
                $info['type_extras'] = '(19,4)';
                break;

            case DbSimpleTypes::TYPE_STRING:
                $fixed =
                    (isset($info['fixed_length'])) ? filter_var($info['fixed_length'], FILTER_VALIDATE_BOOLEAN) : false;
                $national =
                    (isset($info['supports_multibyte'])) ? filter_var($info['supports_multibyte'],
                        FILTER_VALIDATE_BOOLEAN) : false;
                if ($fixed) {
                    $info['type'] = ($national) ? 'nchar' : 'char';
                } elseif ($national) {
                    $info['type'] = 'nvarchar';
                } else {
                    $info['type'] = 'varchar';
                }
                break;

            case DbSimpleTypes::TYPE_BINARY:
                $fixed =
                    (isset($info['fixed_length'])) ? filter_var($info['fixed_length'], FILTER_VALIDATE_BOOLEAN) : false;
                $info['type'] = ($fixed) ? 'binary' : 'varbinary';
                break;
        }
    }

    protected function validateColumnSettings(array &$info)
    {
        // override this in each schema class
        $type = (isset($info['type'])) ? $info['type'] : null;
        switch ($type) {
            // some types need massaging, some need other required properties
            case 'bit':
            case 'tinyint':
            case 'smallint':
            case 'mediumint':
            case 'int':
            case 'bigint':
                if (!isset($info['type_extras'])) {
                    $length =
                        (isset($info['length']))
                            ? $info['length']
                            : ((isset($info['precision'])) ? $info['precision']
                            : null);
                    if (!empty($length)) {
                        $info['type_extras'] = "($length)"; // sets the viewable length
                    }
                }

                $default = (isset($info['default'])) ? $info['default'] : null;
                if (isset($default) && is_numeric($default)) {
                    $info['default'] = intval($default);
                }
                break;

            case 'decimal':
            case 'numeric':
            case 'real':
            case 'float':
            case 'double':
                if (!isset($info['type_extras'])) {
                    $length =
                        (isset($info['length']))
                            ? $info['length']
                            : ((isset($info['precision'])) ? $info['precision']
                            : null);
                    if (!empty($length)) {
                        $scale =
                            (isset($info['decimals']))
                                ? $info['decimals']
                                : ((isset($info['scale'])) ? $info['scale']
                                : null);
                        $info['type_extras'] = (!empty($scale)) ? "($length,$scale)" : "($length)";
                    }
                }

                $default = (isset($info['default'])) ? $info['default'] : null;
                if (isset($default) && is_numeric($default)) {
                    $info['default'] = floatval($default);
                }
                break;

            case 'char':
            case 'nchar':
            case 'binary':
                $length = (isset($info['length'])) ? $info['length'] : ((isset($info['size'])) ? $info['size'] : null);
                if (isset($length)) {
                    $info['type_extras'] = "($length)";
                }
                break;

            case 'varchar':
            case 'nvarchar':
            case 'varbinary':
                $length = (isset($info['length'])) ? $info['length'] : ((isset($info['size'])) ? $info['size'] : null);
                if (isset($length)) {
                    $info['type_extras'] = "($length)";
                } else // requires a max length
                {
                    $info['type_extras'] = '(' . static::DEFAULT_STRING_MAX_SIZE . ')';
                }
                break;

            case 'time':
            case 'timestamp':
            case 'datetime':
                $default = (isset($info['default'])) ? $info['default'] : null;
                if ('0000-00-00 00:00:00' == $default) {
                    // read back from MySQL has formatted zeros, can't send that back
                    $info['default'] = 0;
                }

                $length = (isset($info['length'])) ? $info['length'] : ((isset($info['size'])) ? $info['size'] : null);
                if (isset($length)) {
                    $info['type_extras'] = "($length)";
                }
                break;
        }
    }

    /**
     * @param array $info
     *
     * @return string
     * @throws \Exception
     */
    protected function buildColumnDefinition(array $info)
    {
        $type = (isset($info['type'])) ? $info['type'] : null;
        $typeExtras = (isset($info['type_extras'])) ? $info['type_extras'] : null;

        $definition = $type . $typeExtras;

        $allowNull = (isset($info['allow_null'])) ? filter_var($info['allow_null'], FILTER_VALIDATE_BOOLEAN) : false;
        $definition .= ($allowNull) ? ' NULL' : ' NOT NULL';

        $default = (isset($info['default'])) ? $info['default'] : null;
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

        $auto = (isset($info['auto_increment'])) ? filter_var($info['auto_increment'], FILTER_VALIDATE_BOOLEAN) : false;
        if ($auto) {
            $definition .= ' AUTO_INCREMENT';
        }

        if (isset($info['is_primary_key']) && filter_var($info['is_primary_key'], FILTER_VALIDATE_BOOLEAN)) {
            $definition .= ' PRIMARY KEY';
        } elseif (isset($info['is_unique']) && filter_var($info['is_unique'], FILTER_VALIDATE_BOOLEAN)) {
            $definition .= ' UNIQUE KEY';
        }

        return $definition;
    }

    /**
     * Compares two table names.
     * The table names can be either quoted or unquoted. This method
     * will consider both cases.
     *
     * @param string $name1 table name 1
     * @param string $name2 table name 2
     *
     * @return boolean whether the two table names refer to the same table.
     */
    public function compareTableNames($name1, $name2)
    {
        return parent::compareTableNames(strtolower($name1), strtolower($name2));
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
     *
     */
    public function resetSequence($table, $value = null)
    {
        if ($table->sequenceName === null) {
            return;
        }

        if ($value !== null) {
            $value = (int)$value;
        } else {
            $sql = 'SELECT MAX(`' . $table->primaryKey . '`) + 1 FROM ' . $table->quotedName;
            $value = (int)$this->selectValue($sql);
        }

        $sql = <<<MYSQL
ALTER TABLE {$table->quotedName} AUTO_INCREMENT = :value
MYSQL;
        $this->connection->statement($sql, [':value' => $value]);
    }

    /**
     * @return float server version.
     */
    protected function getServerVersion()
    {
        /** @noinspection PhpUndefinedMethodInspection */
        $version = $this->connection->getAttribute(\PDO::ATTR_SERVER_VERSION);
        $digits = [];
        preg_match('/(\d+)\.(\d+)\.(\d+)/', $version, $digits);

        return floatval($digits[1] . '.' . $digits[2] . $digits[3]);
    }

    /**
     * @inheritdoc
     */
    protected function loadTableColumns(TableSchema $table)
    {
        $sql = 'SHOW FULL COLUMNS FROM ' . $table->quotedName;

        $result = $this->connection->select($sql);
        foreach ($result as $column) {
            $column = array_change_key_case((array)$column, CASE_LOWER);
            $c = new ColumnSchema(['name' => $column['field']]);
            $c->quotedName = $this->quoteColumnName($c->name);
            $c->allowNull = $column['null'] === 'YES';
            $c->isPrimaryKey = strpos($column['key'], 'PRI') !== false;
            $c->isUnique = strpos($column['key'], 'UNI') !== false;
            $c->isIndex = strpos($column['key'], 'MUL') !== false;
            $c->autoIncrement = strpos(strtolower($column['extra']), 'auto_increment') !== false;
            $c->dbType = $column['type'];
            if (isset($column['collation']) && !empty($column['collation'])) {
                $collation = $column['collation'];
                if (0 === stripos($collation, 'utf') || 0 === stripos($collation, 'ucs')) {
                    $c->supportsMultibyte = true;
                }
            }
            if (isset($column['comment'])) {
                $c->comment = $column['comment'];
            }
            $this->extractLimit($c, $c->dbType);
            $c->fixedLength = $this->extractFixedLength($c->dbType);
            $this->extractType($c, $c->dbType);

            if ($c->dbType === 'timestamp' && (0 === strcasecmp(strval($column['default']), 'CURRENT_TIMESTAMP'))) {
                if (0 === strcasecmp(strval($column['extra']), 'on update CURRENT_TIMESTAMP')) {
                    $c->defaultValue = ['expression' => 'CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP'];
                    $c->type = DbSimpleTypes::TYPE_TIMESTAMP_ON_UPDATE;
                } else {
                    $c->defaultValue = ['expression' => 'CURRENT_TIMESTAMP'];
                    $c->type = DbSimpleTypes::TYPE_TIMESTAMP_ON_CREATE;
                }
            } else {
                $this->extractDefault($c, $column['default']);
            }

            if ($c->isPrimaryKey) {
                if ($c->autoIncrement) {
                    $table->sequenceName = array_get($column, 'sequence', $c->name);
                    if ((DbSimpleTypes::TYPE_INTEGER === $c->type)) {
                        $c->type = DbSimpleTypes::TYPE_ID;
                    }
                }
                $table->addPrimaryKey($c->name);
            }
            $table->addColumn($c);
        }
    }

    /**
     * @inheritdoc
     */
    protected function getTableConstraints($schema = '')
    {
        if (is_array($schema)) {
            $schema = implode("','", $schema);
        }

        $sql = <<<SQL
SELECT tc.constraint_type, tc.constraint_schema, tc.constraint_name, tc.table_schema, tc.table_name, 
kcu.column_name, kcu.referenced_table_schema, kcu.referenced_table_name, kcu.referenced_column_name,
rc.update_rule, rc.delete_rule
FROM information_schema.TABLE_CONSTRAINTS tc
JOIN information_schema.KEY_COLUMN_USAGE kcu ON tc.constraint_schema = kcu.constraint_schema AND 
tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema AND tc.table_name = kcu.table_name 
LEFT JOIN information_schema.REFERENTIAL_CONSTRAINTS rc ON tc.constraint_schema = rc.constraint_schema AND 
tc.constraint_name = rc.constraint_name AND tc.table_name = rc.table_name
WHERE tc.constraint_schema IN ('{$schema}');
SQL;

        $results = $this->connection->select($sql);
        $constraints = [];
        foreach ($results as $row) {
            $row = array_change_key_case((array)$row, CASE_LOWER);
            $ts = strtolower($row['table_schema']);
            $tn = strtolower($row['table_name']);
            $cn = strtolower($row['constraint_name']);
            $colName = array_get($row, 'column_name');
            $refColName = array_get($row, 'referenced_column_name');
            if (isset($constraints[$ts][$tn][$cn])) {
                $constraints[$ts][$tn][$cn]['column_name'] =
                    array_merge((array)$constraints[$ts][$tn][$cn]['column_name'], (array)$colName);

                if (isset($refColName)) {
                    $constraints[$ts][$tn][$cn]['referenced_column_name'] =
                        array_merge((array)$constraints[$ts][$tn][$cn]['referenced_column_name'], (array)$refColName);
                }
            } else {
                $constraints[$ts][$tn][$cn] = $row;
            }
        }

        return $constraints;
    }

    /**
     * @inheritdoc
     */
    public function getSchemas()
    {
        $sql = <<<MYSQL
SHOW DATABASES WHERE `Database` NOT IN ('information_schema','mysql','performance_schema','phpmyadmin')
MYSQL;

        return $this->selectColumn($sql);
    }

    /**
     * @inheritdoc
     */
    protected function getTableNames($schema = '')
    {
        $sql = 'SHOW FULL TABLES';

        if (!empty($schema)) {
            $sql .= ' FROM ' . $this->quoteTableName($schema);
        }

        $sql .= " WHERE TABLE_TYPE = 'BASE TABLE'";

        $rows = $this->connection->select($sql);

        $names = [];
        foreach ($rows as $row) {
            $row = array_values((array)$row);
            $schemaName = $schema;
            $resourceName = $row[0];
            $internalName = $schemaName . '.' . $resourceName;
            $name = $resourceName;
            $quotedName = $this->quoteTableName($schemaName) . '.' . $this->quoteTableName($resourceName);;
            $settings = compact('schemaName', 'resourceName', 'name', 'internalName', 'quotedName');
            $names[strtolower($name)] = new TableSchema($settings);
        }

        return $names;
    }

    /**
     * @inheritdoc
     */
    protected function getViewNames($schema = '')
    {
        $sql = 'SHOW FULL TABLES';

        if (!empty($schema)) {
            $sql .= ' FROM ' . $this->quoteTableName($schema);
        }

        $sql .= " WHERE TABLE_TYPE = 'VIEW'";

        $rows = $this->connection->select($sql);

        $names = [];
        foreach ($rows as $row) {
            $row = array_values((array)$row);
            $schemaName = $schema;
            $resourceName = $row[0];
            $internalName = $schemaName . '.' . $resourceName;
            $name = $resourceName;
            $quotedName = $this->quoteTableName($schemaName) . '.' . $this->quoteTableName($resourceName);
            $settings = compact('schemaName', 'resourceName', 'name', 'internalName', 'quotedName');
            $settings['isView'] = true;
            $names[strtolower($name)] = new TableSchema($settings);
        }

        return $names;
    }

    protected function getRoutineNames($type, $schema = '')
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

        $names = [];
        foreach ($rows as $row) {
            $row = array_change_key_case((array)$row, CASE_UPPER);
            $resourceName = array_get($row, 'ROUTINE_NAME');
            $schemaName = $schema;
            $internalName = $schemaName . '.' . $resourceName;
            $name = $resourceName;
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

    protected function loadParameters(RoutineSchema $holder)
    {
        $sql = <<<MYSQL
SELECT p.ORDINAL_POSITION, p.PARAMETER_MODE, p.PARAMETER_NAME, p.DATA_TYPE, p.CHARACTER_MAXIMUM_LENGTH, 
p.NUMERIC_PRECISION, p.NUMERIC_SCALE
FROM INFORMATION_SCHEMA.PARAMETERS AS p 
JOIN INFORMATION_SCHEMA.ROUTINES AS r ON r.SPECIFIC_NAME = p.SPECIFIC_NAME
WHERE r.ROUTINE_NAME = '{$holder->resourceName}' AND r.ROUTINE_SCHEMA = '{$holder->schemaName}'
MYSQL;

        $params = $this->connection->select($sql);
        foreach ($params as $row) {
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
     * Builds a SQL statement for renaming a column.
     *
     * @param string $table   the table whose column is to be renamed. The name will be properly quoted by the method.
     * @param string $name    the old name of the column. The name will be properly quoted by the method.
     * @param string $newName the new name of the column. The name will be properly quoted by the method.
     *
     * @throws \Exception if specified column is not found in given table
     * @return string the SQL statement for renaming a DB column.
     */
    public function renameColumn($table, $name, $newName)
    {
        $db = $this->connection;
        if (null === $row = $db->selectOne('SHOW CREATE TABLE ' . $this->quoteTableName($table))) {
            throw new \Exception("Unable to find '$name' in table '$table'.");
        }

        if (isset($row['Create Table'])) {
            $sql = $row['Create Table'];
        } else {
            $row = array_values($row);
            $sql = $row[1];
        }

        $table = $this->quoteTableName($table);
        $name = $this->quoteColumnName($name);
        $newName = $this->quoteColumnName($newName);

        if (preg_match_all('/^\s*[`"](.*?)[`"]\s+(.*?),?$/m', $sql, $matches)) {
            foreach ($matches[1] as $i => $c) {
                if ($c === $name) {
                    return <<<MYSQL
ALTER TABLE {$table} CHANGE {$name} $newName {$matches[2][$i]}
MYSQL;
                }
            }
        }

        return <<<MYSQL
ALTER TABLE {$table} CHANGE {$name} $newName
MYSQL;
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
        /** @noinspection SqlNoDataSourceInspection */
        return 'ALTER TABLE ' . $this->quoteTableName($table) . ' DROP FOREIGN KEY ' . $this->quoteColumnName($name);
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
        /** @noinspection SqlNoDataSourceInspection */
        return 'ALTER TABLE ' . $this->quoteTableName($table) . ' DROP PRIMARY KEY';
    }

    /**
     * Builds a SQL statement for adding a primary key constraint to a table.
     *
     * @param string       $name    not used in the MySQL syntax, the primary key is always called PRIMARY and is
     *                              reserved.
     * @param string       $table   the table that the primary key constraint will be added to.
     * @param string|array $columns comma separated string or array of columns that the primary key will consist of.
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

        /** @noinspection SqlNoDataSourceInspection */
        return 'ALTER TABLE ' . $this->quoteTableName($table) . ' ADD PRIMARY KEY (' . implode(', ', $columns) . ' )';
    }

    /**
     * @return string default schema.
     */
    public function getDefaultSchema()
    {
        $sql = <<<MYSQL
SELECT DATABASE() FROM DUAL
MYSQL;

        return $this->selectValue($sql);
    }

    /**
     * Extracts the default value for the column.
     * The value is typecasted to correct PHP type.
     *
     * @param ColumnSchema $field
     * @param mixed        $defaultValue the default value obtained from metadata
     */
    public function extractDefault(ColumnSchema $field, $defaultValue)
    {
        if (strncmp($field->dbType, 'bit', 3) === 0) {
            $field->defaultValue = bindec(trim($defaultValue, 'b\''));
        } else {
            parent::extractDefault($field, $defaultValue);
        }
    }

    /**
     * Extracts size, precision and scale information from column's DB type.
     *
     * @param ColumnSchema $field
     * @param string       $dbType the column's DB type
     */
    public function extractLimit(ColumnSchema $field, $dbType)
    {
        if (strncmp($dbType, 'enum', 4) === 0 && preg_match('/\(([\'"])(.*)\\1\)/', $dbType, $matches)) {
            // explode by (single or double) quote and comma (ENUM values may contain commas)
            $values = explode($matches[1] . ',' . $matches[1], $matches[2]);
            $size = 0;
            foreach ($values as $value) {
                if (($n = strlen($value)) > $size) {
                    $size = $n;
                }
            }
            $field->size = $field->precision = $size;
        } else {
            parent::extractLimit($field, $dbType);
        }
    }

    /**
     * @inheritdoc
     */
    protected function getProcedureStatement(RoutineSchema $routine, array $param_schemas, array &$values)
    {
        $paramStr = '';
        $pre = '';
        foreach ($param_schemas as $key => $paramSchema) {
            switch ($paramSchema->paramType) {
                case 'IN':
                    $pName = ':' . $paramSchema->name;
                    $paramStr .= (empty($paramStr)) ? $pName : ", $pName";
                    break;
                case 'INOUT':
                    $pName = '@' . $paramSchema->name;
                    $paramStr .= (empty($paramStr)) ? $pName : ", $pName";
                    // not using binding for out or inout params here due to earlier (<5.5.3) mysql library bug
                    // since binding isn't working, set the values via statements, get the values via select
                    if (is_null($value = array_get($values, $key))) {
                        $value = 'NULL';
                    }
                    $pre .= "SET $pName = $value;";
                    break;
                case 'OUT':
                    $pName = '@' . $paramSchema->name;
                    $paramStr .= (empty($paramStr)) ? $pName : ", $pName";
                    // not using binding for out or inout params here due to earlier (<5.5.3) mysql library bug
                    // since binding isn't working, get the values via select
                    break;
                default:
                    break;
            }
        }

        !empty($pre) && $this->connection->statement($pre);

        return "CALL {$routine->quotedName}($paramStr)";
    }

    protected function postProcedureCall(array $param_schemas, array &$values)
    {
        $post = '';
        foreach ($param_schemas as $key => $paramSchema) {
            switch ($paramSchema->paramType) {
                case 'INOUT':
                case 'OUT':
                    // not using binding for out or inout params here due to earlier (<5.5.3) mysql library bug
                    // since binding isn't working, get the values via select
                    $pName = '@' . $paramSchema->name;
                    $post .= (empty($post)) ? $pName : ", $pName";
                    break;
                default:
                    break;
            }
        }
        if (!empty($post)) {
            // must query to get output parameters back
            if (null !== $out = $this->connection->selectOne("SELECT $post;")) {
                $out = (array)$out;
                foreach ($param_schemas as $key => $paramSchema) {
                    switch ($paramSchema->paramType) {
                        case 'INOUT':
                        case 'OUT':
                            $values[$key] = array_get($out, '@' . $paramSchema->name);
                            break;
                    }
                }
            }
        }
    }

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
                    // not using binding for out or inout params here due to earlier (<5.5.3) mysql library bug
                    // since binding isn't working, set the values via statements, get the values via select
                    break;
            }
        }
    }

    protected function handleRoutineException(\Exception $ex)
    {
        // catch 2053 https://dev.mysql.com/doc/refman/5.7/en/error-messages-client.html#error_cr_no_result_set
        // this may happen as we try to get as many result sets from routine calls as possible
        if (false !== stripos($ex->getMessage(), 'SQLSTATE[HY000]: General error: 2053')) {
            return true;
        }

        return false;
    }
}
