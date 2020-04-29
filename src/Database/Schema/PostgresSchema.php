<?php

namespace DreamFactory\Core\SqlDb\Database\Schema;

use DreamFactory\Core\Database\Schema\ColumnSchema;
use DreamFactory\Core\Database\Schema\FunctionSchema;
use DreamFactory\Core\Database\Schema\ParameterSchema;
use DreamFactory\Core\Database\Schema\ProcedureSchema;
use DreamFactory\Core\Database\Schema\RoutineSchema;
use DreamFactory\Core\Database\Schema\TableSchema;
use DreamFactory\Core\Enums\DbSimpleTypes;

/**
 * Schema is the class for retrieving metadata information from a PostgreSQL database.
 */
class PostgresSchema extends SqlSchema
{
    const DEFAULT_SCHEMA = 'public';

    /**
     * @inheritdoc
     */
    public function getDefaultSchema()
    {
        return static::DEFAULT_SCHEMA;
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
                $info['type'] = 'serial';
                $info['allow_null'] = false;
                $info['auto_increment'] = true;
                $info['is_primary_key'] = true;
                break;

            case 'fk':
            case DbSimpleTypes::TYPE_REF:
                $info['type'] = 'integer';
                $info['is_foreign_key'] = true;
                // check foreign tables
                break;

            case DbSimpleTypes::TYPE_DATETIME:
            case DbSimpleTypes::TYPE_TIMESTAMP:
                $info['type'] = 'timestamp';
                break;

            case DbSimpleTypes::TYPE_DATETIME_TZ:
            case DbSimpleTypes::TYPE_TIMESTAMP_TZ:
                $info['type'] = 'timestamp with time zone';
                break;

            case DbSimpleTypes::TYPE_TIME:
                $info['type'] = 'time';
                break;

            case DbSimpleTypes::TYPE_TIME_TZ:
                $info['type'] = 'time with time zone';
                break;

            case DbSimpleTypes::TYPE_TIMESTAMP_ON_CREATE:
            case DbSimpleTypes::TYPE_TIMESTAMP_ON_UPDATE:
                $info['type'] = 'timestamp';
                $default = (isset($info['default'])) ? $info['default'] : null;
                if (!isset($default)) {
                    $default = 'CURRENT_TIMESTAMP';
                    // ON UPDATE CURRENT_TIMESTAMP not supported by PostgreSQL, use triggers
                    $info['default'] = $default;
                }
                break;

            case DbSimpleTypes::TYPE_USER_ID:
            case DbSimpleTypes::TYPE_USER_ID_ON_CREATE:
            case DbSimpleTypes::TYPE_USER_ID_ON_UPDATE:
                $info['type'] = 'integer';
                break;

            case 'int':
                $info['type'] = 'integer';
                break;

            case DbSimpleTypes::TYPE_FLOAT:
                $info['type'] = 'real';
                break;

            case DbSimpleTypes::TYPE_DOUBLE:
                $info['type'] = 'double precision';
                break;

            case DbSimpleTypes::TYPE_STRING:
                $fixed =
                    (isset($info['fixed_length'])) ? filter_var($info['fixed_length'], FILTER_VALIDATE_BOOLEAN) : false;
                $national =
                    (isset($info['supports_multibyte'])) ? filter_var($info['supports_multibyte'],
                        FILTER_VALIDATE_BOOLEAN) : false;
                if ($fixed) {
                    $info['type'] = ($national) ? 'national char' : 'char';
                } elseif ($national) {
                    $info['type'] = 'national varchar';
                } else {
                    $info['type'] = 'varchar';
                }
                break;

            case DbSimpleTypes::TYPE_BINARY:
                $info['type'] = 'bytea';
                break;
        }
    }

    protected function validateColumnSettings(array &$info)
    {
        // override this in each schema class
        $type = (isset($info['type'])) ? $info['type'] : null;
        switch ($type) {
            // some types need massaging, some need other required properties
            case 'boolean':
                $default = (isset($info['default'])) ? $info['default'] : null;
                if (isset($default)) {
                    // convert to bit 0 or 1, where necessary
                    $info['default'] = (filter_var($default, FILTER_VALIDATE_BOOLEAN)) ? 'TRUE' : 'FALSE';
                }
                break;

            case 'smallint':
            case 'integer':
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
            case 'double precision':
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
            case 'national char':
                $length = (isset($info['length'])) ? $info['length'] : ((isset($info['size'])) ? $info['size'] : null);
                if (isset($length)) {
                    $info['type_extras'] = "($length)";
                }
                break;

            case 'varchar':
            case 'national varchar':
                $length = (isset($info['length'])) ? $info['length'] : ((isset($info['size'])) ? $info['size'] : null);
                if (isset($length)) {
                    $info['type_extras'] = "($length)";
                } else // requires a max length
                {
                    $info['type_extras'] = '(' . static::DEFAULT_STRING_MAX_SIZE . ')';
                }
                break;

            case 'time':
            case 'time with time zone':
            case 'timestamp':
            case 'timestamp with time zone':
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

        if ('time with time zone' === $type) {
            $definition = 'time' . $typeExtras . ' with time zone';
        } elseif ('timestamp with time zone' === $type) {
            $definition = 'timestamp' . $typeExtras . ' with time zone';
        } else {
            $definition = $type . $typeExtras;
        }

        $allowNull = (isset($info['allow_null'])) ? filter_var($info['allow_null'], FILTER_VALIDATE_BOOLEAN) : false;
        $definition .= ($allowNull) ? ' NULL' : ' NOT NULL';

        $default = (isset($info['default'])) ? $info['default'] : null;
        if (isset($default)) {
            $quoteDefault =
                (isset($info['quote_default'])) ? filter_var($info['quote_default'], FILTER_VALIDATE_BOOLEAN) : false;
            if ($quoteDefault) {
                $default = "'" . $default . "'";
            }

            $definition .= ' DEFAULT ' . $default;
        }

        if (isset($info['is_primary_key']) && filter_var($info['is_primary_key'], FILTER_VALIDATE_BOOLEAN)) {
            $definition .= ' PRIMARY KEY';
        } elseif (isset($info['is_unique']) && filter_var($info['is_unique'], FILTER_VALIDATE_BOOLEAN)) {
            $definition .= ' UNIQUE';
        }

        return $definition;
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
        $sequence = '"' . $table->sequenceName . '"';
        if (strpos($sequence, '.') !== false) {
            $sequence = str_replace('.', '"."', $sequence);
        }
        if ($value !== null) {
            $value = (int)$value;
        } else {
            $value = "(SELECT COALESCE(MAX(\"{$table->primaryKey}\"),0) FROM {$table->quotedName})+1";
        }
        $this->connection->statement("SELECT SETVAL('$sequence',$value,false)");
    }

    /**
     * @inheritdoc
     */
    protected function loadTableColumns(TableSchema $table)
    {
        $params = [':table' => $table->resourceName, ':schema' => $table->schemaName];
        $version = $this->connection->select('select version();')[0]->version;
        $adsrc = strpos($version, 'PostgreSQL 12') !== false ? 'pg_get_expr(d.adbin, d.adrelid) AS adsrc' : 'd.adsrc';
        $sql = <<<SQL
SELECT a.attname, LOWER(format_type(a.atttypid, a.atttypmod)) AS type, $adsrc, a.attnotnull, a.atthasdef,
	pg_catalog.col_description(a.attrelid, a.attnum) AS comment
FROM pg_attribute a LEFT JOIN pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum
WHERE a.attnum > 0 AND NOT a.attisdropped
	AND a.attrelid = (SELECT oid FROM pg_catalog.pg_class WHERE relname=:table
		AND relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = :schema))
ORDER BY a.attnum
SQL;
        $result = $this->connection->select($sql, $params);
            foreach ($result as $column) {
                $column = array_change_key_case((array)$column, CASE_LOWER);

                if (stripos($column['adsrc'], 'nextval') === 0 &&
                    preg_match('/nextval\([^\']*\'([^\']+)\'[^\)]*\)/i', $column['adsrc'], $matches)
                ) {
                    if (strpos($matches[1], '.') !== false || $table->schemaName === self::DEFAULT_SCHEMA) {
                        $column['sequence'] = $matches[1];
                    } else {
                        $column['sequence'] = $table->schemaName . '.' . $matches[1];
                    }
                    $column['auto_increment'] = true;
                }

                $c = new ColumnSchema(['name' => $column['attname']]);
                $c->quotedName = $this->quoteColumnName($c->name);
                $c->autoIncrement = array_get($column, 'auto_increment', false);
                $c->isPrimaryKey = array_get($column, 'is_primary_key', false);
                $c->allowNull = !$column['attnotnull'];
                $c->comment = $column['comment'] === null ? '' : $column['comment'];
                $c->dbType = $column['type'];
                $this->extractLimit($c, $column['type']);
                $c->fixedLength = $this->extractFixedLength($column['type']);
                $c->supportsMultibyte = $this->extractMultiByteSupport($column['type']);
                $this->extractType($c, $column['type']);
                $this->extractDefault($c, $column['atthasdef'] ? $column['adsrc'] : null);

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
SELECT tc.constraint_type, tc.constraint_schema, tc.constraint_name, tc.constraint_type, tc.table_schema, tc.table_name, kcu.column_name, 
kcu2.table_schema as referenced_table_schema, kcu2.table_name as referenced_table_name, kcu2.column_name as referenced_column_name, 
rc.update_rule, rc.delete_rule
FROM information_schema.TABLE_CONSTRAINTS tc
JOIN information_schema.KEY_COLUMN_USAGE kcu ON tc.constraint_schema = kcu.constraint_schema AND tc.constraint_name = kcu.constraint_name AND tc.table_name = kcu.table_name
LEFT JOIN information_schema.REFERENTIAL_CONSTRAINTS rc ON tc.constraint_schema = rc.constraint_schema AND tc.constraint_name = rc.constraint_name
LEFT JOIN information_schema.KEY_COLUMN_USAGE kcu2 ON rc.unique_constraint_schema = kcu2.constraint_schema AND rc.unique_constraint_name = kcu2.constraint_name
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

    public function getSchemas()
    {
        $sql = <<<MYSQL
SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema','pg_catalog')
MYSQL;
        $rows = $this->selectColumn($sql);

        return $rows;
    }

    /**
     * @inheritdoc
     */
    protected function getTableNames($schema = '')
    {
        $sql = <<<EOD
SELECT table_name, table_schema FROM information_schema.tables WHERE table_type = 'BASE TABLE'
EOD;

        if (!empty($schema)) {
            $sql .= " AND table_schema = '$schema'";
        }

        $defaultSchema = self::DEFAULT_SCHEMA;
        $addSchema = (!empty($schema) && ($defaultSchema !== $schema));

        $rows = $this->connection->select($sql);

        $names = [];
        foreach ($rows as $row) {
            $row = (array)$row;
            $schemaName = isset($row['table_schema']) ? $row['table_schema'] : '';
            $resourceName = isset($row['table_name']) ? $row['table_name'] : '';
            $internalName = $schemaName . '.' . $resourceName;
            $name = ($addSchema) ? $internalName : $resourceName;
            $quotedName = $this->quoteTableName($schemaName) . '.' . $this->quoteTableName($resourceName);
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
        $sql = <<<EOD
SELECT all_views.table_name, all_views.table_schema
  FROM (
       SELECT table_name AS table_name,
              table_schema AS table_schema,
              table_type AS table_type
         FROM information_schema.tables
       UNION ALL
       SELECT matviewname AS table_name,
              schemaname AS table_schema,
              'VIEW' AS table_type
         FROM pg_matviews
           ) AS all_views
 WHERE all_views.table_type = 'VIEW'
EOD;

        if (!empty($schema)) {
            $sql .= " AND table_schema = '$schema'";
        }

        $defaultSchema = self::DEFAULT_SCHEMA;
        $addSchema = (!empty($schema) && ($defaultSchema !== $schema));

        $rows = $this->connection->select($sql);

        $names = [];
        foreach ($rows as $row) {
            $row = (array)$row;
            $schemaName = isset($row['table_schema']) ? $row['table_schema'] : '';
            $resourceName = isset($row['table_name']) ? $row['table_name'] : '';
            $internalName = $schemaName . '.' . $resourceName;
            $name = ($addSchema) ? $internalName : $resourceName;
            $quotedName = $this->quoteTableName($schemaName) . '.' . $this->quoteTableName($resourceName);
            $settings = compact('schemaName', 'resourceName', 'name', 'internalName', 'quotedName');
            $settings['isView'] = true;
            $names[strtolower($name)] = new TableSchema($settings);
        }

        return $names;
    }

    /**
     * @inheritdoc
     */
    public function renameTable($table, $newName)
    {
        return 'ALTER TABLE ' . $this->quoteTableName($table) . ' RENAME TO ' . $this->quoteTableName($newName);
    }

    /**
     * @inheritdoc
     */
    public function alterColumn($table, $column, $definition)
    {
        $sql = "ALTER TABLE $table ALTER COLUMN " . $this->quoteColumnName($column);
        $definition = $this->getColumnType($definition);
        if (false !== $pos = strpos($definition, ' ')) {
            $sql .= ' TYPE ' . substr($definition, 0, $pos);
            switch (substr($definition, $pos + 1)) {
                case 'NULL':
                    $sql .= ', ALTER COLUMN ' . $this->quoteColumnName($column) . ' DROP NOT NULL';
                    break;
                case 'NOT NULL':
                    $sql .= ', ALTER COLUMN ' . $this->quoteColumnName($column) . ' SET NOT NULL';
                    break;
            }
        } else {
            $sql .= ' TYPE ' . $definition;
        }

        return $sql;
    }

    /**
     * Builds a SQL statement for creating a new index.
     *
     * @param string  $name    the name of the index. The name will be properly quoted by the method.
     * @param string  $table   the table that the new index will be created for. The table name will be properly quoted
     *                         by the method.
     * @param string  $columns the column(s) that should be included in the index. If there are multiple columns,
     *                         please separate them by commas. Each column name will be properly quoted by the method,
     *                         unless a parenthesis is found in the name.
     * @param boolean $unique  whether to add UNIQUE constraint on the created index.
     *
     * @return string the SQL statement for creating a new index.
     * @since 1.1.6
     */
    public function createIndex($name, $table, $columns, $unique = false)
    {
        $cols = [];
        if (is_string($columns)) {
            $columns = preg_split('/\s*,\s*/', $columns, -1, PREG_SPLIT_NO_EMPTY);
        }
        foreach ($columns as $col) {
            if (strpos($col, '(') !== false) {
                $cols[] = $col;
            } else {
                $cols[] = $this->quoteColumnName($col);
            }
        }
        if ($unique) {
            return
                'ALTER TABLE ONLY ' .
                $this->quoteTableName($table) .
                ' ADD CONSTRAINT ' .
                $this->quoteTableName($name) .
                ' UNIQUE (' .
                implode(', ', $cols) .
                ')';
        } else {
            return
                'CREATE INDEX ' .
                $this->quoteTableName($name) .
                ' ON ' .
                $this->quoteTableName($table) .
                ' (' .
                implode(', ', $cols) .
                ')';
        }
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
        return 'DROP INDEX ' . $this->quoteTableName($name);
    }

    public function typecastToNative($value, $field_info, $allow_null = true)
    {
        switch ($field_info->type) {
            case DbSimpleTypes::TYPE_BOOLEAN:
                if (!(is_null($value) && $field_info->allowNull)) {
                    $value = (to_bool($value) ? 't' : 'f');
                }
                break;
            default:
                $value = parent::typecastToNative($value, $field_info, $allow_null);
                break;
        }

        return $value;
    }

    protected function formatValueToPhpType($value, $type, $allow_null = true)
    {
        if (!is_null($value)) {
            switch (strtolower(strval($type))) {
                case 'int':
                case 'integer':
                    if ('' === $value) {
                        // Postgresql strangely returns "" for null integers
                        return null;
                    }
            }
        }

        return parent::formatValueToPhpType($value, $type, $allow_null);
    }

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

            case DbSimpleTypes::TYPE_TIME:
                return 'H:i:s.u';
            case DbSimpleTypes::TYPE_TIME_TZ:
                return 'H:i:s.uP';

            case DbSimpleTypes::TYPE_DATETIME:
            case DbSimpleTypes::TYPE_TIMESTAMP:
            case DbSimpleTypes::TYPE_TIMESTAMP_ON_CREATE:
            case DbSimpleTypes::TYPE_TIMESTAMP_ON_UPDATE:
                return 'Y-m-d H:i:s.u';

            case DbSimpleTypes::TYPE_DATETIME_TZ:
            case DbSimpleTypes::TYPE_TIMESTAMP_TZ:
                return 'Y-m-d H:i:s.uP';
        }

        return null;
    }

    /**
     * @inheritdoc
     */
    public function extractType(ColumnSchema $column, $dbType)
    {
        parent::extractType($column, $dbType);
        if (strpos($dbType, '[') !== false || strpos($dbType, 'char') !== false || strpos($dbType, 'text') !== false) {
            $column->type = DbSimpleTypes::TYPE_STRING;
        } elseif (preg_match('/(real|float|double)/', $dbType)) {
            $column->type = DbSimpleTypes::TYPE_DOUBLE;
        } elseif (preg_match('/(integer|oid|serial|smallint)/', $dbType)) {
            $column->type = DbSimpleTypes::TYPE_INTEGER;
        } elseif (false !== strpos($dbType, ' with time zone')) {
            switch ($column->type) {
                case DbSimpleTypes::TYPE_TIME:
                    $column->type = DbSimpleTypes::TYPE_TIME_TZ;
                    break;
                case DbSimpleTypes::TYPE_TIMESTAMP:
                    $column->type = DbSimpleTypes::TYPE_TIMESTAMP_TZ;
                    break;

            }
        }
    }

    /**
     * Extracts the PHP type from DF type.
     *
     * @param string $type DF type
     *
     * @return string
     */
    public static function extractPhpType($type)
    {
        switch ($type) {
            case DbSimpleTypes::TYPE_MONEY:
                return 'string';
        }

        return parent::extractPhpType($type);
    }

    /**
     * Extracts size, precision and scale information from column's DB type.
     *
     * @param ColumnSchema $field
     * @param string       $dbType the column's DB type
     */
    public function extractLimit(ColumnSchema $field, $dbType)
    {
        if (strpos($dbType, '(')) {
            if (preg_match('/^time.*\((.*)\)/', $dbType, $matches)) {
                $field->precision = (int)$matches[1];
            } elseif (preg_match('/\((.*)\)/', $dbType, $matches)) {
                $values = explode(',', $matches[1]);
                $field->size = $field->precision = (int)$values[0];
                if (isset($values[1])) {
                    $field->scale = (int)$values[1];
                }
            }
        }
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
        if ($defaultValue === 'true') {
            $field->defaultValue = true;
        } elseif ($defaultValue === 'false') {
            $field->defaultValue = false;
        } elseif (strpos($defaultValue, 'nextval') === 0) {
            $field->defaultValue = null;
        } elseif (preg_match('/^\'(.*)\'::/', $defaultValue, $matches)) {
            parent::extractDefault($field, str_replace("''", "'", $matches[1]));
        } elseif (preg_match('/^(-?\d+(\.\d*)?)(::.*)?$/', $defaultValue, $matches)) {
            parent::extractDefault($field, $matches[1]);
        } else {
            // could be a internal function call like setting uuids
            $field->defaultValue = $defaultValue;
        }
    }

    /**
     * @inheritdoc
     */
    protected function getRoutineNames($type, $schema = '')
    {
        $bindings = [];
        $where = '';
        if (!empty($schema)) {
            $where .= 'WHERE ROUTINE_SCHEMA = :schema';
            $bindings[':schema'] = $schema;
        }

        $sql = <<<MYSQL
SELECT ROUTINE_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.ROUTINES {$where}
MYSQL;

        $rows = $this->connection->select($sql, $bindings);

        $sql = <<<MYSQL
SELECT r.ROUTINE_NAME
FROM INFORMATION_SCHEMA.PARAMETERS AS p JOIN INFORMATION_SCHEMA.ROUTINES AS r ON r.SPECIFIC_NAME = p.SPECIFIC_NAME 
WHERE p.SPECIFIC_SCHEMA = :schema AND (p.PARAMETER_MODE = 'INOUT' OR p.PARAMETER_MODE = 'OUT')
MYSQL;

        $procedures = $this->selectColumn($sql, $bindings);

        $names = [];
        foreach ($rows as $row) {
            $row = array_change_key_case((array)$row, CASE_UPPER);
            $resourceName = array_get($row, 'ROUTINE_NAME');
            switch (strtoupper($type)) {
                case 'PROCEDURE':
                    if (false === array_search($resourceName, $procedures)) {
                        // only way to determine proc from func is by params??
                        continue 2;
                    }
                    break;
                case 'FUNCTION':
                    if (false !== array_search($resourceName, $procedures)) {
                        // only way to determine proc from func is by params??
                        continue 2;
                    }
                    break;
            }
            $schemaName = $schema;
            $internalName = $schemaName . '.' . $resourceName;
            $name = $resourceName;
            $quotedName = $this->quoteTableName($schemaName) . '.' . $this->quoteTableName($resourceName);
            $returnType = array_get($row, 'DATA_TYPE');
            if (!empty($returnType) && (0 !== strcasecmp('void', $returnType))) {
                $returnType = static::extractSimpleType($returnType);
            }
            $settings = compact('schemaName', 'resourceName', 'name', 'quotedName', 'internalName', 'returnType');
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

    protected function doRoutineBinding($statement, array $paramSchemas, array &$values)
    {
        // do binding
        foreach ($paramSchemas as $key => $paramSchema) {
            switch ($paramSchema->paramType) {
                case 'IN':
                case 'INOUT':
                    $this->bindValue($statement, ':' . $paramSchema->name, array_get($values, $key));
                    break;
                case 'OUT':
                    // not sent as parameters, but pulled from fetch results
                    break;
            }
        }
    }

    /**
     * @inheritdoc
     */
    protected function getRoutineParamString(array $param_schemas, array &$values)
    {
        $paramStr = '';
        foreach ($param_schemas as $key => $paramSchema) {
            switch ($paramSchema->paramType) {
                case 'IN':
                case 'INOUT':
                    $pName = ':' . $paramSchema->name;
                    $paramStr .= (empty($paramStr)) ? $pName : ", $pName";
                    break;
                case 'OUT':
                    // not sent as parameters, but pulled from fetch results
                    break;
                default:
                    break;
            }
        }

        return $paramStr;
    }

    protected function getProcedureStatement(RoutineSchema $routine, array $param_schemas, array &$values)
    {
        $paramStr = $this->getRoutineParamString($param_schemas, $values);

        return "SELECT * FROM {$routine->quotedName}($paramStr);";
    }

    /**
     * @inheritdoc
     */
    protected function getFunctionStatement(RoutineSchema $routine, array $param_schemas, array &$values)
    {
        $paramStr = $this->getRoutineParamString($param_schemas, $values);

        return "SELECT * FROM {$routine->quotedName}($paramStr)";
    }

    protected function handleRoutineException(\Exception $ex)
    {
        if (false !== stripos($ex->getMessage(), 'does not support multiple rowsets')) {
            return true;
        }

        return false;
    }
}
