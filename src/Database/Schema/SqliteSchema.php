<?php

namespace DreamFactory\Core\SqlDb\Database\Schema;

use DreamFactory\Core\Database\Schema\ColumnSchema;
use DreamFactory\Core\Database\Schema\TableSchema;
use DreamFactory\Core\Enums\DbResourceTypes;
use DreamFactory\Core\Enums\DbSimpleTypes;

/**
 * Schema is the class for retrieving metadata information from a SQLite (2/3) database.
 */
class SqliteSchema extends SqlSchema
{
    public function getSupportedResourceTypes()
    {
        return [
            DbResourceTypes::TYPE_TABLE,
            DbResourceTypes::TYPE_TABLE_FIELD,
            DbResourceTypes::TYPE_TABLE_CONSTRAINT,
            DbResourceTypes::TYPE_TABLE_RELATIONSHIP,
        ];
    }

    protected function translateSimpleColumnTypes(array &$info)
    {
        // override this in each schema class
        $type = (isset($info['type'])) ? $info['type'] : null;
        switch ($type) {
            // some types need massaging, some need other required properties
            case 'pk':
            case DbSimpleTypes::TYPE_ID:
                $info['type'] = 'integer';
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

            case DbSimpleTypes::TYPE_TIMESTAMP_ON_CREATE:
            case DbSimpleTypes::TYPE_TIMESTAMP_ON_UPDATE:
                $info['type'] = 'timestamp';
                $default = (isset($info['default'])) ? $info['default'] : null;
                if (!isset($default)) {
                    $default = 'CURRENT_TIMESTAMP';
                    $info['default'] = ['expression' => $default];
                }
                break;

            case DbSimpleTypes::TYPE_USER_ID:
            case DbSimpleTypes::TYPE_USER_ID_ON_CREATE:
            case DbSimpleTypes::TYPE_USER_ID_ON_UPDATE:
                $info['type'] = 'integer';
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
                $default = (isset($info['default'])) ? $info['default'] : null;
                if (isset($default)) {
                    $info['default'] = floatval($default);
                }
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
                $info['type'] = 'blob';
                break;
        }
    }

    protected function validateColumnSettings(array &$info)
    {
        // override this in each schema class
        $type = (isset($info['type'])) ? $info['type'] : null;
        switch ($type) {
            // some types need massaging, some need other required properties
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

        if (isset($info['is_primary_key']) && filter_var($info['is_primary_key'], FILTER_VALIDATE_BOOLEAN)) {
            $definition .= ' PRIMARY KEY';
        } elseif (isset($info['is_unique']) && filter_var($info['is_unique'], FILTER_VALIDATE_BOOLEAN)) {
            $definition .= ' UNIQUE';
        }

        $isForeignKey = (isset($info['is_foreign_key'])) ? boolval($info['is_foreign_key']) : false;
        if ((DbSimpleTypes::TYPE_REF == $type) || $isForeignKey) {
            // special case for references because the table referenced may not be created yet
            $refTable = (isset($info['ref_table'])) ? $info['ref_table'] : null;
            if (empty($refTable)) {
                throw new \Exception("Invalid schema detected - no table element for reference type.");
            }

            $refColumns = array_get($info, 'ref_field', array_get($info, 'ref_fields'));
            $refOnDelete = array_get($info, 'ref_on_delete');
            $refOnUpdate = array_get($info, 'ref_on_update');

            $definition .= " REFERENCES $refTable($refColumns)";
            if (!empty($refOnUpdate)) {
                $definition .= " ON UPDATE $refOnUpdate";
            }
            if (!empty($refOnDelete)) {
                $definition .= " ON DELETE $refOnDelete";
            }
        }

        $auto = (isset($info['auto_increment'])) ? filter_var($info['auto_increment'], FILTER_VALIDATE_BOOLEAN) : false;
        if ($auto) {
            $definition .= ' AUTOINCREMENT';
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

        if ($value !== null) {
            $value = (int)($value) - 1;
        } else {
            $value = $this->selectValue("SELECT MAX(`{$table->primaryKey}`) FROM {$table->quotedName}");
            $value = intval($value);
        }
        try {
            // it's possible that 'sqlite_sequence' does not exist
            $this->connection->statement("UPDATE sqlite_sequence SET seq='$value' WHERE name='{$table->name}'");
        } catch (\Exception $e) {
        }
    }

    /**
     * @inheritdoc
     */
    protected function getTableNames($schema = '')
    {
        $sql = "SELECT DISTINCT tbl_name FROM sqlite_master WHERE tbl_name<>'sqlite_sequence'";

        $rows = $this->connection->select($sql);

        $names = [];
        foreach ($rows as $row) {
            $name = $row->tbl_name;
            $quotedName = $this->quoteTableName($name);;
            $settings = compact('name', 'quotedName');
            $names[strtolower($name)] = new TableSchema($settings);
        }

        return $names;
    }

    /**
     * @inheritdoc
     */
    protected function loadTableColumns(TableSchema $table)
    {
        $sql = "PRAGMA table_info({$table->quotedName})";

        $result = $this->connection->select($sql);
        foreach ($result as $column) {
            $column = array_change_key_case((array)$column, CASE_LOWER);
            $c = new ColumnSchema(['name' => $column['name']]);
            $c->quotedName = $this->quoteColumnName($c->name);
            $c->allowNull = (1 != $column['notnull']);
            $c->isPrimaryKey = ($column['pk'] != 0);
            $c->comment = null; // SQLite does not support column comments at all

            $c->dbType = strtolower($column['type']);
            $this->extractLimit($c, $c->dbType);
            $c->fixedLength = $this->extractFixedLength($c->dbType);
            $c->supportsMultibyte = $this->extractMultiByteSupport($c->dbType);
            $this->extractType($c, $c->dbType);
            $this->extractDefault($c, $column['dflt_value']);

            if ($c->isPrimaryKey) {
                if (DbSimpleTypes::TYPE_INTEGER === $c->type) {
                    $c->autoIncrement = true; //defaults to alias of ROWID internally
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
        $constraints = [];
        /** @type TableSchema $each */
        foreach ($this->getTableNames() as $each) {
            $tn = strtolower($each->name);
            $sql = "PRAGMA index_list({$each->quotedName})";
            $results = $this->connection->select($sql);
            /* seq, name, unique, origin, partial */
            foreach ($results as $index) {
                $index = (array)$index;
                $name = $index['name'];
                $sql = "PRAGMA index_info({$name})";
                $cols = $this->connection->select($sql);
                /* seq, name, unique, origin, partial */
                $columnNames = [];
                foreach ($cols as $col) {
                    $col = (array)$col;
                    $columnNames[] = $col['name'];
                }
                $constraints[''][$tn][$name] = [
                    'table_schema'    => '',
                    'table_name'      => $each->name,
                    'column_name'     => $columnNames,
                    'constraint_name' => $name,
                    'constraint_type' => $index['origin'],
                ];
            }

            $sql = "PRAGMA foreign_key_list({$each->quotedName})";
            $fks = $this->connection->select($sql);
            /* id, seq, table, from, to, on_update, on_delete, match */
            foreach ($fks as $key) {
                $key = (array)$key;
                $name = 'fk_' . $each->name . '_' . $key['from'];
                $constraints[''][$tn][$name] = [
                    'table_schema'            => '',
                    'table_name'              => $each->name,
                    'column_name'             => $key['from'],
                    'constraint_name'         => $name,
                    'constraint_type'         => 'f',
                    'referenced_table_schema' => '',
                    'referenced_table_name'   => $key['table'],
                    'referenced_column_name'  => $key['to'],
                    'update_rule'             => $key['on_update'],
                    'delete_rule'             => $key['on_delete'],
                ];
            }
        }

        return $constraints;
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
        return 'ALTER TABLE ' . $this->quoteTableName($table) . ' RENAME TO ' . $this->quoteTableName($newName);
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
        return "DELETE FROM " . $this->quoteTableName($table);
    }

    /**
     * @inheritdoc
     */
    public function dropColumns($table, $column)
    {
        throw new \Exception('Dropping DB column is not supported by SQLite.');
    }

    /**
     * Builds a SQL statement for renaming a column.
     * Because SQLite does not support renaming a DB column, calling this method will throw an exception.
     *
     * @param string $table   the table whose column is to be renamed. The name will be properly quoted by the method.
     * @param string $name    the old name of the column. The name will be properly quoted by the method.
     * @param string $newName the new name of the column. The name will be properly quoted by the method.
     *
     * @throws \Exception
     * @return string the SQL statement for renaming a DB column.
     */
    public function renameColumn($table, $name, $newName)
    {
        throw new \Exception('Renaming a DB column is not supported by SQLite.');
    }

    /**
     * Builds a SQL statement for adding a foreign key constraint to an existing table.
     * Because SQLite does not support adding foreign key to an existing table, calling this method will throw an
     * exception.
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
     * @throws \Exception
     * @return string the SQL statement for adding a foreign key constraint to an existing table.
     */
    public function addForeignKey($name, $table, $columns, $refTable, $refColumns, $delete = null, $update = null)
    {
        throw new \Exception('Adding a foreign key constraint to an existing table is not supported by SQLite.');
    }

    /**
     * Builds a SQL statement for dropping a foreign key constraint.
     * Because SQLite does not support dropping a foreign key constraint, calling this method will throw an exception.
     *
     * @param string $name  the name of the foreign key constraint to be dropped. The name will be properly quoted by
     *                      the method.
     * @param string $table the table whose foreign is to be dropped. The name will be properly quoted by the method.
     *
     * @throws \Exception
     * @return string the SQL statement for dropping a foreign key constraint.
     */
    public function dropForeignKey($name, $table)
    {
        throw new \Exception('Dropping a foreign key constraint is not supported by SQLite.');
    }

    /**
     * @inheritdoc
     */
    public function alterColumn($table, $column, $definition)
    {
        throw new \Exception('Altering a DB column is not supported by SQLite.');
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

    /**
     * Builds a SQL statement for adding a primary key constraint to an existing table.
     * Because SQLite does not support adding a primary key on an existing table this method will throw an exception.
     *
     * @param string       $name    the name of the primary key constraint.
     * @param string       $table   the table that the primary key constraint will be added to.
     * @param string|array $columns comma separated string or array of columns that the primary key will consist of.
     *
     * @throws \Exception
     * @return string the SQL statement for adding a primary key constraint to an existing table.
     */
    public function addPrimaryKey($name, $table, $columns)
    {
        throw new \Exception('Adding a primary key after table has been created is not supported by SQLite.');
    }

    /**
     * Builds a SQL statement for removing a primary key constraint to an existing table.
     * Because SQLite does not support dropping a primary key from an existing table this method will throw an exception
     *
     * @param string $name  the name of the primary key constraint to be removed.
     * @param string $table the table that the primary key constraint will be removed from.
     *
     * @throws \Exception
     * @return string the SQL statement for removing a primary key constraint from an existing table.
     */
    public function dropPrimaryKey($name, $table)
    {
        throw new \Exception('Removing a primary key after table has been created is not supported by SQLite.');
    }

    public function getTimestampForSet()
    {
        return $this->connection->raw("datetime('now')");
    }

    public function allowsSeparateForeignConstraint()
    {
        return false;
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
        if ($field->dbType === 'timestamp' && $defaultValue === 'CURRENT_TIMESTAMP') {
            $defaultValue = null;
        } else {
            $defaultValue = strcasecmp($defaultValue, 'null') ? $defaultValue : null;
        }

        if (is_string($defaultValue)) // PHP 5.2.6 adds single quotes while 5.2.0 doesn't
        {
            $defaultValue = trim($defaultValue, "'\"");
        }

        parent::extractDefault($field, $defaultValue);
    }
}
