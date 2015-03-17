<?php
/**
 * CPgsqlSchema class file.
 *
 * @author    Qiang Xue <qiang.xue@gmail.com>
 * @link      http://www.yiiframework.com/
 * @copyright Copyright &copy; 2008-2011 Yii Software LLC
 * @license   http://www.yiiframework.com/license/
 */

/**
 * CPgsqlSchema is the class for retrieving metadata information from a PostgreSQL database.
 *
 * @author  Qiang Xue <qiang.xue@gmail.com>
 * @package system.db.schema.pgsql
 * @since   1.0
 */
class CPgsqlSchema extends CDbSchema
{
    const DEFAULT_SCHEMA = 'public';

    /**
     * @var array the abstract column types mapped to physical column types.
     * @since 1.1.6
     */
    public $columnTypes = array(
        'pk'        => 'serial NOT NULL PRIMARY KEY',
        'string'    => 'character varying (255)',
        'text'      => 'text',
        'integer'   => 'integer',
        'float'     => 'real',
        'double'    => 'double precision',
        'decimal'   => 'numeric',
        'datetime'  => 'timestamp',
        'timestamp' => 'timestamp',
        'time'      => 'time',
        'date'      => 'date',
        'binary'    => 'bytea',
        'boolean'   => 'boolean',
        'money'     => 'decimal(19,4)',
    );

    private $_sequences = array();

    /**
     * @return string default schema.
     */
    public function getDefaultSchema()
    {
        return static::DEFAULT_SCHEMA;
    }

    /**
     * Quotes a table name for use in a query.
     * A simple table name does not schema prefix.
     *
     * @param string $name table name
     *
     * @return string the properly quoted table name
     * @since 1.1.6
     */
    public function quoteSimpleTableName( $name )
    {
        return '"' . $name . '"';
    }

    /**
     * Resets the sequence value of a table's primary key.
     * The sequence will be reset such that the primary key of the next new row inserted
     * will have the specified value or max value of a primary key plus one (i.e. sequence trimming).
     *
     * @param CDbTableSchema $table the table schema whose primary key sequence will be reset
     * @param integer|null   $value the value for the primary key of the next new row inserted.
     *                              If this is not set, the next new row's primary key will have the max value of a primary
     *                              key plus one (i.e. sequence trimming).
     *
     * @since 1.1
     */
    public function resetSequence( $table, $value = null )
    {
        if ( $table->sequenceName === null )
        {
            return;
        }
        $sequence = '"' . $table->sequenceName . '"';
        if ( strpos( $sequence, '.' ) !== false )
        {
            $sequence = str_replace( '.', '"."', $sequence );
        }
        if ( $value !== null )
        {
            $value = (int)$value;
        }
        else
        {
            $value = "(SELECT COALESCE(MAX(\"{$table->primaryKey}\"),0) FROM {$table->rawName})+1";
        }
        $this->getDbConnection()->createCommand( "SELECT SETVAL('$sequence',$value,false)" )->execute();
    }

    /**
     * Enables or disables integrity check.
     *
     * @param boolean $check  whether to turn on or off the integrity check.
     * @param string  $schema the schema of the tables. Defaults to empty string, meaning the current or default schema.
     *
     * @since 1.1
     */
    public function checkIntegrity( $check = true, $schema = '' )
    {
        $enable = $check ? 'ENABLE' : 'DISABLE';
        $tableNames = $this->getTableNames( $schema );
        $db = $this->getDbConnection();
        foreach ( $tableNames as $tableName )
        {
            $tableName = '"' . $tableName . '"';
            if ( strpos( $tableName, '.' ) !== false )
            {
                $tableName = str_replace( '.', '"."', $tableName );
            }
            $db->createCommand( "ALTER TABLE $tableName $enable TRIGGER ALL" )->execute();
        }
    }

    /**
     * Loads the metadata for the specified table.
     *
     * @param string $name table name
     *
     * @return CDbTableSchema driver dependent table metadata.
     */
    protected function loadTable( $name )
    {
        $table = new CPgsqlTableSchema;
        $this->resolveTableNames( $table, $name );
        if ( !$this->findColumns( $table ) )
        {
            return null;
        }
        $this->findConstraints( $table );

        if ( is_string( $table->primaryKey ) && isset( $this->_sequences[$table->rawName . '.' . $table->primaryKey] ) )
        {
            $table->sequenceName = $this->_sequences[$table->rawName . '.' . $table->primaryKey];
        }
        elseif ( is_array( $table->primaryKey ) )
        {
            foreach ( $table->primaryKey as $pk )
            {
                if ( isset( $this->_sequences[$table->rawName . '.' . $pk] ) )
                {
                    $table->sequenceName = $this->_sequences[$table->rawName . '.' . $pk];
                    break;
                }
            }
        }

        return $table;
    }

    /**
     * Generates various kinds of table names.
     *
     * @param CPgsqlTableSchema $table the table instance
     * @param string            $name  the unquoted table name
     */
    protected function resolveTableNames( $table, $name )
    {
        $parts = explode( '.', str_replace( '"', '', $name ) );
        if ( isset( $parts[1] ) )
        {
            $schemaName = $parts[0];
            $tableName = $parts[1];
        }
        else
        {
            $schemaName = self::DEFAULT_SCHEMA;
            $tableName = $parts[0];
        }

        $table->name = $tableName;
        $table->schemaName = $schemaName;
        if ( $schemaName === self::DEFAULT_SCHEMA )
        {
            $table->rawName = $this->quoteTableName( $tableName );
        }
        else
        {
            $table->rawName = $this->quoteTableName( $schemaName ) . '.' . $this->quoteTableName( $tableName );
        }
    }

    /**
     * Collects the table column metadata.
     *
     * @param CPgsqlTableSchema $table the table metadata
     *
     * @return boolean whether the table exists in the database
     */
    protected function findColumns( $table )
    {
        $sql = <<<EOD
SELECT a.attname, LOWER(format_type(a.atttypid, a.atttypmod)) AS type, d.adsrc, a.attnotnull, a.atthasdef,
	pg_catalog.col_description(a.attrelid, a.attnum) AS comment
FROM pg_attribute a LEFT JOIN pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum
WHERE a.attnum > 0 AND NOT a.attisdropped
	AND a.attrelid = (SELECT oid FROM pg_catalog.pg_class WHERE relname=:table
		AND relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = :schema))
ORDER BY a.attnum
EOD;
        $command = $this->getDbConnection()->createCommand( $sql );
        $command->bindValue( ':table', $table->name );
        $command->bindValue( ':schema', $table->schemaName );

        if ( ( $columns = $command->queryAll() ) === array() )
        {
            return false;
        }

        foreach ( $columns as $column )
        {
            $c = $this->createColumn( $column );
            $table->columns[$c->name] = $c;

            if ( stripos( $column['adsrc'], 'nextval' ) === 0 && preg_match( '/nextval\([^\']*\'([^\']+)\'[^\)]*\)/i', $column['adsrc'], $matches ) )
            {
                if ( strpos( $matches[1], '.' ) !== false || $table->schemaName === self::DEFAULT_SCHEMA )
                {
                    $this->_sequences[$table->rawName . '.' . $c->name] = $matches[1];
                }
                else
                {
                    $this->_sequences[$table->rawName . '.' . $c->name] = $table->schemaName . '.' . $matches[1];
                }
                $c->autoIncrement = true;
            }
        }

        return true;
    }

    /**
     * Creates a table column.
     *
     * @param array $column column metadata
     *
     * @return CDbColumnSchema normalized column metadata
     */
    protected function createColumn( $column )
    {
        $c = new CPgsqlColumnSchema;
        $c->name = $column['attname'];
        $c->rawName = $this->quoteColumnName( $c->name );
        $c->allowNull = !$column['attnotnull'];
        $c->isPrimaryKey = false;
        $c->isForeignKey = false;
        $c->comment = $column['comment'] === null ? '' : $column['comment'];

        $c->init( $column['type'], $column['atthasdef'] ? $column['adsrc'] : null );

        return $c;
    }

    /**
     * Collects the primary and foreign key column details for the given table.
     *
     * @param CPgsqlTableSchema $table the table metadata
     */
    protected function findConstraints( $table )
    {
        $this->findPrimaryKey( $table );

        $schema = ( !empty( $table->schemaName ) ) ? $table->schemaName : static::DEFAULT_SCHEMA;
        $rc = 'information_schema.referential_constraints';
        $kcu = 'information_schema.key_column_usage';
        if ( isset( $table->catalogName ) )
        {
            $kcu = $table->catalogName . '.' . $kcu;
            $rc = $table->catalogName . '.' . $rc;
        }

        $sql = <<<EOD
		SELECT
		     KCU1.TABLE_SCHEMA AS table_schema
		   , KCU1.TABLE_NAME AS table_name
		   , KCU1.COLUMN_NAME AS column_name
		   , KCU2.TABLE_SCHEMA AS referenced_table_schema
		   , KCU2.TABLE_NAME AS referenced_table_name
		   , KCU2.COLUMN_NAME AS referenced_column_name
		FROM {$this->quoteTableName( $rc )} RC
		JOIN {$this->quoteTableName( $kcu )} KCU1
		ON KCU1.CONSTRAINT_CATALOG = RC.CONSTRAINT_CATALOG
		   AND KCU1.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA
		   AND KCU1.CONSTRAINT_NAME = RC.CONSTRAINT_NAME
		JOIN {$this->quoteTableName( $kcu )} KCU2
		ON KCU2.CONSTRAINT_CATALOG = RC.UNIQUE_CONSTRAINT_CATALOG
		   AND KCU2.CONSTRAINT_SCHEMA =	RC.UNIQUE_CONSTRAINT_SCHEMA
		   AND KCU2.CONSTRAINT_NAME = RC.UNIQUE_CONSTRAINT_NAME
		   AND KCU2.ORDINAL_POSITION = KCU1.ORDINAL_POSITION
EOD;

        $columns = $columns2 = $this->getDbConnection()->createCommand( $sql )->queryAll();

        foreach ( $columns as $key => $column )
        {
            $ts = $column['table_schema'];
            $tn = $column['table_name'];
            $cn = $column['column_name'];
            $rts = $column['referenced_table_schema'];
            $rtn = $column['referenced_table_name'];
            $rcn = $column['referenced_column_name'];
            if ( ( 0 == strcasecmp( $tn, $table->name ) ) && ( 0 == strcasecmp( $ts, $schema ) ) )
            {
                $name = ( $rts == static::DEFAULT_SCHEMA ) ? $rtn : $rts . '.' . $rtn;

                $table->foreignKeys[$cn] = array($name, $rcn);
                if ( isset( $table->columns[$cn] ) )
                {
                    $table->columns[$cn]->isForeignKey = true;
                }

                // Add it to our foreign references as well
                $table->foreignRefs[] = array(
                    'type'      => 'belongs_to',
                    'ref_table' => $name,
                    'ref_field' => $rcn,
                    'field'     => $cn
                );
            }
            elseif ( ( 0 == strcasecmp( $rtn, $table->name ) ) && ( 0 == strcasecmp( $rts, $schema ) ) )
            {
                $name = ( $ts == static::DEFAULT_SCHEMA ) ? $tn : $ts . '.' . $tn;
                $table->foreignRefs[] = array(
                    'type'      => 'has_many',
                    'ref_table' => $name,
                    'ref_field' => $cn,
                    'field'     => $rcn
                );

                // if other has foreign keys to other tables, we can say these are related as well
                foreach ( $columns2 as $key2 => $column2 )
                {
                    if ( 0 != strcasecmp( $key, $key2 ) ) // not same key
                    {
                        $ts2 = $column2['table_schema'];
                        $tn2 = $column2['table_name'];
                        $cn2 = $column2['column_name'];
                        if ( ( 0 == strcasecmp( $ts2, $ts ) ) && ( 0 == strcasecmp( $tn2, $tn ) )
                        )
                        {
                            $rts2 = $column2['referenced_table_schema'];
                            $rtn2 = $column2['referenced_table_name'];
                            $rcn2 = $column2['referenced_column_name'];
                            if ( ( 0 != strcasecmp( $rts2, $schema ) ) || ( 0 != strcasecmp( $rtn2, $table->name ) )
                            )
                            {
                                $name2 = ( $rts2 == static::DEFAULT_SCHEMA ) ? $rtn2 : $rts2 . '.' . $rtn2;
                                // not same as parent, i.e. via reference back to self
                                // not the same key
                                $table->foreignRefs[] = array(
                                    'type'      => 'many_many',
                                    'ref_table' => $name2,
                                    'ref_field' => $rcn2,
                                    'join'      => "$name($cn,$cn2)",
                                    'field'     => $rcn
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Gets the primary key column(s) details for the given table.
     *
     * @param CPgsqlTableSchema $table table
     *
     * @return mixed primary keys (null if no pk, string if only 1 column pk, or array if composite pk)
     */
    protected function findPrimaryKey( $table )
    {
        $kcu = 'information_schema.key_column_usage';
        $tc = 'information_schema.table_constraints';
        if ( isset( $table->catalogName ) )
        {
            $kcu = $table->catalogName . '.' . $kcu;
            $tc = $table->catalogName . '.' . $tc;
        }

        $sql = <<<EOD
		SELECT k.column_name field_name
			FROM {$this->quoteTableName( $kcu )} k
		    LEFT JOIN {$this->quoteTableName( $tc )} c
		      ON k.table_name = c.table_name
		     AND k.constraint_name = c.constraint_name
		   WHERE c.constraint_type ='PRIMARY KEY'
		   	    AND k.table_name = :table
				AND k.table_schema = :schema
EOD;
        $command = $this->getDbConnection()->createCommand( $sql );
        $command->bindValue( ':table', $table->name );
        $command->bindValue( ':schema', $table->schemaName );

        $table->primaryKey = null;
        foreach ( $command->queryAll() as $row )
        {
            $name = $row['field_name'];
            if ( isset( $table->columns[$name] ) )
            {
                $table->columns[$name]->isPrimaryKey = true;
                if ( $table->primaryKey === null )
                {
                    $table->primaryKey = $name;
                }
                elseif ( is_string( $table->primaryKey ) )
                {
                    $table->primaryKey = array($table->primaryKey, $name);
                }
                else
                {
                    $table->primaryKey[] = $name;
                }
            }
        }
    }

    protected function findSchemaNames()
    {
        $sql = <<<SQL
SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema','pg_catalog')
SQL;
        $rows = $this->getDbConnection()->createCommand( $sql )->queryColumn();

        if ( false === array_search( static::DEFAULT_SCHEMA, $rows ) )
        {
            $rows[] = static::DEFAULT_SCHEMA;
        }

        return $rows;
    }

    /**
     * Returns all table names in the database.
     *
     * @param string  $schema        the schema of the tables. Defaults to empty string, meaning the current or default schema.
     *                               If not empty, the returned table names will be prefixed with the schema name.
     * @param boolean $include_views whether to include views in the result. Defaults to true.
     *
     * @return array all table names in the database.
     */
    protected function findTableNames( $schema = '', $include_views = true )
    {
        if ( $include_views )
        {
            $condition = "table_type in ('BASE TABLE','VIEW')";
        }
        else
        {
            $condition = "table_type = 'BASE TABLE'";
        }

        $sql = <<<EOD
SELECT table_name, table_schema FROM information_schema.tables
WHERE $condition
EOD;

        if ( !empty( $schema ) )
        {
            $sql .= " AND table_schema = '$schema'";
        }

        $defaultSchema = self::DEFAULT_SCHEMA;

        $rows = $this->getDbConnection()->createCommand( $sql )->queryAll();

        $names = array();
        foreach ( $rows as $row )
        {
            $ts = isset( $row['table_schema'] ) ? $row['table_schema'] : '';
            $tn = isset( $row['table_name'] ) ? $row['table_name'] : '';
            $names[] = ( $defaultSchema == $ts ) ? $tn : $ts . '.' . $tn;
        }

        return $names;
    }

    /**
     * Returns all stored procedure names in the database.
     *
     * @param string $schema the schema of the stored procedures. Defaults to empty string, meaning the current or default schema.
     *                       If not empty, the returned stored procedure names will be prefixed with the schema name.
     *
     * @return array all stored procedure names in the database.
     */
    protected function findProcedureNames( $schema = '' )
    {
        return $this->_findRoutines( 'procedure', $schema );
    }

    /**
     * @param string $name
     * @param array  $params
     *
     * @return mixed
     * @throws \Exception
     */
    public function callProcedure( $name, &$params )
    {
        $name = $this->getDbConnection()->quoteTableName( $name );
        $_paramStr = '';
        $_bindings = array();
        foreach ( $params as $_key => $_param )
        {
            $_pName = ( isset( $_param['name'] ) && !empty( $_param['name'] ) ) ? $_param['name'] : "p$_key";
            $_pValue = ( isset( $_param['value'] ) ) ? $_param['value'] : null;

            switch ( strtoupper( strval( isset( $_param['param_type'] ) ? $_param['param_type'] : 'IN' ) ) )
            {
                case 'OUT':
                    // not sent as parameters, but pulled from fetch results
                    break;

                case 'INOUT':
                case 'IN':
                default:
                    $_bindings[":$_pName"] = $_pValue;
                    if ( !empty( $_paramStr ) )
                    {
                        $_paramStr .= ', ';
                    }
                    $_paramStr .= ":$_pName";
                    break;
            }
        }

        $_sql = "SELECT * FROM $name($_paramStr);";
        $_command = $this->getDbConnection()->createCommand( $_sql );

        // do binding
        $_command->bindValues( $_bindings );

        // driver does not support multiple result sets currently
        $_result = $_command->queryAll();

        // out parameters come back in fetch results, put them in the params for client
        if ( isset( $_result, $_result[0] ) )
        {
            foreach ( $params as $_key => $_param )
            {
                if ( false !== stripos( strval( isset( $_param['param_type'] ) ? $_param['param_type'] : '' ), 'OUT' ) )
                {
                    $_pName = ( isset( $_param['name'] ) && !empty( $_param['name'] ) ) ? $_param['name'] : "p$_key";
                    if ( isset( $_result[0][$_pName] ) )
                    {
                        $params[$_key]['value'] = $_result[0][$_pName];
                    }
                }
            }
        }

        return $_result;
    }

    /**
     * Returns all stored function names in the database.
     *
     * @param string $schema the schema of the stored function. Defaults to empty string, meaning the current or
     *                       default schema. If not empty, the returned stored function names will be prefixed with the
     *                       schema name.
     *
     * @return array all stored function names in the database.
     */
    protected function findFunctionNames( $schema = '' )
    {
        return $this->_findRoutines( 'function', $schema );
    }

    /**
     * @param string $name
     * @param array  $params
     *
     * @throws Exception
     * @return mixed
     */
    public function callFunction( $name, &$params )
    {
        $name = $this->getDbConnection()->quoteTableName( $name );
        $_bindings = array();
        foreach ( $params as $_key => $_param )
        {
            $_name = ( isset( $_param['name'] ) && !empty( $_param['name'] ) ) ? ':' . $_param['name'] : ":p$_key";
            $_value = isset( $_param['value'] ) ? $_param['value'] : null;

            $_bindings[$_name] = $_value;
        }

        $_paramStr = implode( ',', array_keys( $_bindings ) );
        $_sql = "SELECT * FROM $name($_paramStr)";
        $_command = $this->getDbConnection()->createCommand( $_sql );

        // do binding
        $_command->bindValues( $_bindings );

        // driver does not support multiple result sets currently
        $_result = $_command->queryAll();

        return $_result;
    }

    /**
     * Returns all routines in the database.
     *
     * @param string $type   "procedure" or "function"
     * @param string $schema the schema of the routine. Defaults to empty string, meaning the current or
     *                       default schema. If not empty, the returned stored function names will be prefixed with the
     *                       schema name.
     *
     * @throws InvalidArgumentException
     * @return array all stored function names in the database.
     */
    protected function _findRoutines( $type, $schema = '' )
    {
        $defaultSchema = $this->getDefaultSchema();

        $_select = ( empty( $schema ) || ( $defaultSchema == $schema ) ) ? 'ROUTINE_NAME' : "CONCAT('" . $schema . "','.',ROUTINE_NAME) as ROUTINE_NAME";
        $_schema = !empty( $schema ) ? " WHERE ROUTINE_SCHEMA = '" . $schema . "'" : null;

        $_sql = <<<MYSQL
SELECT
    {$_select}
FROM
    information_schema.ROUTINES
    {$_schema}
MYSQL;

        return $this->getDbConnection()->createCommand( $_sql )->queryColumn();
    }

    /**
     * Builds a SQL statement for renaming a DB table.
     *
     * @param string $table   the table to be renamed. The name will be properly quoted by the method.
     * @param string $newName the new table name. The name will be properly quoted by the method.
     *
     * @return string the SQL statement for renaming a DB table.
     * @since 1.1.6
     */
    public function renameTable( $table, $newName )
    {
        return 'ALTER TABLE ' . $this->quoteTableName( $table ) . ' RENAME TO ' . $this->quoteTableName( $newName );
    }

    /**
     * Builds a SQL statement for adding a new DB column.
     *
     * @param string $table  the table that the new column will be added to. The table name will be properly quoted by the method.
     * @param string $column the name of the new column. The name will be properly quoted by the method.
     * @param string $type   the column type. The {@link getColumnType} method will be invoked to convert abstract column type (if any)
     *                       into the physical one. Anything that is not recognized as abstract type will be kept in the generated SQL.
     *                       For example, 'string' will be turned into 'varchar(255)', while 'string not null' will become 'varchar(255) not null'.
     *
     * @return string the SQL statement for adding a new column.
     * @since 1.1.6
     */
    public function addColumn( $table, $column, $type )
    {
        $type = $this->getColumnType( $type );
        $sql = 'ALTER TABLE ' . $this->quoteTableName( $table ) . ' ADD COLUMN ' . $this->quoteColumnName( $column ) . ' ' . $type;

        return $sql;
    }

    /**
     * Builds a SQL statement for changing the definition of a column.
     *
     * @param string $table  the table whose column is to be changed. The table name will be properly quoted by the method.
     * @param string $column the name of the column to be changed. The name will be properly quoted by the method.
     * @param string $definition   the new column type. The {@link getColumnType} method will be invoked to convert abstract column type (if any)
     *                       into the physical one. Anything that is not recognized as abstract type will be kept in the generated SQL.
     *                       For example, 'string' will be turned into 'varchar(255)', while 'string not null' will become 'varchar(255) not null'.
     *
     * @return string the SQL statement for changing the definition of a column.
     * @since 1.1.6
     */
    public function alterColumn( $table, $column, $definition )
    {
        $sql = 'ALTER TABLE ' . $this->quoteTableName( $table ) . ' ALTER COLUMN ' . $this->quoteColumnName( $column );
        if (false !== $_pos = strpos( $definition, ' ') )
        {
            $sql .= ' TYPE ' . $this->getColumnType( substr( $definition, 0, $_pos ) );
            switch ( substr( $definition, $_pos + 1 ) )
            {
                case 'NULL':
                    $sql .= ', ALTER COLUMN ' . $this->quoteColumnName( $column ) . ' DROP NOT NULL';
                    break;
                case 'NOT NULL':
                    $sql .= ', ALTER COLUMN ' . $this->quoteColumnName( $column ) . ' SET NOT NULL';
                    break;
            }
        }
        else
        {
            $sql .= ' TYPE ' . $this->getColumnType( $definition );
        }

        return $sql;
    }

    /**
     * Builds a SQL statement for creating a new index.
     *
     * @param string  $name    the name of the index. The name will be properly quoted by the method.
     * @param string  $table   the table that the new index will be created for. The table name will be properly quoted by the method.
     * @param string  $columns the column(s) that should be included in the index. If there are multiple columns, please separate them
     *                         by commas. Each column name will be properly quoted by the method, unless a parenthesis is found in the name.
     * @param boolean $unique  whether to add UNIQUE constraint on the created index.
     *
     * @return string the SQL statement for creating a new index.
     * @since 1.1.6
     */
    public function createIndex( $name, $table, $columns, $unique = false )
    {
        $cols = array();
        if ( is_string( $columns ) )
        {
            $columns = preg_split( '/\s*,\s*/', $columns, -1, PREG_SPLIT_NO_EMPTY );
        }
        foreach ( $columns as $col )
        {
            if ( strpos( $col, '(' ) !== false )
            {
                $cols[] = $col;
            }
            else
            {
                $cols[] = $this->quoteColumnName( $col );
            }
        }
        if ( $unique )
        {
            return
                'ALTER TABLE ONLY ' .
                $this->quoteTableName( $table ) .
                ' ADD CONSTRAINT ' .
                $this->quoteTableName( $name ) .
                ' UNIQUE (' .
                implode( ', ', $cols ) .
                ')';
        }
        else
        {
            return 'CREATE INDEX ' . $this->quoteTableName( $name ) . ' ON ' . $this->quoteTableName( $table ) . ' (' . implode( ', ', $cols ) . ')';
        }
    }

    /**
     * Builds a SQL statement for dropping an index.
     *
     * @param string $name  the name of the index to be dropped. The name will be properly quoted by the method.
     * @param string $table the table whose index is to be dropped. The name will be properly quoted by the method.
     *
     * @return string the SQL statement for dropping an index.
     * @since 1.1.6
     */
    public function dropIndex( $name, $table )
    {
        return 'DROP INDEX ' . $this->quoteTableName( $name );
    }

    /**
     * Creates a command builder for the database.
     * This method may be overridden by child classes to create a DBMS-specific command builder.
     *
     * @return CPgsqlCommandBuilder command builder instance.
     */
    protected function createCommandBuilder()
    {
        return new CPgsqlCommandBuilder( $this );
    }
}
