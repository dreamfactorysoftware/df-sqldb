<?php
/**
 * COciSchema class file.
 *
 * @author    Ricardo Grana <rickgrana@yahoo.com.br>
 * @link      http://www.yiiframework.com/
 * @copyright Copyright &copy; 2008-2011 Yii Software LLC
 * @license   http://www.yiiframework.com/license/
 */

/**
 * COciSchema is the class for retrieving metadata information from an Oracle database.
 *
 * @property string $defaultSchema Default schema.
 *
 * @author  Ricardo Grana <rickgrana@yahoo.com.br>
 * @package system.db.schema.oci
 */
class COciSchema extends CDbSchema
{
    private $_defaultSchema = '';

    /**
     * @var array the abstract column types mapped to physical column types.
     * @since 1.1.6
     */
    public $columnTypes = array(
        // no autoincrement, requires sequences and optionally triggers or client input
        'pk'        => 'NUMBER(10) NOT NULL PRIMARY KEY',
        // new no sequence identity setting from 12c
        //        'pk' => 'NUMBER GENERATED ALWAYS AS IDENTITY',
        'string'    => 'VARCHAR2(255)',
        'text'      => 'CLOB',
        'integer'   => 'NUMBER(10)',
        'float'     => 'BINARY_FLOAT',
        'double'    => 'BINARY_DOUBLE',
        'decimal'   => 'NUMBER',
        'datetime'  => 'TIMESTAMP',
        'timestamp' => 'TIMESTAMP',
        'time'      => 'TIMESTAMP',
        'date'      => 'DATE',
        'binary'    => 'BLOB',
        'boolean'   => 'NUMBER(1)',
        'money'     => 'NUMBER(19,4)',
    );

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
     * Quotes a column name for use in a query.
     * A simple column name does not contain prefix.
     *
     * @param string $name column name
     *
     * @return string the properly quoted column name
     * @since 1.1.6
     */
    public function quoteSimpleColumnName( $name )
    {
        return '"' . $name . '"';
    }

    /**
     * Creates a command builder for the database.
     * This method may be overridden by child classes to create a DBMS-specific command builder.
     *
     * @return CDbCommandBuilder command builder instance
     */
    protected function createCommandBuilder()
    {
        return new COciCommandBuilder( $this );
    }

    /**
     * @param string $schema default schema.
     */
    public function setDefaultSchema( $schema )
    {
        $this->_defaultSchema = $schema;
    }

    /**
     * @return string default schema.
     */
    public function getDefaultSchema()
    {
        if ( !strlen( $this->_defaultSchema ) )
        {
            $this->setDefaultSchema( strtoupper( $this->getDbConnection()->username ) );
        }

        return $this->_defaultSchema;
    }

    /**
     * @param string $table table name with optional schema name prefix, uses default schema name prefix is not provided.
     *
     * @return array tuple as ($schemaName,$tableName)
     */
    protected function getSchemaTableName( $table )
    {
        $table = strtoupper( $table );
        if ( count( $parts = explode( '.', str_replace( '"', '', $table ) ) ) > 1 )
        {
            return array($parts[0], $parts[1]);
        }
        else
        {
            return array($this->getDefaultSchema(), $parts[0]);
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
        $table = new COciTableSchema;
        $this->resolveTableNames( $table, $name );

        if ( !$this->findColumns( $table ) )
        {
            return null;
        }
        $this->findConstraints( $table );

        return $table;
    }

    /**
     * Generates various kinds of table names.
     *
     * @param COciTableSchema $table the table instance
     * @param string          $name  the unquoted table name
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
            $schemaName = $this->getDefaultSchema();
            $tableName = $parts[0];
        }

        $table->name = $tableName;
        $table->schemaName = $schemaName;
        if ( $schemaName === $this->getDefaultSchema() )
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
     * @param COciTableSchema $table the table metadata
     *
     * @return boolean whether the table exists in the database
     */
    protected function findColumns( $table )
    {
        $schemaName = $table->schemaName;
        $tableName = $table->name;

        $sql = <<<EOD
SELECT a.column_name, a.data_type ||
    case
        when data_precision is not null
            then '(' || a.data_precision ||
                    case when a.data_scale > 0 then ',' || a.data_scale else '' end
                || ')'
        when data_type = 'DATE' then ''
        when data_type = 'NUMBER' then ''
        else '(' || to_char(a.data_length) || ')'
    end as data_type,
    a.nullable, a.data_default,
    (   SELECT D.constraint_type
        FROM ALL_CONS_COLUMNS C
        inner join ALL_constraints D on D.OWNER = C.OWNER and D.constraint_name = C.constraint_name
        WHERE C.OWNER = B.OWNER
           and C.table_name = B.object_name
           and C.column_name = A.column_name
           and D.constraint_type = 'P') as Key,
    com.comments as column_comment
FROM ALL_TAB_COLUMNS A
inner join ALL_OBJECTS B ON b.owner = a.owner and ltrim(B.OBJECT_NAME) = ltrim(A.TABLE_NAME)
LEFT JOIN user_col_comments com ON (A.table_name = com.table_name AND A.column_name = com.column_name)
WHERE
    a.owner = '{$schemaName}'
	and (b.object_type = 'TABLE' or b.object_type = 'VIEW')
	and b.object_name = '{$tableName}'
ORDER by a.column_id
EOD;

        $command = $this->getDbConnection()->createCommand( $sql );

        if ( ( $columns = $command->queryAll() ) === array() )
        {
            return false;
        }

        foreach ( $columns as $column )
        {
            $c = $this->createColumn( $column );

            $table->columns[$c->name] = $c;
            if ( $c->isPrimaryKey )
            {
                if ( $table->primaryKey === null )
                {
                    $table->primaryKey = $c->name;
                }
                elseif ( is_string( $table->primaryKey ) )
                {
                    $table->primaryKey = array($table->primaryKey, $c->name);
                }
                else
                {
                    $table->primaryKey[] = $c->name;
                }

                // set defaults
                $c->autoIncrement = false;
                $table->sequenceName = '';

                $sql = <<<EOD
SELECT trigger_body FROM ALL_TRIGGERS
WHERE table_owner = '{$schemaName}' and table_name = '{$tableName}'
and triggering_event = 'INSERT' and status = 'ENABLED' and trigger_type = 'BEFORE EACH ROW'
EOD;

                $trig = $command = $this->getDbConnection()->createCommand( $sql )->queryScalar();
                if ( !empty( $trig ) )
                {
                    $c->autoIncrement = true;
                    $seq = stristr( $trig, '.nextval', true );
                    $seq = substr( $seq, strrpos( $seq, ' ' ) + 1 );
                    $table->sequenceName = $seq;
                }
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
        $c = new COciColumnSchema;
        $c->name = $column['COLUMN_NAME'];
        $c->rawName = $this->quoteColumnName( $c->name );
        $c->allowNull = $column['NULLABLE'] === 'Y';
        $c->isPrimaryKey = strpos( $column['KEY'], 'P' ) !== false;
        $c->isForeignKey = false;
        $c->init( $column['DATA_TYPE'], $column['DATA_DEFAULT'] );
        $c->comment = $column['COLUMN_COMMENT'] === null ? '' : $column['COLUMN_COMMENT'];

        return $c;
    }

    /**
     * Collects the primary and foreign key column details for the given table.
     *
     * @param COciTableSchema $table the table metadata
     */
    protected function findConstraints( $table )
    {
        $defaultSchema = static::getDefaultSchema();
        $schema = ( !empty( $table->schemaName ) ) ? $table->schemaName : $defaultSchema;

        $sql = <<<EOD
		SELECT D.constraint_type, C.position, D.r_constraint_name,
            C.owner as table_schema,
            C.table_name as table_name,
		    C.column_name as column_name,
            E.owner as referenced_table_schema,
            E.table_name as referenced_table_name,
            F.column_name as referenced_column_name
        FROM ALL_CONS_COLUMNS C
        inner join ALL_constraints D on D.OWNER = C.OWNER and D.constraint_name = C.constraint_name
        left join ALL_constraints E on E.OWNER = D.r_OWNER and E.constraint_name = D.r_constraint_name
        left join ALL_cons_columns F on F.OWNER = E.OWNER and F.constraint_name = E.constraint_name and F.position = C.position
        WHERE D.constraint_type = 'R'
        ORDER BY D.constraint_name, C.position
EOD;
        $columns = $columns2 = $command = $this->getDbConnection()->createCommand( $sql )->queryAll();

        foreach ( $columns as $key => $column )
        {
            $ts = $column['TABLE_SCHEMA'];
            $tn = $column['TABLE_NAME'];
            $cn = $column['COLUMN_NAME'];
            $rts = $column['REFERENCED_TABLE_SCHEMA'];
            $rtn = $column['REFERENCED_TABLE_NAME'];
            $rcn = $column['REFERENCED_COLUMN_NAME'];
            if ( ( 0 == strcasecmp( $tn, $table->name ) ) && ( 0 == strcasecmp( $ts, $schema ) ) )
            {
                $name = ( $rts == $defaultSchema ) ? $rtn : $rts . '.' . $rtn;

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
                $name = ( $ts == $defaultSchema ) ? $tn : $ts . '.' . $tn;
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
                        $ts2 = $column2['TABLE_SCHEMA'];
                        $tn2 = $column2['TABLE_NAME'];
                        $cn2 = $column2['COLUMN_NAME'];
                        if ( ( 0 == strcasecmp( $ts2, $ts ) ) && ( 0 == strcasecmp( $tn2, $tn ) )
                        )
                        {
                            $rts2 = $column2['REFERENCED_TABLE_SCHEMA'];
                            $rtn2 = $column2['REFERENCED_TABLE_NAME'];
                            $rcn2 = $column2['REFERENCED_COLUMN_NAME'];
                            if ( ( 0 != strcasecmp( $rts2, $schema ) ) || ( 0 != strcasecmp( $rtn2, $table->name ) )
                            )
                            {
                                $name2 = ( $rts2 == $defaultSchema ) ? $rtn2 : $rts2 . '.' . $rtn2;
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

    protected function findSchemaNames()
    {
        if ( 'SYSTEM' == $this->getDefaultSchema() )
        {
            $sql = 'SELECT username FROM all_users';
        }
        else
        {
            $sql = <<<SQL
SELECT username FROM all_users WHERE username not in ('SYSTEM','SYS','SYSAUX')
SQL;
        }

        return $this->getDbConnection()->createCommand( $sql )->queryColumn();
    }

    /**
     * Returns all table names in the database.
     *
     * @param string $schema the schema of the tables. Defaults to empty string, meaning the current or default schema.
     *                       If not empty, the returned table names will be prefixed with the schema name.
     * @param bool   $include_views
     *
     * @return array all table names in the database.
     */
    protected function findTableNames( $schema = '', $include_views = true )
    {
        if ( $include_views )
        {
            $condition = "object_type in ('TABLE','VIEW')";
        }
        else

        {
            $condition = "object_type = 'TABLE'";
        }

//SELECT table_name, '{$schema}' as table_schema FROM user_tables

        $sql = <<<EOD
SELECT object_name as table_name, owner as table_schema FROM all_objects WHERE $condition
EOD;

        if ( !empty( $schema ) )
        {
            $sql .= " AND owner = '$schema'";
        }

        $defaultSchema = $this->getDefaultSchema();

        $rows = $this->getDbConnection()->createCommand( $sql )->queryAll();

        $names = array();
        foreach ( $rows as $row )
        {
            $ts = isset( $row['TABLE_SCHEMA'] ) ? $row['TABLE_SCHEMA'] : '';
            $tn = isset( $row['TABLE_NAME'] ) ? $row['TABLE_NAME'] : '';
            $names[] = ( $defaultSchema == $ts ) ? $tn : $ts . '.' . $tn;
        }

        return $names;
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
     * Builds a SQL statement for changing the definition of a column.
     *
     * @param string $table      the table whose column is to be changed. The table name will be properly quoted by the method.
     * @param string $column     the name of the column to be changed. The name will be properly quoted by the method.
     * @param string $definition the new column type. The {@link getColumnType} method will be invoked to convert abstract column type (if any)
     *                           into the physical one. Anything that is not recognized as abstract type will be kept in the generated SQL.
     *                           For example, 'string' will be turned into 'varchar( 255 )', while 'string not null' will become 'varchar( 255 ) not null'.
     *
     * @return string the SQL statement for changing the definition of a column.
     * @since 1.1.6
     */
    public function alterColumn( $table, $column, $definition )
    {
        $definition = $this->getColumnType( $definition );
        $sql = 'ALTER TABLE ' . $this->quoteTableName( $table ) . ' MODIFY ' . $this->quoteColumnName( $column ) . ' ' . $this->getColumnType( $definition );

        return $sql;
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
     * Resets the sequence value of a table's primary key .
     * The sequence will be reset such that the primary key of the next new row inserted
     * will have the specified value or max value of a primary key plus one (i.e. sequence trimming).
     *
     * Note, behavior of this method has changed since 1.1.14 release.
     * Please refer to the following issue for more details:
     * {@link  https://github.com/yiisoft/yii/issues/2241}
     *
     * @param CDbTableSchema $table the table schema whose primary key sequence will be reset
     * @param integer | null $value the value for the primary key of the next new row inserted.
     *                              If this is not set, the next new row's primary key will
     *                              have the max value of a primary key plus one (i.e. sequence trimming).
     *
     * @since 1.1.13
     */
    public function resetSequence( $table, $value = null )
    {
        if ( $table->sequenceName === null )
        {
            return;
        }

        if ( $value !== null )
        {
            $value = (int)$value;
        }
        else
        {
            $value = (int)$this->getDbConnection()->createCommand( "SELECT MAX(\"{$table->primaryKey}\") FROM {$table->rawName}" )->queryScalar();
            $value++;
        }
        $this->getDbConnection()->createCommand(
            "DROP SEQUENCE \"{
            $table->name}_SEQ\""
        )->execute();
        $this->getDbConnection()->createCommand(
            "CREATE SEQUENCE \"{
            $table->name}_SEQ\" START WITH {
            $value} INCREMENT BY 1 NOMAXVALUE NOCACHE"
        )->execute();
    }

    /**
     * Enables or disables integrity check.
     *
     * @param boolean $check  whether to turn on or off the integrity check.
     * @param string  $schema the schema of the tables. Defaults to empty string, meaning the current or default schema.
     *
     * @since 1.1.14
     */
    public function checkIntegrity( $check = true, $schema = '' )
    {
        if ( $schema === '' )
        {
            $schema = $this->getDefaultSchema();
        }
        $mode = $check ? 'ENABLE' : 'DISABLE';
        foreach ( $this->getTableNames( $schema ) as $table )
        {
            $constraints =
                $this->getDbConnection()->createCommand( "SELECT CONSTRAINT_NAME FROM USER_CONSTRAINTS WHERE TABLE_NAME=:t AND OWNER=:o" )->queryColumn(
                    array(':t' => $table, ':o' => $schema)
                );
            foreach ( $constraints as $constraint )
            {
                $this->getDbConnection()->createCommand( "ALTER TABLE \"{$schema}\".\"{$table}\" {$mode} CONSTRAINT \"{$constraint}\"" )->execute();
            }
        }
    }

    /**
     * {@InheritDoc}
     */
    public function addForeignKey( $name, $table, $columns, $refTable, $refColumns, $delete = null, $update = null )
    {
        // ON UPDATE not supported by Oracle
        return parent::addForeignKey( $name, $table, $columns, $refTable, $refColumns, $delete, null );
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
        foreach ( $params as $_key => $_param )
        {
            $_pName = ( isset( $_param['name'] ) && !empty( $_param['name'] ) ) ? $_param['name'] : "p$_key";

            if ( !empty( $_paramStr ) )
            {
                $_paramStr .= ', ';
            }

//            switch ( strtoupper( strval( isset($_param['param_type']) ? $_param['param_type'] : 'IN' ) ) )
//            {
//                case 'INOUT':
//                case 'OUT':
//                default:
            $_paramStr .= ":$_pName";
//                    break;
//            }
        }

        $_sql = "BEGIN $name($_paramStr); END;";
        $_command = $this->getDbConnection()->createCommand( $_sql );
        // do binding
        foreach ( $params as $_key => $_param )
        {
            $_pName = ( isset( $_param['name'] ) && !empty( $_param['name'] ) ) ? $_param['name'] : "p$_key";

//            switch ( strtoupper( strval( isset($_param['param_type']) ? $_param['param_type'] : 'IN' ) ) )
//            {
//                case 'IN':
//                case 'INOUT':
//                case 'OUT':
//                default:
            $_rType = ( isset( $_param['type'] ) ) ? $_param['type'] : 'string';
            $_rLength = ( isset( $_param['length'] ) ) ? $_param['length'] : 256;
            $_pdoType = $_command->getConnection()->getPdoType( $_rType );
            $_command->bindParam( ":$_pName", $params[$_key]['value'], $_pdoType | PDO::PARAM_INPUT_OUTPUT, $_rLength );
//                    break;
//            }
        }

        // Oracle stored procedures don't return result sets directly, must use OUT parameter.
        $_command->execute();

        return null;
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
        $_sql = "SELECT $name($_paramStr) FROM DUAL";
        $_command = $this->getDbConnection()->createCommand( $_sql );

        // do binding
        $_command->bindValues( $_bindings );

        // Move to the next result and get results
        $_reader = $_command->query();
        $_result = $_reader->readAll();
        if ( $_reader->nextResult() )
        {
            // more data coming, make room
            $_result = array($_result);
            do
            {
                $_result[] = $_reader->readAll();
            }
            while ( $_reader->nextResult() );
        }

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
        $type = trim( strtoupper( $type ) );

        if ( $type != 'PROCEDURE' && $type != 'FUNCTION' )
        {
            throw new InvalidArgumentException( 'The type "' . $type . '" is invalid.' );
        }

        $_select = ( empty( $schema ) || ( $defaultSchema == $schema ) ) ? 'OBJECT_NAME' : "CONCAT(CONCAT(OWNER,'.'),OBJECT_NAME)";
        $_schema = !empty( $schema ) ? " AND OWNER = '" . $schema . "'" : null;

        $_sql = <<<MYSQL
SELECT
    {$_select}
FROM
    all_objects
WHERE
    OBJECT_TYPE = :routine_type
    {$_schema}
MYSQL;

        return $this->getDbConnection()->createCommand( $_sql )->queryColumn( array(':routine_type' => $type) );
    }
}
