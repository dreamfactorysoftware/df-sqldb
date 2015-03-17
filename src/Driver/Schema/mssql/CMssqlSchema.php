<?php
/**
 * CMssqlSchema class file.
 *
 * @author    Qiang Xue <qiang.xue@gmail.com>
 * @author    Christophe Boulain <Christophe.Boulain@gmail.com>
 * @link      http://www.yiiframework.com/
 * @copyright Copyright &copy; 2008-2011 Yii Software LLC
 * @license   http://www.yiiframework.com/license/
 */

/**
 * CMssqlSchema is the class for retrieving metadata information from a MS SQL Server database.
 *
 * @author  Qiang Xue <qiang.xue@gmail.com>
 * @author  Christophe Boulain <Christophe.Boulain@gmail.com>
 * @package system.db.schema.mssql
 */
class CMssqlSchema extends CDbSchema
{
    const DEFAULT_SCHEMA = 'dbo';

    /**
     * @var array the abstract column types mapped to physical column types.
     * @since 1.1.6
     */
    public $columnTypes = array(
        'pk'        => 'int IDENTITY PRIMARY KEY',
        'string'    => 'varchar(255)',
        'text'      => 'varchar(max)',
        'integer'   => 'int',
        'float'     => 'real',
        'double'    => 'float',
        'decimal'   => 'decimal',
        'datetime'  => 'datetime2',
        'timestamp' => 'datetimeoffset',
        'time'      => 'time',
        'date'      => 'date',
        'binary'    => 'binary',
        'boolean'   => 'bit',
    );

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
        return '[' . $name . ']';
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
        return '[' . $name . ']';
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
    public function compareTableNames( $name1, $name2 )
    {
        $name1 = str_replace( array('[', ']'), '', $name1 );
        $name2 = str_replace( array('[', ']'), '', $name2 );

        return parent::compareTableNames( strtolower( $name1 ), strtolower( $name2 ) );
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
     * @since 1.1.6
     */
    public function resetSequence( $table, $value = null )
    {
        if ( $table->sequenceName === null )
        {
            return;
        }
        if ( $value !== null )
        {
            $value = (int)( $value ) - 1;
        }
        else
        {
            $value = (int)$this->getDbConnection()->createCommand( "SELECT MAX([{$table->primaryKey}]) FROM {$table->rawName}" )->queryScalar();
        }
        $name = strtr( $table->rawName, array('[' => '', ']' => '') );
        $this->getDbConnection()->createCommand( "DBCC CHECKIDENT ('$name',RESEED,$value)" )->execute();
    }

    private $_normalTables = array();  // non-view tables

    /**
     * Enables or disables integrity check.
     *
     * @param boolean $check  whether to turn on or off the integrity check.
     * @param string  $schema the schema of the tables. Defaults to empty string, meaning the current or default schema.
     *
     * @since 1.1.6
     */
    public function checkIntegrity( $check = true, $schema = '' )
    {
        $enable = $check ? 'CHECK' : 'NOCHECK';
        if ( !isset( $this->_normalTables[$schema] ) )
        {
            $this->_normalTables[$schema] = $this->findTableNames( $schema, false );
        }
        $db = $this->getDbConnection();
        foreach ( $this->_normalTables[$schema] as $tableName )
        {
            $tableName = $this->quoteTableName( $tableName );
            $db->createCommand( "ALTER TABLE $tableName $enable CONSTRAINT ALL" )->execute();
        }
    }

    /**
     * Loads the metadata for the specified table.
     *
     * @param string $name table name
     *
     * @return CMssqlTableSchema driver dependent table metadata. Null if the table does not exist.
     */
    protected function loadTable( $name )
    {
        $table = new CMssqlTableSchema;
        $this->resolveTableNames( $table, $name );
        //if (!in_array($table->name, $this->tableNames)) return null;

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
     * @param CMssqlTableSchema $table the table instance
     * @param string            $name  the unquoted table name
     */
    protected function resolveTableNames( $table, $name )
    {
        $parts = explode( '.', str_replace( array('[', ']'), '', $name ) );
        if ( ( $c = count( $parts ) ) == 3 )
        {
            // Catalog name, schema name and table name provided
            $table->catalogName = $parts[0];
            $table->schemaName = $parts[1];
            $table->name = $parts[2];
            $table->rawName =
                $this->quoteTableName( $table->catalogName ) . '.' . $this->quoteTableName( $table->schemaName ) . '.' . $this->quoteTableName( $table->name );
        }
        elseif ( $c == 2 )
        {
            // Only schema name and table name provided
            $table->name = $parts[1];
            $table->schemaName = $parts[0];
            $table->rawName = $this->quoteTableName( $table->schemaName ) . '.' . $this->quoteTableName( $table->name );
        }
        else
        {
            // Only the name given, we need to get at least the schema name
            //if (empty($this->_schemaNames)) $this->findTableNames();
            $table->name = $parts[0];
            $table->schemaName = self::DEFAULT_SCHEMA;
            $table->rawName = $this->quoteTableName( $table->schemaName ) . '.' . $this->quoteTableName( $table->name );
        }
    }

    /**
     * Gets the primary key column(s) details for the given table.
     *
     * @param CMssqlTableSchema $table table
     *
     * @return mixed primary keys (null if no pk, string if only 1 column pk, or array if composite pk)
     */
    protected function findPrimaryKey( $table )
    {
        $kcu = 'INFORMATION_SCHEMA.KEY_COLUMN_USAGE';
        $tc = 'INFORMATION_SCHEMA.TABLE_CONSTRAINTS';
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
        $primary = $command->queryColumn();
        switch ( count( $primary ) )
        {
            case 0: // No primary key on table
                $primary = null;
                break;
            case 1: // Only 1 primary key
                $primary = $primary[0];
                if ( isset( $table->columns[$primary] ) )
                {
                    $table->columns[$primary]->isPrimaryKey = true;
                }
                break;
            default:
                if ( is_array( $primary ) )
                {
                    foreach ( $primary as $key )
                    {
                        if ( isset( $table->columns[$key] ) )
                        {
                            $table->columns[$key]->isPrimaryKey = true;
                        }
                    }
                }
                break;
        }
        $table->primaryKey = $primary;
    }

    /**
     * Collects the foreign key column details for the given table.
     * Also, collects the foreign tables and columns that reference the given table.
     *
     * @param CMssqlTableSchema $table the table metadata
     */
    protected function findConstraints( $table )
    {
        $this->findPrimaryKey( $table );

        $schema = ( !empty( $table->schemaName ) ) ? $table->schemaName : static::DEFAULT_SCHEMA;
        $rc = 'INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS';
        $kcu = 'INFORMATION_SCHEMA.KEY_COLUMN_USAGE';
        if ( isset( $table->catalogName ) )
        {
            $kcu = $table->catalogName . '.' . $kcu;
            $rc = $table->catalogName . '.' . $rc;
        }

        //From http://msdn2.microsoft.com/en-us/library/aa175805(SQL.80).aspx
        $sql = <<<EOD
		SELECT
		     KCU1.TABLE_SCHEMA AS 'table_schema'
		   , KCU1.TABLE_NAME AS 'table_name'
		   , KCU1.COLUMN_NAME AS 'column_name'
		   , KCU2.TABLE_SCHEMA AS 'referenced_table_schema'
		   , KCU2.TABLE_NAME AS 'referenced_table_name'
		   , KCU2.COLUMN_NAME AS 'referenced_column_name'
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
                                $name2 = ( $rts2 == $schema ) ? $rtn2 : $rts2 . '.' . $rtn2;
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
     * Collects the table column metadata.
     *
     * @param CMssqlTableSchema $table the table metadata
     *
     * @return boolean whether the table exists in the database
     */
    protected function findColumns( $table )
    {
        $columnsTable = "INFORMATION_SCHEMA.COLUMNS";
        $where = array();
        $where[] = "t1.TABLE_NAME='" . $table->name . "'";
        if ( isset( $table->catalogName ) )
        {
            $where[] = "t1.TABLE_CATALOG='" . $table->catalogName . "'";
            $columnsTable = $table->catalogName . '.' . $columnsTable;
        }
        if ( isset( $table->schemaName ) )
        {
            $where[] = "t1.TABLE_SCHEMA='" . $table->schemaName . "'";
        }

        if ( false === strpos( $this->dbConnection->connectionString, '.database.windows.net' ) )
        {
            $sql =
                "SELECT t1.*, columnproperty(object_id(t1.table_schema+'.'+t1.table_name), t1.column_name, 'IsIdentity') AS IsIdentity, " .
                "CONVERT(VARCHAR, t2.value) AS Comment FROM " .
                $this->quoteTableName( $columnsTable ) .
                " AS t1 " .
                "LEFT OUTER JOIN sys.extended_properties AS t2 ON t1.ORDINAL_POSITION = t2.minor_id AND " .
                "object_name(t2.major_id) = t1.TABLE_NAME AND t2.class=1 AND t2.class_desc='OBJECT_OR_COLUMN' AND t2.name='MS_Description' " .
                "WHERE " .
                join( ' AND ', $where );
        }
        else
        {
            $sql =
                "SELECT t1.*, columnproperty(object_id(t1.table_schema+'.'+t1.table_name), t1.column_name, 'IsIdentity') AS IsIdentity " .
                "FROM " .
                $this->quoteTableName( $columnsTable ) .
                " AS t1 WHERE " .
                join( ' AND ', $where );
        }

        try
        {
            $columns = $this->getDbConnection()->createCommand( $sql )->queryAll();
            if ( empty( $columns ) )
            {
                return false;
            }
        }
        catch ( Exception $e )
        {
            return false;
        }

        foreach ( $columns as $column )
        {
            $c = $this->createColumn( $column );
            $table->columns[$c->name] = $c;
            if ( $c->autoIncrement && $table->sequenceName === null )
            {
                $table->sequenceName = $table->name;
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
        $c = new CMssqlColumnSchema;
        $c->name = $column['COLUMN_NAME'];
        $c->rawName = $this->quoteColumnName( $c->name );
        $c->allowNull = $column['IS_NULLABLE'] == 'YES';
        if ( $column['NUMERIC_PRECISION_RADIX'] !== null )
        {
            // We have a numeric datatype
            $c->size = $c->precision = $column['NUMERIC_PRECISION'] !== null ? (int)$column['NUMERIC_PRECISION'] : null;
            $c->scale = $column['NUMERIC_SCALE'] !== null ? (int)$column['NUMERIC_SCALE'] : null;
        }
        elseif ( $column['DATA_TYPE'] == 'image' || $column['DATA_TYPE'] == 'text' )
        {
            $c->size = $c->precision = null;
        }
        else
        {
            $c->size = $c->precision = ( $column['CHARACTER_MAXIMUM_LENGTH'] !== null ) ? (int)$column['CHARACTER_MAXIMUM_LENGTH'] : null;
        }
        $c->autoIncrement = ( isset( $column['IsIdentity'] ) ? ( $column['IsIdentity'] == 1 ) : false );
        $c->comment = ( isset( $column['Comment'] ) ? ( $column['Comment'] === null ? '' : $column['Comment'] ) : '' );

        $c->init( $column['DATA_TYPE'], $column['COLUMN_DEFAULT'] );

        return $c;
    }

    protected function findSchemaNames()
    {
        $sql = <<<SQL
SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN
('INFORMATION_SCHEMA', 'sys', 'db_owner', 'db_accessadmin', 'db_securityadmin',
'db_ddladmin', 'db_backupoperator', 'db_datareader', 'db_datawriter',
'db_denydatareader', 'db_denydatawriter')
SQL;

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
            $condition = "TABLE_TYPE in ('BASE TABLE','VIEW')";
        }
        else
        {
            $condition = "TABLE_TYPE='BASE TABLE'";
        }

        $sql = <<<EOD
SELECT TABLE_NAME, TABLE_SCHEMA FROM [INFORMATION_SCHEMA].[TABLES] WHERE $condition
EOD;

        if ( !empty( $schema ) )
        {
            $sql .= " AND TABLE_SCHEMA = '$schema'";
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

        $_pdo = $this->getDbConnection()->getPdoInstance();
        if ( $_pdo instanceof CMssqlSqlsrvPdoAdapter )
        {
            return $this->_callProcedureSqlsrv( $name, $params );
        }
        else
        {
            return $this->_callProcedureDblib( $name, $params );
        }
    }

    protected function _callProcedureSqlsrv( $name, &$params )
    {
        $_paramStr = '';
        foreach ( $params as $_key => $_param )
        {
            $_pName = ( isset( $_param['name'] ) && !empty( $_param['name'] ) ) ? $_param['name'] : "p$_key";

            if ( !empty( $_paramStr ) )
            {
                $_paramStr .= ', ';
            }

            switch ( strtoupper( strval( isset( $_param['param_type'] ) ? $_param['param_type'] : 'IN' ) ) )
            {
                case 'INOUT':
                case 'OUT':
                    $_paramStr .= "@$_pName=:$_pName";
                    break;

                default:
                    $_paramStr .= ":$_pName";
                    break;
            }
        }

        $_sql = "EXEC $name $_paramStr;";
        $_command = $this->getDbConnection()->createCommand( $_sql );

        // do binding
        foreach ( $params as $_key => $_param )
        {
            $_pName = ( isset( $_param['name'] ) && !empty( $_param['name'] ) ) ? $_param['name'] : "p$_key";
            if ( !isset( $_param['value'] ) )
            {
                $_param['value'] = null;
            }

            switch ( strtoupper( strval( isset( $_param['param_type'] ) ? $_param['param_type'] : 'IN' ) ) )
            {
                case '':
                case 'IN':
                    $_command->bindValue( ":$_pName", $_param['value'] );
                    break;
                case 'INOUT':
                case 'OUT':
                    $_rType = ( isset( $_param['type'] ) ) ? $_param['type'] : 'string';
                    $_rLength = ( isset( $_param['length'] ) ) ? $_param['length'] : 256;
                    $_pdoType = $_command->getConnection()->getPdoType( $_rType );
                    $_command->bindParam( ":$_pName", $params[$_key]['value'], $_pdoType | PDO::PARAM_INPUT_OUTPUT, $_rLength );
                    break;
            }
        }

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

    protected function _callProcedureDblib( $name, &$params )
    {
        // Note that using the dblib driver doesn't allow binding of output parameters,
        // and also requires declaration prior to and selecting after to retrieve them.
        $_paramStr = '';
        $_pre = '';
        $_post = '';
        $_skip = 0;
        $_bindings = array();
        foreach ( $params as $_key => $_param )
        {
            $_pName = ( isset( $_param['name'] ) && !empty( $_param['name'] ) ) ? $_param['name'] : "p$_key";
            $_pValue = ( isset( $_param['value'] ) ) ? $_param['value'] : null;

            if ( !empty( $_paramStr ) )
            {
                $_paramStr .= ', ';
            }

            switch ( strtoupper( strval( isset( $_param['param_type'] ) ? $_param['param_type'] : 'IN' ) ) )
            {
                case 'INOUT':
                    // with dblib driver you can't bind output parameters
                    $_rType = $_param['type'];
                    $_pre .= "DECLARE @$_pName $_rType; SET @$_pName = $_pValue;";
                    $_skip++;
                    $_post .= "SELECT @$_pName AS [$_pName];";
                    $_paramStr .= "@$_pName OUTPUT";
                    break;

                case 'OUT':
                    // with dblib driver you can't bind output parameters
                    $_rType = $_param['type'];
                    $_pre .= "DECLARE @$_pName $_rType;";
                    $_post .= "SELECT @$_pName AS [$_pName];";
                    $_paramStr .= "@$_pName OUTPUT";
                    break;

                default:
                    $_bindings[":$_pName"] = $_pValue;
                    $_paramStr .= ":$_pName";
                    break;
            }
        }

        $this->getDbConnection()->createCommand( 'SET QUOTED_IDENTIFIER ON; SET ANSI_WARNINGS ON;' )->execute();
        $_sql = "$_pre EXEC $name $_paramStr; $_post";
        $_command = $this->getDbConnection()->createCommand( $_sql );

        // do binding
        $_command->bindValues( $_bindings );

        $_reader = $_command->query();
        $_result = $_reader->readAll();
        for ( $_i = 0; $_i < $_skip; $_i++ )
        {
            if ( $_reader->nextResult() )
            {
                $_result = $_reader->readAll();
            }
        }
        if ( $_reader->nextResult() )
        {
            // more data coming, make room
            $_result = array($_result);
            do
            {
                $_temp = $_reader->readAll();
                $_keep = true;
                if ( 1 == count( $_temp ) )
                {
                    $_check = current( $_temp );
                    foreach ( $params as &$_param )
                    {
                        $_pName = ( isset( $_param['name'] ) ) ? $_param['name'] : '';
                        if ( isset( $_check[$_pName] ) )
                        {
                            $_param['value'] = $_check[$_pName];
                            $_keep = false;
                        }
                    }
                }
                if ( $_keep )
                {
                    $_result[] = $_temp;
                }
            }
            while ( $_reader->nextResult() );

            // if there is only one data set, just return it
            if ( 1 == count( $_result ) )
            {
                $_result = $_result[0];
            }
        }

        return $_result;
    }

    /**
     * Returns all stored function names in the database.
     *
     * @param string $schema the schema of the stored function. Defaults to empty string, meaning the current or default schema.
     *                       If not empty, the returned stored function names will be prefixed with the schema name.
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
        if ( false === strpos( $name, '.' ) )
        {
            // requires full name with schema here.
            $name = $this->getDefaultSchema() . '.' . $name;
        }
        $name = $this->getDbConnection()->quoteTableName( $name );

        $_bindings = array();
        foreach ( $params as $_key => $_param )
        {
            $_name = ( isset( $_param['name'] ) && !empty( $_param['name'] ) ) ? ':' . $_param['name'] : ":p$_key";
            $_value = isset( $_param['value'] ) ? $_param['value'] : null;

            $_bindings[$_name] = $_value;
        }

        $_paramStr = implode( ',', array_keys( $_bindings ) );
        $_sql = "SELECT $name($_paramStr);";
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
     * Creates a command builder for the database.
     * This method overrides parent implementation in order to create a MSSQL specific command builder
     *
     * @return CDbCommandBuilder command builder instance
     */
    protected function createCommandBuilder()
    {
        return new CMssqlCommandBuilder( $this );
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
        return "sp_rename '$table', '$newName'";
    }

    /**
     * Builds a SQL statement for renaming a column.
     *
     * @param string $table   the table whose column is to be renamed. The name will be properly quoted by the method.
     * @param string $name    the old name of the column. The name will be properly quoted by the method.
     * @param string $newName the new name of the column. The name will be properly quoted by the method.
     *
     * @return string the SQL statement for renaming a DB column.
     * @since 1.1.6
     */
    public function renameColumn( $table, $name, $newName )
    {
        return "sp_rename '$table.$name', '$newName', 'COLUMN'";
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
        $definition = $this->getColumnType( $definition );
        $sql = 'ALTER TABLE ' . $this->quoteTableName( $table ) . ' ALTER COLUMN ' . $this->quoteColumnName( $column ) . ' ' . $this->getColumnType( $definition );

        return $sql;
    }

    /**
     * Returns all routines in the database.
     *
     * @param string $_type  "procedure" or "function"
     * @param string $schema the schema of the routine. Defaults to empty string, meaning the current or
     *                       default schema. If not empty, the returned stored function names will be prefixed with the
     *                       schema name.
     *
     * @throws InvalidArgumentException
     * @return array all stored function names in the database.
     */
    protected function _findRoutines( $_type, $schema = '' )
    {
        $_defaultSchema = $this->getDefaultSchema();
        $_type = trim( strtoupper( $_type ) );

        if ( $_type != 'PROCEDURE' && $_type != 'FUNCTION' )
        {
            throw new InvalidArgumentException( 'The type "' . $_type . '" is invalid.' );
        }

        $_where = !empty( $schema ) ? " AND ROUTINE_SCHEMA = '" . $schema . "'" : null;

        $_sql = <<<MYSQL
SELECT
    ROUTINE_NAME
FROM
    information_schema.ROUTINES
WHERE
    ROUTINE_TYPE = :routine_type
    {$_where}
MYSQL;

        $_results = $this->getDbConnection()->createCommand( $_sql )->queryColumn( array(':routine_type' => $_type) );
        if ( !empty( $_results ) && ( $_defaultSchema != $schema ) )
        {
            foreach ( $_results as $_key => $_name )
            {
                $_results[$_key] = $schema . '.' . $_name;
            }
        }

        return $_results;
    }
}
