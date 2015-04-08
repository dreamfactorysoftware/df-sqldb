<?php
/**
 * CMysqlSchema class file.
 *
 * @author    Qiang Xue <qiang.xue@gmail.com>
 * @link      http://www.yiiframework.com/
 * @copyright Copyright &copy; 2008-2011 Yii Software LLC
 * @license   http://www.yiiframework.com/license/
 */
namespace DreamFactory\Rave\SqlDb\Driver\Schema\Mysql;

use DreamFactory\Library\Utility\ArrayUtils;
use DreamFactory\Library\Utility\Scalar;
use DreamFactory\Rave\SqlDb\Driver\Schema\CDbSchema;
use DreamFactory\Rave\SqlDb\Driver\Schema\CDbTableSchema;
use DreamFactory\Rave\SqlDb\Driver\Schema\CDbColumnSchema;
use DreamFactory\Rave\SqlDb\Driver\Schema\CDbCommandBuilder;

/**
 * CMysqlSchema is the class for retrieving metadata information from a MySQL database (version 4.1.x and 5.x).
 *
 * @author  Qiang Xue <qiang.xue@gmail.com>
 * @package system.db.schema.mysql
 * @since   1.0
 */
class CMysqlSchema extends CDbSchema
{
    //******************************************************************************
    //* Members
    //******************************************************************************

    /**
     * @type string
     */
    private $_defaultSchema;

    //******************************************************************************
    //* Methods
    //******************************************************************************

    protected function translateSimpleColumnTypes( array &$info )
    {
        // override this in each schema class
        $type = ArrayUtils::get( $info, 'type' );
        switch ( $type )
        {
            // some types need massaging, some need other required properties
            case 'pk':
            case 'id':
                $info['type'] = 'int';
                $info['type_extras'] = '(11)';
                $info['allow_null'] = false;
                $info['auto_increment'] = true;
                $info['is_primary_key'] = true;
                break;

            case 'fk':
            case 'reference':
                $info['type'] = 'int';
                $info['type_extras'] = '(11)';
                $info['is_foreign_key'] = true;
                // check foreign tables
                break;

            case 'timestamp_on_create':
            case 'timestamp_on_update':
                $info['type'] = 'timestamp';
                $default = ArrayUtils::get( $info, 'default' );
                if ( !isset( $default ) )
                {
                    $default = 'CURRENT_TIMESTAMP';
                    if ( 'timestamp_on_update' === $type )
                    {
                        $default .= ' ON UPDATE CURRENT_TIMESTAMP';
                    }
                    $info['default'] = array( 'expression' => $default );
                }
                break;

            case 'boolean':
                $info['type'] = 'tinyint';
                $info['type_extras'] = '(1)';
                $default = ArrayUtils::get( $info, 'default' );
                if ( isset( $default ) )
                {
                    // convert to bit 0 or 1, where necessary
                    $info['default'] = ( Scalar::boolval( $default ) ) ? 1 : 0;
                }
                break;

            case 'money':
                $info['type'] = 'decimal';
                $info['type_extras'] = '(19,4)';
                break;

            case 'string':
                $fixed = ArrayUtils::getBool( $info, 'fixed_length' );
                $national = ArrayUtils::getBool( $info, 'supports_multibyte' );
                if ( $fixed )
                {
                    $info['type'] = ( $national ) ? 'nchar' : 'char';
                }
                elseif ( $national )
                {
                    $info['type'] = 'nvarchar';
                }
                else
                {
                    $info['type'] = 'varchar';
                }
                break;

            case 'binary':
                $fixed = ArrayUtils::getBool( $info, 'fixed_length' );
                $info['type'] = ( $fixed ) ? 'binary' : 'varbinary';
                break;
        }
    }

    protected function validateColumnSettings( array &$info )
    {
        // override this in each schema class
        $type = ArrayUtils::get( $info, 'type' );
        switch ( $type )
        {
            // some types need massaging, some need other required properties
            case 'bit':
            case 'tinyint':
            case 'smallint':
            case 'mediumint':
            case 'int':
            case 'bigint':
                if ( !isset( $info['type_extras'] ) )
                {
                    $length = ArrayUtils::get( $info, 'length', ArrayUtils::get( $info, 'precision' ) );
                    if ( !empty( $length ) )
                    {
                        $info['type_extras'] = "($length)"; // sets the viewable length
                    }
                }

                $default = ArrayUtils::get( $info, 'default' );
                if ( isset( $default ) && is_numeric( $default ) )
                {
                    $info['default'] = intval( $default );
                }
                break;

            case 'decimal':
            case 'numeric':
            case 'real':
            case 'float':
            case 'double':
                if ( !isset( $info['type_extras'] ) )
                {
                    $length = ArrayUtils::get( $info, 'length', ArrayUtils::get( $info, 'precision' ) );
                    if ( !empty( $length ) )
                    {
                        $scale = ArrayUtils::get( $info, 'scale', ArrayUtils::get( $info, 'decimals' ) );
                        $info['type_extras'] = ( !empty( $scale ) ) ? "($length,$scale)" : "($length)";
                    }
                }

                $default = ArrayUtils::get( $info, 'default' );
                if ( isset( $default ) && is_numeric( $default ) )
                {
                    $info['default'] = floatval( $default );
                }
                break;

            case 'char':
            case 'nchar':
            case 'binary':
                $length = ArrayUtils::get( $info, 'length', ArrayUtils::get( $info, 'size' ) );
                if ( isset( $length ) )
                {
                    $info['type_extras'] = "($length)";
                }
                break;

            case 'varchar':
            case 'nvarchar':
            case 'varbinary':
                $length = ArrayUtils::get( $info, 'length', ArrayUtils::get( $info, 'size' ) );
                if ( isset( $length ) )
                {
                    $info['type_extras'] = "($length)";
                }
                else // requires a max length
                {
                    $info['type_extras'] = '(' . static::DEFAULT_STRING_MAX_SIZE . ')';
                }
                break;

            case 'time':
            case 'timestamp':
            case 'datetime':
                $default = ArrayUtils::get( $info, 'default' );
                if ( '0000-00-00 00:00:00' == $default )
                {
                    // read back from MySQL has formatted zeros, can't send that back
                    $info['default'] = 0;
                }

                $length = ArrayUtils::get( $info, 'length', ArrayUtils::get( $info, 'size' ) );
                if ( isset( $length ) )
                {
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
    protected function buildColumnDefinition( array $info )
    {
        $type = ArrayUtils::get( $info, 'type' );
        $typeExtras = ArrayUtils::get( $info, 'type_extras' );

        $definition = $type . $typeExtras;

        $allowNull = ArrayUtils::getBool( $info, 'allow_null', true );
        $definition .= ( $allowNull ) ? ' NULL' : ' NOT NULL';

        $default = ArrayUtils::get( $info, 'default' );
        if ( isset( $default ) )
        {
            if ( is_array( $default ) )
            {
                if ( null !== $expression = ArrayUtils::get( $default, 'expression' ) )
                {
                    $definition .= ' DEFAULT ' . $expression;
                }
            }
            else
            {
                $default = $this->getDbConnection()->quoteValue( $default );
                $definition .= ' DEFAULT ' . $default;
            }
        }

        if ( ArrayUtils::getBool( $info, 'auto_increment', false ) )
        {
            $definition .= ' AUTO_INCREMENT';
        }

        $isUniqueKey = ArrayUtils::getBool( $info, 'is_unique', false );
        $isPrimaryKey = ArrayUtils::getBool( $info, 'is_primary_key', false );
        if ( $isPrimaryKey && $isUniqueKey )
        {
            throw new \Exception( 'Unique and Primary designations not allowed simultaneously.' );
        }
        if ( $isUniqueKey )
        {
            $definition .= ' UNIQUE KEY';
        }
        elseif ( $isPrimaryKey )
        {
            $definition .= ' PRIMARY KEY';
        }

        return $definition;
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
        return '`' . $name . '`';
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
        return '`' . $name . '`';
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
        return parent::compareTableNames( strtolower( $name1 ), strtolower( $name2 ) );
    }

    /**
     * Resets the sequence value of a table's primary key.
     * The sequence will be reset such that the primary key of the next new row inserted
     * will have the specified value or max value of a primary key plus one (i.e. sequence trimming).
     *
     * @param CDbTableSchema $table the table schema whose primary key sequence will be reset
     * @param integer|null   $value the value for the primary key of the next new row inserted.
     *                              If this is not set, the next new row's primary key will have the max value of a
     *                              primary key plus one (i.e. sequence trimming).
     *
     * @return int|void
     * @since 1.1
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
            $value = (int)$this->getDbConnection()->createCommand( 'SELECT MAX(`' . $table->primaryKey . '`) + 1 FROM ' . $table->rawName )->queryScalar();
        }

        $this->getDbConnection()->createCommand(
            <<<MYSQL
ALTER TABLE {$table->rawName} AUTO_INCREMENT = :value
MYSQL
        )->execute( array( ':value' => $value ) );
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
        $this->getDbConnection()->createCommand( 'SET FOREIGN_KEY_CHECKS=' . ( $check ? 1 : 0 ) )->execute();
    }

    /**
     * Loads the metadata for the specified table.
     *
     * @param string $name table name
     *
     * @return CDbTableSchema driver dependent table metadata. Null if the table does not exist.
     */
    protected function loadTable( $name )
    {
        $table = new CDbTableSchema;
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
     * @param CDbTableSchema $table the table instance
     * @param string            $name  the unquoted table name
     */
    protected function resolveTableNames( $table, $name )
    {
        $parts = explode( '.', str_replace( array( '`', '"' ), '', $name ) );
        if ( isset( $parts[1] ) )
        {
            $table->schemaName = $parts[0];
            $table->name = $parts[1];
            $table->rawName = $this->quoteTableName( $table->schemaName ) . '.' . $this->quoteTableName( $table->name );
        }
        else
        {
            $table->name = $parts[0];
            $table->rawName = $this->quoteTableName( $table->name );
        }
    }

    /**
     * Collects the table column metadata.
     *
     * @param CDbTableSchema $table the table metadata
     *
     * @return boolean whether the table exists in the database
     */
    protected function findColumns( $table )
    {
        $sql = 'SHOW FULL COLUMNS FROM ' . $table->rawName;
        try
        {
            $columns = $this->getDbConnection()->createCommand( $sql )->queryAll();
        }
        catch ( \Exception $e )
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
                    $table->primaryKey = array( $table->primaryKey, $c->name );
                }
                else
                {
                    $table->primaryKey[] = $c->name;
                }
                if ( $c->autoIncrement )
                {
                    $table->sequenceName = '';
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
        $c = new CMysqlColumnSchema;
        $c->name = $column['Field'];
        $c->rawName = $this->quoteColumnName( $c->name );
        $c->allowNull = $column['Null'] === 'YES';
        $c->isPrimaryKey = strpos( $column['Key'], 'PRI' ) !== false;
        $c->isUnique = strpos( $column['Key'], 'UNI' ) !== false;
        $c->isIndex = strpos( $column['Key'], 'MUL' ) !== false;
        $c->autoIncrement = strpos( strtolower( $column['Extra'] ), 'auto_increment' ) !== false;
        $c->dbType = $column['Type'];
        if ( isset( $column['Collation'] ) && !empty( $column['Collation'] ) )
        {
            $collation = $column['Collation'];
            if ( 0 === stripos( $collation, 'utf' ) || 0 === stripos( $collation, 'ucs' ) )
            {
                $c->supportsMultibyte = true;
            }
        }
        if ( isset( $column['Comment'] ) )
        {
            $c->comment = $column['Comment'];
        }
        $c->extractLimit( $column['Type'] );
        $c->extractFixedLength( $column['Type'] );
//        $c->extractMultiByteSupport( $column['Type'] );
        $c->extractType( $column['Type'] );

        if ( $c->dbType === 'timestamp' && ( 0 === strcasecmp( strval($column['Default']), 'CURRENT_TIMESTAMP' ) ) )
        {
            if ( 0 === strcasecmp( strval($column['Extra']), 'on update CURRENT_TIMESTAMP' ) )
            {
                $c->defaultValue = array( 'expression' => 'CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP' );
                $c->type = 'timestamp_on_update';
            }
            else
            {
                $c->defaultValue = array( 'expression' => 'CURRENT_TIMESTAMP' );
                $c->type = 'timestamp_on_create';
            }
        }
        else
        {
            $c->extractDefault( $column['Default'] );
        }

        return $c;
    }

    /**
     * @return float server version.
     */
    protected function getServerVersion()
    {
        $version = $this->getDbConnection()->getAttribute( \PDO::ATTR_SERVER_VERSION );
        $digits = array();
        preg_match( '/(\d+)\.(\d+)\.(\d+)/', $version, $digits );

        return floatval( $digits[1] . '.' . $digits[2] . $digits[3] );
    }

    /**
     * Collects the foreign key column details for the given table.
     * Also, collects the foreign tables and columns that reference the given table.
     *
     * @param CDbTableSchema $table the table metadata
     */
    protected function findConstraints( $table )
    {
        $defaultSchema = $this->getDefaultSchema();
        $tableSchema = ( !empty( $table->schemaName ) ) ? $table->schemaName : $this->getDefaultSchema();
        $columns = array();
        foreach ( $this->getSchemaNames() as $schema )
        {
            $sql = <<<MYSQL
SELECT table_schema, table_name, column_name, referenced_table_schema, referenced_table_name, referenced_column_name
FROM information_schema.KEY_COLUMN_USAGE WHERE referenced_table_name IS NOT NULL AND table_schema = '$schema';
MYSQL;

            $columns = array_merge( $columns, $this->getDbConnection()->createCommand( $sql )->queryAll() );
        }

        $columns2 = $columns;
        foreach ( $columns as $key => $column )
        {
            $ts = $column['table_schema'];
            $tn = $column['table_name'];
            $cn = $column['column_name'];
            $rts = $column['referenced_table_schema'];
            $rtn = $column['referenced_table_name'];
            $rcn = $column['referenced_column_name'];
            if ( ( 0 == strcasecmp( $tn, $table->name ) ) && ( 0 == strcasecmp( $ts, $tableSchema ) ) )
            {
                $name = ( $rts == $defaultSchema ) ? $rtn : $rts . '.' . $rtn;

                $table->foreignKeys[$cn] = array( $name, $rcn );
                if ( isset( $table->columns[$cn] ) )
                {
                    $table->columns[$cn]->isForeignKey = true;
                    $table->columns[$cn]->refTable = $name;
                    $table->columns[$cn]->refFields = $rcn;
                    if ('integer' === $table->columns[$cn]->type)
                    {
                        $table->columns[$cn]->type = 'reference';
                    }
                }

                // Add it to our foreign references as well
                $table->foreignRefs[] = array(
                    'type'      => 'belongs_to',
                    'ref_table' => $name,
                    'ref_field' => $rcn,
                    'field'     => $cn
                );
            }
            elseif ( ( 0 == strcasecmp( $rtn, $table->name ) ) && ( 0 == strcasecmp( $rts, $tableSchema ) ) )
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
                        $ts2 = $column2['table_schema'];
                        $tn2 = $column2['table_name'];
                        $cn2 = $column2['column_name'];
                        if ( ( 0 == strcasecmp( $ts2, $ts ) ) && ( 0 == strcasecmp( $tn2, $tn ) )
                        )
                        {
                            $rts2 = $column2['referenced_table_schema'];
                            $rtn2 = $column2['referenced_table_name'];
                            $rcn2 = $column2['referenced_column_name'];
                            if ( ( 0 != strcasecmp( $rts2, $tableSchema ) ) || ( 0 != strcasecmp( $rtn2, $table->name ) )
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

    /**
     * Returns all non-system database/schema names on the server
     *
     * @return array|void
     */
    protected function findSchemaNames()
    {
        $sql = <<<MYSQL
SHOW DATABASES WHERE `Database` NOT IN ('information_schema','mysql','performance_schema','phpmyadmin')
MYSQL;

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
        $defaultSchema = $this->getDefaultSchema();

        $sql = 'SHOW FULL TABLES';

        if ( !empty( $schema ) )
        {
            $sql .= ' FROM ' . $this->quoteTableName( $schema );
        }

        if ( !$include_views )
        {
            $sql .= " WHERE TABLE_TYPE = 'BASE TABLE'";
        }

        $names = $this->getDbConnection()->createCommand( $sql )->queryColumn();

        if ( !empty( $schema ) && $defaultSchema != $schema )
        {
            foreach ( $names as &$name )
            {
                $name = $schema . '.' . $name;
            }
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
     * @throws \Exception
     * @return mixed
     */
    public function callProcedure( $name, &$params )
    {
        $name = $this->getDbConnection()->quoteTableName( $name );
        $_paramStr = '';
        $_pre = '';
        $_post = '';
        $_bindings = array();
        foreach ( $params as $_key => $_param )
        {
            $_pName = ( isset( $_param['name'] ) && !empty( $_param['name'] ) ) ? $_param['name'] : "p$_key";
            $_pValue = isset( $_param['value'] ) ? $_param['value'] : null;

            if ( !empty( $_paramStr ) )
            {
                $_paramStr .= ', ';
            }

            switch ( strtoupper( strval( isset( $_param['param_type'] ) ? $_param['param_type'] : 'IN' ) ) )
            {
                case 'INOUT':
                    // not using binding for out or inout params here due to earlier (<5.5.3) mysql library bug
                    // since binding isn't working, set the values via statements, get the values via select
                    $_pre .= "SET @$_pName = $_pValue; ";
                    $_post .= ( empty( $_post ) ) ? "SELECT @$_pName" : ", @$_pName";
                    $_paramStr .= "@$_pName";
                    break;

                case 'OUT':
                    // not using binding for out or inout params here due to earlier (<5.5.3) mysql library bug
                    // since binding isn't working, get the values via select
                    $_post .= ( empty( $_post ) ) ? "SELECT @$_pName" : ", @$_pName";
                    $_paramStr .= "@$_pName";
                    break;

                default:
                    $_bindings[":$_pName"] = $_pValue;
                    $_paramStr .= ":$_pName";
                    break;
            }
        }

        !empty( $_pre ) && $this->getDbConnection()->createCommand( $_pre )->execute();

        $_sql = "CALL $name($_paramStr);";
        $_command = $this->getDbConnection()->createCommand( $_sql );

        // do binding
        $_command->bindValues( $_bindings );

        // Move to the next result and get results
        $_reader = $_command->query();
        $_result = $_reader->readAll();
        if ( $_reader->nextResult() )
        {
            // more data coming, make room
            $_result = array( $_result );
            try
            {
                do
                {
                    $_result[] = $_reader->readAll();
                }
                while ( $_reader->nextResult() );
            }
            catch ( \Exception $ex )
            {
                // mysql via pdo has issue of nextRowSet returning true one too many times
                if ( false !== strpos( $ex->getMessage(), 'General Error' ) )
                {
                    throw $ex;
                }

                // if there is only one data set, just return it
                if ( 1 == count( $_result ) )
                {
                    $_result = $_result[0];
                }
            }
        }

        if ( !empty( $_post ) )
        {
            $_out = $this->getDbConnection()->createCommand( $_post . ';' )->queryRow();
            foreach ( $params as $_key => &$_param )
            {
                $_pName = '@' . $_param['name'];
                switch ( strtoupper( strval( isset( $_param['param_type'] ) ? $_param['param_type'] : 'IN' ) ) )
                {
                    case 'INOUT':
                    case 'OUT':
                        if ( isset( $_out, $_out[$_pName] ) )
                        {
                            $_param['value'] = $_out[$_pName];
                        }
                        break;
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
     * @throws \Exception
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
            $_result = array( $_result );
            try
            {
                do
                {
                    $_result[] = $_reader->readAll();
                }
                while ( $_reader->nextResult() );
            }
            catch ( \Exception $ex )
            {
                // mysql via pdo has issue of nextRowSet returning true one too many times
                if ( false !== strpos( $ex->getMessage(), 'General Error' ) )
                {
                    throw $ex;
                }

                // if there is only one data set, just return it
                if ( 1 == count( $_result ) )
                {
                    $_result = $_result[0];
                }
            }
        }

        return $_result;
    }

    /**
     * Creates a command builder for the database.
     * This method overrides parent implementation in order to create a MySQL specific command builder
     *
     * @return CDbCommandBuilder command builder instance
     * @since 1.1.13
     */
    protected function createCommandBuilder()
    {
        return new CMysqlCommandBuilder( $this );
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
     * @since 1.1.6
     */
    public function renameColumn( $table, $name, $newName )
    {
        $db = $this->getDbConnection();
        $row = $db->createCommand( 'SHOW CREATE TABLE ' . $db->quoteTableName( $table ) )->queryRow();

        if ( $row === false )
        {
            throw new \Exception( "Unable to find '$name' in table '$table'." );
        }

        if ( isset( $row['Create Table'] ) )
        {
            $sql = $row['Create Table'];
        }
        else
        {
            $row = array_values( $row );
            $sql = $row[1];
        }

        $table = $db->quoteTableName( $table );
        $name = $db->quoteColumnName( $name );
        $newName = $db->quoteColumnName( $newName );

        if ( preg_match_all( '/^\s*[`"](.*?)[`"]\s+(.*?),?$/m', $sql, $matches ) )
        {
            foreach ( $matches[1] as $i => $c )
            {
                if ( $c === $name )
                {
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
     * @param string $name  the name of the foreign key constraint to be dropped. The name will be properly quoted by the method.
     * @param string $table the table whose foreign is to be dropped. The name will be properly quoted by the method.
     *
     * @return string the SQL statement for dropping a foreign key constraint.
     * @since 1.1.6
     */
    public function dropForeignKey( $name, $table )
    {
        return 'ALTER TABLE ' . $this->quoteTableName( $table ) . ' DROP FOREIGN KEY ' . $this->quoteColumnName( $name );
    }

    /**
     * Builds a SQL statement for removing a primary key constraint to an existing table.
     *
     * @param string $name  the name of the primary key constraint to be removed.
     * @param string $table the table that the primary key constraint will be removed from.
     *
     * @return string the SQL statement for removing a primary key constraint from an existing table.
     * @since 1.1.13
     */
    public function dropPrimaryKey( $name, $table )
    {
        return 'ALTER TABLE ' . $this->quoteTableName( $table ) . ' DROP PRIMARY KEY';
    }

    /**
     * Builds a SQL statement for adding a primary key constraint to a table.
     *
     * @param string       $name    not used in the MySQL syntax, the primary key is always called PRIMARY and is reserved.
     * @param string       $table   the table that the primary key constraint will be added to.
     * @param string|array $columns comma separated string or array of columns that the primary key will consist of.
     *
     * @return string the SQL statement for adding a primary key constraint to an existing table.
     * @since 1.1.14
     */
    public function addPrimaryKey( $name, $table, $columns )
    {
        if ( is_string( $columns ) )
        {
            $columns = preg_split( '/\s*,\s*/', $columns, -1, PREG_SPLIT_NO_EMPTY );
        }

        foreach ( $columns as $i => $col )
        {
            $columns[$i] = $this->quoteColumnName( $col );
        }

        return 'ALTER TABLE ' . $this->quoteTableName( $table ) . ' ADD PRIMARY KEY (' . implode( ', ', $columns ) . ' )';
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
    protected function _findRoutines( $type, $schema = '' )
    {
        $defaultSchema = $this->getDefaultSchema();
        $type = trim( strtoupper( $type ) );

        if ( $type != 'PROCEDURE' && $type != 'FUNCTION' )
        {
            throw new \InvalidArgumentException( 'The type "' . $type . '" is invalid.' );
        }

        $_select = ( empty( $schema ) || ( $defaultSchema == $schema ) ) ? 'ROUTINE_NAME' : "CONCAT('" . $schema . "','.',ROUTINE_NAME) as ROUTINE_NAME";
        $_schema = !empty( $schema ) ? " AND ROUTINE_SCHEMA = '" . $schema . "'" : null;

        $_sql = <<<MYSQL
SELECT
    {$_select}
FROM
    information_schema.ROUTINES
WHERE
    ROUTINE_TYPE = :routine_type
    {$_schema}
MYSQL;

        return $this->getDbConnection()->createCommand( $_sql )->queryColumn( array( ':routine_type' => $type ) );
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
        if ( empty( $this->_defaultSchema ) )
        {
            $sql = <<<MYSQL
SELECT DATABASE() FROM DUAL
MYSQL;

            $current = $this->getDbConnection()->createCommand( $sql )->queryScalar();
            $this->setDefaultSchema( $current );
        }

        return $this->_defaultSchema;
    }

    public function parseValueForSet( $value, $field_info )
    {
        $_type = ArrayUtils::get( $field_info, 'type' );

        switch ( $_type )
        {
            case 'boolean':
                $value = ( Scalar::boolval( $value ) ? 1 : 0 );
                break;
        }

        return $value;
    }
}
