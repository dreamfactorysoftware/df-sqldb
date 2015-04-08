<?php
/**
 * This file is part of the DreamFactory Rave(tm)
 *
 * DreamFactory Rave(tm) <http://github.com/dreamfactorysoftware/rave>
 * Copyright 2012-2014 DreamFactory Software, Inc. <support@dreamfactory.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace DreamFactory\Rave\SqlDb\Resources;

use DreamFactory\Library\Utility\ArrayUtils;
use DreamFactory\Library\Utility\Inflector;
use DreamFactory\Rave\Exceptions\BadRequestException;
use DreamFactory\Rave\Exceptions\InternalServerErrorException;
use DreamFactory\Rave\Exceptions\NotFoundException;
use DreamFactory\Rave\Exceptions\RestException;
use DreamFactory\Rave\Resources\BaseDbSchemaResource;
use DreamFactory\Rave\SqlDb\Components\SqlDbResource;
use DreamFactory\Rave\SqlDbCore\Command;
use DreamFactory\Rave\SqlDbCore\Connection;
use DreamFactory\Rave\SqlDbCore\Schema\ColumnSchema;
use DreamFactory\Rave\SqlDbCore\Schema\TableSchema;
use DreamFactory\Rave\SqlDb\Services\SqlDb;
use DreamFactory\Rave\Utility\DbUtilities;

class Schema extends BaseDbSchemaResource
{
    //*************************************************************************
    //	Traits
    //*************************************************************************

    use SqlDbResource;

    //*************************************************************************
    //	Members
    //*************************************************************************

    //*************************************************************************
    //	Methods
    //*************************************************************************

    /**
     * {@inheritdoc}
     */
    public function listResources( $include_properties = null )
    {
        $refresh = $this->request->queryBool( 'refresh' );
        $_names = $this->dbConn->getSchema()->getTableNames( null, true, $refresh );

        if ( empty( $include_properties ) )
        {
            return array( 'resource' => $_names );
        }

        $_extras = DbUtilities::getSchemaExtrasForTables( $this->serviceId, $_names, false, 'table,label,plural' );

        $_tables = array();
        foreach ( $_names as $name )
        {
            $label = '';
            $plural = '';
            foreach ( $_extras as $each )
            {
                if ( 0 == strcasecmp( $name, ArrayUtils::get( $each, 'table', '' ) ) )
                {
                    $label = ArrayUtils::get( $each, 'label' );
                    $plural = ArrayUtils::get( $each, 'plural' );
                    break;
                }
            }

            if ( empty( $label ) )
            {
                $label = Inflector::camelize( $name, [ '_', '.' ], true );
            }

            if ( empty( $plural ) )
            {
                $plural = Inflector::pluralize( $label );
            }

            $_tables[] = array( 'name' => $name, 'label' => $label, 'plural' => $plural );
        }

        return $this->makeResourceList( $_tables, $include_properties, true );
    }

    /**
     * {@inheritdoc}
     */
    public function describeTable( $name, $refresh = false )
    {
        if ( empty( $name ) )
        {
            throw new BadRequestException( 'Table name can not be empty.' );
        }

        $this->correctTableName( $name );
        try
        {
            $table = $this->dbConn->getSchema()->getTable( $name, $refresh );
            if ( !$table )
            {
                throw new NotFoundException( "Table '$name' does not exist in the database." );
            }

            $extras = DbUtilities::getSchemaExtrasForTables( $this->serviceId, $name );
            $extras = DbUtilities::reformatFieldLabelArray( $extras );
            $labelInfo = ArrayUtils::get( $extras, '', array() );

            $label = ArrayUtils::get( $labelInfo, 'label', Inflector::camelize( $table->displayName, '_', true ) );
            $plural = ArrayUtils::get( $labelInfo, 'plural', Inflector::pluralize( $label ) );
            $name_field = ArrayUtils::get( $labelInfo, 'name_field' );

            $fields = array();
            foreach ( $table->columns as $column )
            {
                $_info = ArrayUtils::get( $extras, $column->name, array() );
                $fields[] = $this->describeFieldInternal( $column, $_info );
            }

            $_result = array(
                'name'        => $table->displayName,
                'label'       => $label,
                'plural'      => $plural,
                'primary_key' => $table->primaryKey,
                'name_field'  => $name_field,
                'field'       => $fields,
                'related'     => $table->references
            );
            $_result['access'] = $this->getPermissions( $table );

            return $_result;
        }
        catch ( RestException $ex )
        {
            throw $ex;
        }
        catch ( \Exception $ex )
        {
            throw new InternalServerErrorException( "Failed to query database schema.\n{$ex->getMessage()}" );
        }
    }

    /**
     * {@inheritdoc}
     */
    public function describeField( $table, $field, $refresh = false )
    {
        if ( empty( $table ) )
        {
            throw new BadRequestException( 'Table name can not be empty.' );
        }

        try
        {
            $_result = $this->describeTableFields( $table, $field );

            return ArrayUtils::get( $_result, 0 );
        }
        catch ( RestException $ex )
        {
            throw $ex;
        }
        catch ( \Exception $ex )
        {
            throw new InternalServerErrorException( "Error describing database table '$table' field '$field'.\n" . $ex->getMessage(), $ex->getCode() );
        }
    }

    /**
     * {@inheritdoc}
     */
    public function createTables( $tables, $check_exist = false, $return_schema = false )
    {
        $tables = DbUtilities::validateAsArray( $tables, null, true, 'There are no table sets in the request.' );

        // check for system tables and deny
        foreach ( $tables as $_table )
        {
            if ( null === ( $_name = ArrayUtils::get( $_table, 'name' ) ) )
            {
                throw new BadRequestException( "Table schema received does not have a valid name." );
            }
        }

        $_result = $this->updateTablesInternal( $tables );

        //  Any changes here should refresh cached schema
        static::refreshCachedTables( $this->dbConn );

        if ( $return_schema )
        {
            return $this->describeTables( $tables );
        }

        return $_result;
    }

    /**
     * {@inheritdoc}
     */
    public function createTable( $table, $properties = array(), $check_exist = false, $return_schema = false )
    {
        $properties = ArrayUtils::clean( $properties );
        $properties['name'] = $table;

        $_tables = DbUtilities::validateAsArray( $properties, null, true, 'Bad data format in request.' );
        $_result = $this->updateTablesInternal( $_tables );
        $_result = ArrayUtils::get( $_result, 0, array() );

        //  Any changes here should refresh cached schema
        static::refreshCachedTables( $this->dbConn );

        if ( $return_schema )
        {
            return $this->describeTable( $table );
        }

        return $_result;
    }

    /**
     * {@inheritdoc}
     */
    public function createField( $table, $field, $properties = array(), $check_exist = false, $return_schema = false )
    {
        $properties = ArrayUtils::clean( $properties );
        $properties['name'] = $field;

        $_fields = DbUtilities::validateAsArray( $properties, null, true, 'Bad data format in request.' );

        $_result = $this->updateFieldsInternal( $table, $_fields );

        //  Any changes here should refresh cached schema
        static::refreshCachedTables( $this->dbConn );

        if ( $return_schema )
        {
            return $this->describeField( $table, $field );
        }

        return $_result;
    }

    /**
     * {@inheritdoc}
     */
    public function updateTables( $tables, $allow_delete_fields = false, $return_schema = false )
    {
        $tables = DbUtilities::validateAsArray( $tables, null, true, 'There are no table sets in the request.' );

        foreach ( $tables as $_table )
        {
            if ( null === ( $_name = ArrayUtils::get( $_table, 'name' ) ) )
            {
                throw new BadRequestException( "Table schema received does not have a valid name." );
            }
        }

        $_result = $this->updateTablesInternal( $tables, true, $allow_delete_fields );

        //  Any changes here should refresh cached schema
        static::refreshCachedTables( $this->dbConn );

        if ( $return_schema )
        {
            return $this->describeTables( $tables );
        }

        return $_result;
    }

    /**
     * {@inheritdoc}
     */
    public function updateTable( $table, $properties, $allow_delete_fields = false, $return_schema = false )
    {
        $properties = ArrayUtils::clean( $properties );
        $properties['name'] = $table;

        $_tables = DbUtilities::validateAsArray( $properties, null, true, 'Bad data format in request.' );

        $_result = $this->updateTablesInternal( $_tables, true, $allow_delete_fields );
        $_result = ArrayUtils::get( $_result, 0, array() );

        //  Any changes here should refresh cached schema
        static::refreshCachedTables( $this->dbConn );

        if ( $return_schema )
        {
            return $this->describeTable( $table );
        }

        return $_result;
    }

    /**
     * {@inheritdoc}
     */
    public function updateField( $table, $field, $properties = array(), $allow_delete_parts = false, $return_schema = false )
    {
        if ( empty( $table ) )
        {
            throw new BadRequestException( 'Table name can not be empty.' );
        }

        $properties = ArrayUtils::clean( $properties );
        $properties['name'] = $field;

        $_fields = DbUtilities::validateAsArray( $properties, null, true, 'Bad data format in request.' );

        $_result = $this->updateFieldsInternal( $table, $_fields, true );

        //  Any changes here should refresh cached schema
        static::refreshCachedTables( $this->dbConn );

        if ( $return_schema )
        {
            return $this->describeField( $table, $field );
        }

        return $_result;
    }

    /**
     * {@inheritdoc}
     */
    public function deleteTable( $table, $check_empty = false )
    {
        if ( empty( $table ) )
        {
            throw new BadRequestException( 'Table name can not be empty.' );
        }

        //  Any changes here should refresh cached schema
        static::refreshCachedTables( $this->dbConn );

        //  Does it exist
        if ( !$this->doesTableExist( $table ) )
        {
            throw new NotFoundException( 'Table "' . $table . '" not found.' );
        }
        try
        {
            $this->dbConn->createCommand()->dropTable( $table );
        }
        catch ( \Exception $_ex )
        {
            \Log::error( 'Exception dropping table: ' . $_ex->getMessage() );

            throw $_ex;
        }

        //  Any changes here should refresh cached schema
        static::refreshCachedTables( $this->dbConn );

        DbUtilities::removeSchemaExtrasForTables( $this->serviceId, $table );
    }

    /**
     * {@inheritdoc}
     */
    public function deleteField( $table, $field )
    {
        if ( empty( $table ) )
        {
            throw new BadRequestException( 'Table name can not be empty.' );
        }

        // does it already exist
        if ( !$this->doesTableExist( $table ) )
        {
            throw new NotFoundException( "A table with name '$table' does not exist in the database." );
        }
        try
        {
            $this->dbConn->createCommand()->dropColumn( $table, $field );
        }
        catch ( \Exception $ex )
        {
            error_log( $ex->getMessage() );
            throw $ex;
        }

        //  Any changes here should refresh cached schema
        static::refreshCachedTables( $this->dbConn );

        DbUtilities::removeSchemaExtrasForFields( $this->serviceId, $table, $field );
    }

    /**
     * @param string $name
     *
     * @throws BadRequestException
     * @throws NotFoundException
     * @return string
     */
    public function correctTableName( &$name )
    {
        if ( false !== ( $_table = $this->doesTableExist( $name, true ) ) )
        {
            $name = $_table;
        }
        else
        {
            throw new NotFoundException( 'Table "' . $name . '" does not exist in the database.' );
        }
    }

    /**
     * @param string $name       The name of the table to check
     * @param bool   $returnName If true, the table name is returned instead of TRUE
     *
     * @throws \InvalidArgumentException
     * @return bool
     */
    public function doesTableExist( $name, $returnName = false )
    {
        if ( empty( $name ) )
        {
            throw new \InvalidArgumentException( 'Table name cannot be empty.' );
        }

        //  Build the lower-cased table array
        $_tables = $this->dbConn->getSchema()->getTableNames();

        //	Search normal, return real name
        if ( false !== array_search( $name, $_tables ) )
        {
            return $returnName ? $name : true;
        }

        if ( false !== $_key = array_search( strtolower( $name ), array_map( 'strtolower', $_tables ) ) )
        {
            return $returnName ? $_tables[$_key] : true;
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
    public function describeTableFields( $table_name, $field_names = null, $refresh = false )
    {
        $this->correctTableName( $table_name );
        $_table = $this->dbConn->getSchema()->getTable( $table_name, $refresh );
        if ( !$_table )
        {
            throw new NotFoundException( "Table '$table_name' does not exist in the database." );
        }

        if ( !empty( $field_names ) )
        {
            $field_names = DbUtilities::validateAsArray( $field_names, ',', true, 'No valid field names given.' );
            $extras = DbUtilities::getSchemaExtrasForFields( $this->serviceId, $table_name, $field_names );
        }
        else
        {
            $extras = DbUtilities::getSchemaExtrasForTables( $this->serviceId, $table_name );
        }

        $extras = DbUtilities::reformatFieldLabelArray( $extras );

        $_out = array();
        try
        {
            foreach ( $_table->columns as $column )
            {

                if ( empty( $field_names ) || ( false !== array_search( $column->name, $field_names ) ) )
                {
                    $_info = ArrayUtils::get( $extras, $column->name, array() );
                    $_out[] = $this->describeFieldInternal( $column, $_info );
                }
            }
        }
        catch ( \Exception $ex )
        {
            throw new InternalServerErrorException( "Failed to query table field schema.\n{$ex->getMessage()}" );
        }

        if ( empty( $_out ) )
        {
            throw new NotFoundException( "No requested fields found in table '$table_name'." );
        }

        return $_out;
    }

    /**
     * @param                $parent_table
     *
     * @return array
     * @throws \Exception
     */
    public function describeTableRelated( $parent_table )
    {
        $_schema = $this->dbConn->getSchema()->getTable( $parent_table );
        $_related = array();

        // foreign refs point to relationships other tables have with this table
        foreach ( $_schema->foreignRefs as $_refs )
        {
            $_name = array();
            switch ( ArrayUtils::get( $_refs, 'type' ) )
            {
                case 'belongs_to':
                    $_name['name'] = ArrayUtils::get( $_refs, 'ref_table', '' ) . '_by_' . ArrayUtils::get( $_refs, 'field', '' );
                    break;
                case 'has_many':
                    $_name['name'] = Inflector::pluralize( ArrayUtils::get( $_refs, 'ref_table', '' ) ) . '_by_' . ArrayUtils::get( $_refs, 'ref_field', '' );
                    break;
                case 'many_many':
                    $_join = ArrayUtils::get( $_refs, 'join', '' );
                    $_join = substr( $_join, 0, strpos( $_join, '(' ) );
                    $_name['name'] = Inflector::pluralize( ArrayUtils::get( $_refs, 'ref_table', '' ) ) . '_by_' . $_join;
                    break;
            }
            $_related[] = $_name + $_refs;
        }

        return $_related;
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
    public function updateFieldsInternal( $table_name, $fields, $allow_update = false, $allow_delete = false )
    {
        if ( empty( $table_name ) )
        {
            throw new BadRequestException( "Table schema received does not have a valid name." );
        }

        // does it already exist
        if ( !$this->doesTableExist( $table_name ) )
        {
            throw new NotFoundException( "Update schema called on a table with name '$table_name' that does not exist in the database." );
        }

        $_schema = $this->describeTable( $table_name );

        try
        {
            $names = array();
            $results = $this->buildTableFields( $table_name, $fields, $_schema, $allow_update, $allow_delete );
            /** @var Command $command */
            $command = $this->dbConn->createCommand();
            $columns = ArrayUtils::get( $results, 'columns', array() );
            foreach ( $columns as $name => $definition )
            {
                $command->reset();
                $command->addColumn( $table_name, $name, $definition );
                $names[] = $name;
            }
            $columns = ArrayUtils::get( $results, 'alter_columns', array() );
            foreach ( $columns as $name => $definition )
            {
                $command->reset();
                $command->alterColumn( $table_name, $name, $definition );
                $names[] = $name;
            }
            $columns = ArrayUtils::get( $results, 'drop_columns', array() );
            foreach ( $columns as $name )
            {
                $command->reset();
                $command->dropColumn( $table_name, $name );
                $names[] = $name;
            }
            static::createFieldExtras( $results );

            $labels = ArrayUtils::get( $results, 'labels', null, true );
            if ( !empty( $labels ) )
            {
                DbUtilities::setSchemaExtras( $this->serviceId, $labels );
            }

            return array( 'names' => $names );
        }
        catch ( \Exception $ex )
        {
            \Log::error( 'Exception updating fields: ' . $ex->getMessage() );
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
    public function updateTablesInternal( $tables, $allow_merge = false, $allow_delete = false, $rollback = false )
    {
        $tables = DbUtilities::validateAsArray( $tables, null, true, 'There are no table sets in the request.' );

        $_created = $_references = $_indexes = $_labels = $_out = array();
        $_count = 0;
        $_singleTable = ( 1 == count( $tables ) );

        foreach ( $tables as $_table )
        {
            try
            {
                if ( null === ( $_tableName = ArrayUtils::get( $_table, 'name' ) ) )
                {
                    throw new BadRequestException( 'Table name missing from schema.' );
                }

                //	Does it already exist
                if ( $this->doesTableExist( $_tableName ) )
                {
                    if ( !$allow_merge )
                    {
                        throw new BadRequestException( "A table with name '$_tableName' already exist in the database." );
                    }

                    \Log::debug( 'Schema update: ' . $_tableName );

                    $_oldSchema = $this->describeTable( $_tableName );

                    $_results = $this->updateTableInternal( $_tableName, $_table, $_oldSchema, $allow_delete );
                }
                else
                {
                    \Log::debug( 'Creating table: ' . $_tableName );

                    $_results = $this->createTableInternal( $_tableName, $_table, false );

                    if ( !$_singleTable && $rollback )
                    {
                        $_created[] = $_tableName;
                    }
                }

                $_labels = array_merge( $_labels, ArrayUtils::get( $_results, 'labels', array() ) );
                $_references = array_merge( $_references, ArrayUtils::get( $_results, 'references', array() ) );
                $_indexes = array_merge( $_indexes, ArrayUtils::get( $_results, 'indexes', array() ) );
                $_out[$_count] = array( 'name' => $_tableName );
            }
            catch ( \Exception $ex )
            {
                if ( $rollback || $_singleTable )
                {
                    //  Delete any created tables
                    throw $ex;
                }

                $_out[$_count] = array(
                    'error' => array(
                        'message' => $ex->getMessage(),
                        'code'    => $ex->getCode()
                    )
                );
            }

            $_count++;
        }

        $_results = array( 'references' => $_references, 'indexes' => $_indexes );
        static::createFieldExtras( $_results );

        if ( !empty( $_labels ) )
        {
            DbUtilities::setSchemaExtras( $this->serviceId, $_labels );
        }

        return $_out;
    }

    /**
     * @param $type
     *
     * @return int | null
     */
    public static function determinePdoBindingType( $type )
    {
        switch ( $type )
        {
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
     * @param ColumnSchema $column
     * @param array           $label_info
     *
     * @throws \Exception
     * @return array
     */
    protected static function describeFieldInternal( $column, $label_info )
    {
        $label = ArrayUtils::get( $label_info, 'label', Inflector::camelize( $column->name, '_', true ) );
        $validation = json_decode( ArrayUtils::get( $label_info, 'validation' ), true );
        if ( !empty( $validation ) && is_string( $validation ) )
        {
            // backwards compatible with old strings
            $validation = array_map( 'trim', explode( ',', $validation ) );
            $validation = array_flip( $validation );
        }
        $picklist = ArrayUtils::get( $label_info, 'picklist' );
        $picklist = ( !empty( $picklist ) ) ? explode( "\r", $picklist ) : array();

        return array(
            'name'               => $column->name,
            'label'              => $label,
            'type'               => static::determineDfType( $column, $label_info ),
            'db_type'            => $column->dbType,
            'php_type'           => $column->phpType,
            'pdo_type'           => $column->pdoType,
            'length'             => $column->size,
            'precision'          => $column->precision,
            'scale'              => $column->scale,
            'default'            => $column->defaultValue,
            'required'           => static::determineRequired( $column, $validation ),
            'allow_null'         => $column->allowNull,
            'fixed_length'       => $column->fixedLength,
            'supports_multibyte' => $column->supportsMultibyte,
            'auto_increment'     => $column->autoIncrement,
            'is_primary_key'     => $column->isPrimaryKey,
            'is_unique'          => $column->isUnique,
            'is_index'           => $column->isIndex,
            'is_foreign_key'     => $column->isForeignKey,
            'ref_table'          => $column->refTable,
            'ref_fields'         => $column->refFields,
            'validation'         => $validation,
            'value'              => $picklist
        );
    }

    /**
     * @param            $column
     * @param null|array $label_info
     *
     * @return string
     */
    protected static function determineDfType( $column, $label_info = null )
    {
        switch ( $column->type )
        {
            case 'integer':
                if ( isset( $label_info['user_id_on_update'] ) )
                {
                    return 'user_id_on_' . ( ArrayUtils::getBool( $label_info, 'user_id_on_update' ) ? 'update' : 'create' );
                }

                if ( null !== ArrayUtils::get( $label_info, 'user_id' ) )
                {
                    return 'user_id';
                }
                break;

            case 'timestamp':
                if ( isset( $label_info['timestamp_on_update'] ) )
                {
                    return 'timestamp_on_' . ( ArrayUtils::getBool( $label_info, 'timestamp_on_update' ) ? 'update' : 'create' );
                }
                break;
        }

        return $column->type;
    }

    /**
     * @param            $column
     * @param array|null $validations
     *
     * @return bool
     */
    protected static function determineRequired( $column, $validations = null )
    {
        if ( ( 1 == $column->allowNull ) || ( isset( $column->defaultValue ) ) || ( 1 == $column->autoIncrement ) )
        {
            return false;
        }

        if ( is_array( $validations ) && isset( $validations['api_read_only'] ) )
        {
            return false;
        }

        return true;
    }

    /**
     * @param string              $table_name
     * @param array               $fields
     * @param null|TableSchema $schema
     * @param bool                $allow_update
     * @param bool                $allow_delete
     *
     * @throws \Exception
     * @return string
     */
    protected function buildTableFields( $table_name, $fields, $schema = null, $allow_update = false, $allow_delete = false )
    {
        $fields = DbUtilities::validateAsArray( $fields, null, true, "No valid fields exist in the received table schema." );

        $_columns = array();
        $_alterColumns = array();
        $_references = array();
        $_indexes = array();
        $_labels = array();
        $_dropColumns = array();
        $_oldFields = array();
        $_extraCommands = array();
        if ( $schema )
        {
            foreach ( ArrayUtils::clean( ArrayUtils::get( $schema, 'field' ) ) as $_old )
            {
                $_old = array_change_key_case( $_old, CASE_LOWER );

                $_oldFields[ArrayUtils::get( $_old, 'name' )] = $_old;
            }
        }
        $_fields = array();
        foreach ( $fields as $_field )
        {
            $_field = array_change_key_case( $_field, CASE_LOWER );

            $_fields[ArrayUtils::get( $_field, 'name' )] = $_field;
        }

        if ( $allow_delete && !empty( $_oldFields ) )
        {
            // check for columns to drop
            foreach ( $_oldFields as $_oldName => $_oldField )
            {
                if ( !isset( $_fields[$_oldName] ) )
                {
                    $_dropColumns[] = $_oldName;
                }
            }
        }

        foreach ( $_fields as $_name => $_field )
        {
            if ( empty( $_name ) )
            {
                throw new BadRequestException( "Invalid schema detected - no name element." );
            }

            $_isAlter = isset( $_oldFields[$_name] );
            if ( $_isAlter && !$allow_update )
            {
                throw new BadRequestException( "Field '$_name' already exists in table '$table_name'." );
            }

            $_oldField = ( $_isAlter ) ? $_oldFields[$_name] : array();
            $_oldForeignKey = ArrayUtils::get( $_oldField, 'is_foreign_key', false );
            $_temp = array();

            // if same as old, don't bother
            if ( !empty( $_oldField ) )
            {
                $_same = true;

                foreach ( $_field as $_key => $_value )
                {
                    switch ( strtolower( $_key ) )
                    {
                        case 'label':
                        case 'value':
                        case 'validation':
                            break;
                        default:
                            if ( isset( $_oldField[$_key] ) ) // could be extra stuff from client
                            {
                                if ( $_value != $_oldField[$_key] )
                                {
                                    $_same = false;
                                    break 2;
                                }
                            }
                            break;
                    }
                }

                if ( $_same )
                {
                    continue;
                }
            }

            $_type = strtolower( ArrayUtils::get( $_field, 'type', '' ) );

            switch ( $_type )
            {
                case 'user_id':
                    $_field['type'] = 'int';
                    $_temp['user_id'] = true;
                    break;
                case 'user_id_on_create':
                    $_field['type'] = 'int';
                    $_temp['user_id_on_update'] = false;
                    break;
                case 'user_id_on_update':
                    $_field['type'] = 'int';
                    $_temp['user_id_on_update'] = true;
                    break;
                case 'timestamp_on_create':
                    $_temp['timestamp_on_update'] = false;
                    break;
                case 'timestamp_on_update':
                    $_temp['timestamp_on_update'] = true;
                    break;
                case 'id':
                case 'pk':
                    $pkExtras = $this->dbConn->getSchema()->getPrimaryKeyCommands( $table_name, $_name );
                    $_extraCommands = array_merge( $_extraCommands, $pkExtras );
                    break;

            }

            if ( ( 'reference' == $_type ) || ArrayUtils::getBool( $_field, 'is_foreign_key' ) )
            {
                // special case for references because the table referenced may not be created yet
                $refTable = ArrayUtils::get( $_field, 'ref_table' );
                if ( empty( $refTable ) )
                {
                    throw new BadRequestException( "Invalid schema detected - no table element for reference type of $_name." );
                }
                $refColumns = ArrayUtils::get( $_field, 'ref_fields', 'id' );
                $refOnDelete = ArrayUtils::get( $_field, 'ref_on_delete' );
                $refOnUpdate = ArrayUtils::get( $_field, 'ref_on_update' );

                // will get to it later, $refTable may not be there
                $_keyName = $this->dbConn->getSchema()->makeConstraintName( 'fk', $table_name, $_name );
                if ( !$_isAlter || !$_oldForeignKey )
                {
                    $_references[] = array(
                        'name'       => $_keyName,
                        'table'      => $table_name,
                        'column'     => $_name,
                        'ref_table'  => $refTable,
                        'ref_fields' => $refColumns,
                        'delete'     => $refOnDelete,
                        'update'     => $refOnUpdate
                    );
                }
            }

            // regardless of type
            if ( ArrayUtils::getBool( $_field, 'is_unique' ) )
            {
                // will get to it later, create after table built
                $_keyName = $this->dbConn->getSchema()->makeConstraintName( 'undx', $table_name, $_name );
                $_indexes[] = array(
                    'name'   => $_keyName,
                    'table'  => $table_name,
                    'column' => $_name,
                    'unique' => true,
                    'drop'   => $_isAlter
                );
            }
            elseif ( ArrayUtils::get( $_field, 'is_index' ) )
            {
                // will get to it later, create after table built
                $_keyName = $this->dbConn->getSchema()->makeConstraintName( 'ndx', $table_name, $_name );
                $_indexes[] = array(
                    'name'   => $_keyName,
                    'table'  => $table_name,
                    'column' => $_name,
                    'drop'   => $_isAlter
                );
            }

            $_values = ArrayUtils::get( $_field, 'value' );
            if ( empty( $_values ) )
            {
                $_values = ArrayUtils::getDeep( $_field, 'values', 'value', array() );
            }
            if ( !is_array( $_values ) )
            {
                $_values = array_map( 'trim', explode( ',', trim( $_values, ',' ) ) );
            }
            if ( !empty( $_values ) && ( $_values != ArrayUtils::get( $_oldField, 'value' ) ) )
            {
                $_picklist = '';
                foreach ( $_values as $_value )
                {
                    if ( !empty( $_picklist ) )
                    {
                        $_picklist .= "\r";
                    }
                    $_picklist .= $_value;
                }
                if ( !empty( $_picklist ) )
                {
                    $_temp['picklist'] = $_picklist;
                }
            }

            // labels
            $_label = ArrayUtils::get( $_field, 'label' );
            if ( !empty( $_label ) && ( $_label != ArrayUtils::get( $_oldField, 'label' ) ) )
            {
                $_temp['label'] = $_label;
            }

            $_validation = ArrayUtils::get( $_field, 'validation' );
            if ( !empty( $_validation ) && ( $_validation != ArrayUtils::get( $_oldField, 'validation' ) ) )
            {
                $_temp['validation'] = json_encode( $_validation );
            }

            if ( !empty( $_temp ) )
            {
                $_temp['table'] = $table_name;
                $_temp['field'] = $_name;
                $_labels[] = $_temp;
            }

            if ( $_isAlter )
            {
                $_alterColumns[$_name] = $_field;
            }
            else
            {
                $_columns[$_name] = $_field;
            }

        }

        return array(
            'columns'       => $_columns,
            'alter_columns' => $_alterColumns,
            'drop_columns'  => $_dropColumns,
            'references'    => $_references,
            'indexes'       => $_indexes,
            'labels'        => $_labels,
            'extras'        => $_extraCommands
        );
    }

    /**
     * @param array $extras
     *
     * @return array
     */
    protected function createFieldExtras( $extras )
    {
        /** @var Command $command */
        $command = $this->dbConn->createCommand();
        $references = ArrayUtils::get( $extras, 'references', array() );
        if ( !empty( $references ) )
        {
            foreach ( $references as $reference )
            {
                $command->reset();
                $name = $reference['name'];
                $table = $reference['table'];
                $drop = ArrayUtils::getBool( $reference, 'drop' );
                if ( $drop )
                {
                    try
                    {
                        $command->dropForeignKey( $name, $table );
                    }
                    catch ( \Exception $ex )
                    {
                        \Log::debug( $ex->getMessage() );
                    }
                }
                // add new reference
                $refTable = ArrayUtils::get( $reference, 'ref_table' );
                if ( !empty( $refTable ) )
                {
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
        $indexes = ArrayUtils::get( $extras, 'indexes', array() );
        if ( !empty( $indexes ) )
        {
            foreach ( $indexes as $index )
            {
                $command->reset();
                $name = $index['name'];
                $table = $index['table'];
                $drop = ArrayUtils::getBool( $index, 'drop' );
                if ( $drop )
                {
                    try
                    {
                        $command->dropIndex( $name, $table );
                    }
                    catch ( \Exception $ex )
                    {
                        \Log::debug( $ex->getMessage() );
                    }
                }
                $unique = ArrayUtils::getBool( $index, 'unique' );

                /** @noinspection PhpUnusedLocalVariableInspection */
                $rows = $command->createIndex( $name, $table, $index['column'], $unique );
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
    protected function createTableInternal( $table_name, $data, $checkExist = true )
    {
        if ( empty( $table_name ) )
        {
            throw new BadRequestException( "Table schema received does not have a valid name." );
        }

        // does it already exist
        if ( true === $checkExist && $this->doesTableExist( $table_name ) )
        {
            throw new BadRequestException( "A table with name '$table_name' already exist in the database." );
        }

        $data = array_change_key_case( $data, CASE_LOWER );
        $_fields = ArrayUtils::get( $data, 'field' );
        try
        {
            $_results = $this->buildTableFields( $table_name, $_fields );
            $_columns = ArrayUtils::get( $_results, 'columns' );

            if ( empty( $_columns ) )
            {
                throw new BadRequestException( "No valid fields exist in the received table schema." );
            }

            $this->dbConn->createCommand()->createTable( $table_name, $_columns );

            $_extras = ArrayUtils::get( $_results, 'extras', null, true );
            if ( !empty( $_extras ) )
            {
                foreach ( $_extras as $_extraCommand )
                {
                    try
                    {
                        $this->dbConn->createCommand()->setText( $_extraCommand )->execute();
                    }
                    catch ( \Exception $_ex )
                    {
                        // oh well, we tried.
                    }
                }
            }

            $_labels = ArrayUtils::get( $_results, 'labels', array() );
            // add table labels
            $_label = ArrayUtils::get( $data, 'label' );
            $_plural = ArrayUtils::get( $data, 'plural' );
            if ( !empty( $_label ) || !empty( $_plural ) )
            {
                $_labels[] = array(
                    'table'  => $table_name,
                    'field'  => '',
                    'label'  => $_label,
                    'plural' => $_plural
                );
            }
            $_results['labels'] = $_labels;

            return $_results;
        }
        catch ( \Exception $ex )
        {
            \Log::error( 'Exception creating table: ' . $ex->getMessage() );
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
    protected function updateTableInternal( $table_name, $data, $old_schema, $allow_delete = false )
    {
        if ( empty( $table_name ) )
        {
            throw new BadRequestException( "Table schema received does not have a valid name." );
        }
        // does it already exist
        if ( !$this->doesTableExist( $table_name ) )
        {
            throw new BadRequestException( "Update schema called on a table with name '$table_name' that does not exist in the database." );
        }

        //  Is there a name update
        $_newName = ArrayUtils::get( $data, 'new_name' );

        if ( !empty( $_newName ) )
        {
            // todo change table name, has issue with references
        }

        // update column types

        $_labels = array();
        $_references = array();
        $_indexes = array();
        $_fields = ArrayUtils::get( $data, 'field' );
        if ( !empty( $_fields ) )
        {
            try
            {
                /** @var Command $_command */
                $_command = $this->dbConn->createCommand();
                $_results = $this->buildTableFields( $table_name, $_fields, $old_schema, true, $allow_delete );
                $_columns = ArrayUtils::get( $_results, 'columns', array() );
                foreach ( $_columns as $_name => $_definition )
                {
                    $_command->reset();
                    $_command->addColumn( $table_name, $_name, $_definition );
                }
                $_columns = ArrayUtils::get( $_results, 'alter_columns', array() );
                foreach ( $_columns as $_name => $_definition )
                {
                    $_command->reset();
                    $_command->alterColumn( $table_name, $_name, $_definition );
                }
                $_columns = ArrayUtils::get( $_results, 'drop_columns', array() );
                foreach ( $_columns as $_name )
                {
                    $_command->reset();
                    $_command->dropColumn( $table_name, $_name );
                }
            }
            catch ( \Exception $ex )
            {
                \Log::error( 'Exception updating table: ' . $ex->getMessage() );
                throw $ex;
            }

            $_labels = ArrayUtils::get( $_results, 'labels', array() );
            $_references = ArrayUtils::get( $_results, 'references', array() );
            $_indexes = ArrayUtils::get( $_results, 'indexes', array() );
        }

        // add table labels
        $_label = ArrayUtils::get( $data, 'label' );
        $_plural = ArrayUtils::get( $data, 'plural' );
        if ( !is_null( $_label ) || !is_null( $_plural ) )
        {
            if ( ( $_label != ArrayUtils::get( $old_schema, 'label' ) ) && ( $_plural != ArrayUtils::get( $old_schema, 'plural' ) ) )
            {
                $_labels[] = array(
                    'table'  => $table_name,
                    'field'  => '',
                    'label'  => $_label,
                    'plural' => $_plural
                );
            }
        }

        $_results = array( 'references' => $_references, 'indexes' => $_indexes, 'labels' => $_labels );

        return $_results;
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

}