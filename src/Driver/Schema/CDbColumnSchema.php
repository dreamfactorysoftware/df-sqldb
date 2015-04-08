<?php
/**
 * CDbColumnSchema class file.
 *
 * @author    Qiang Xue <qiang.xue@gmail.com>
 * @link      http://www.yiiframework.com/
 * @copyright Copyright &copy; 2008-2011 Yii Software LLC
 * @license   http://www.yiiframework.com/license/
 */
namespace DreamFactory\Rave\SqlDb\Driver\Schema;

/**
 * CDbColumnSchema class describes the column meta data of a database table.
 *
 * @author  Qiang Xue <qiang.xue@gmail.com>
 * @package system.db.schema
 * @since   1.0
 */
class CDbColumnSchema
{
    /**
     * @var string name of this column (without quotes).
     */
    public $name;
    /**
     * @var string raw name of this column. This is the quoted name that can be used in SQL queries.
     */
    public $rawName;
    /**
     * @var string the DB type of this column.
     */
    public $dbType;
    /**
     * @var string the DreamFactory simple type of this column.
     */
    public $type;
    /**
     * @var string the PHP type of this column.
     */
    public $phpType;
    /**
     * @var string the PHP PDO type of this column.
     */
    public $pdoType;
    /**
     * @var boolean whether this column can be null.
     */
    public $allowNull;
    /**
     * @var mixed default value of this column
     */
    public $defaultValue;
    /**
     * @var integer size of the column.
     */
    public $size;
    /**
     * @var integer precision of the column data, if it is numeric.
     */
    public $precision;
    /**
     * @var integer scale of the column data, if it is numeric.
     */
    public $scale;
    /**
     * @var boolean whether this column is a primary key
     */
    public $isPrimaryKey = false;
    /**
     * @var boolean whether this column has a unique constraint
     */
    public $isUnique = false;
    /**
     * @var boolean whether this column is indexed
     */
    public $isIndex = false;
    /**
     * @var boolean whether this column is a foreign key
     */
    public $isForeignKey = false;
    /**
     * @var string if a foreign key, then this is referenced table name
     */
    public $refTable;
    /**
     * @var string if a foreign key, then this is the referenced fields of the referenced table
     */
    public $refFields;
    /**
     * @var boolean whether this column is auto-incremental
     * @since 1.1.7
     */
    public $autoIncrement = false;
    /**
     * @var boolean whether this column supports
     * @since 1.1.7
     */
    public $supportsMultibyte = false;
    /**
     * @var boolean whether this column is auto-incremental
     * @since 1.1.7
     */
    public $fixedLength = false;
    /**
     * @var string comment of this column. Default value is empty string which means that no comment
     * has been set for the column. Null value means that RDBMS does not support column comments
     * at all (SQLite) or comment retrieval for the active RDBMS is not yet supported by the framework.
     * @since 1.1.13
     */
    public $comment = '';

    /**
     * Extracts the PHP type from DF type.
     *
     * @param string $type DF type
     *
     * @return string
     */
    public static function extractPhpType( $type )
    {
        switch ( $type )
        {
            case 'boolean':
                return 'boolean';

            case 'integer':
            case 'id':
            case 'reference':
                return 'integer';

            case 'decimal':
            case 'double':
            case 'float':
            case 'money':
                return 'double';

            case 'string':
            case 'text':
            case 'binary':
            case 'date':
            case 'time':
            case 'datetime':
            case 'timestamp':
            default:
                return 'string';
        }
    }

    /**
     * Extracts the PHP PDO type from DF type.
     *
     * @param string $type DF type
     *
     * @return int|null
     */
    public static function extractPdoType( $type )
    {
        switch ( $type )
        {
            case 'boolean':
                return \PDO::PARAM_BOOL;

            case 'integer':
            case 'id':
            case 'reference':
                return \PDO::PARAM_INT;

            case 'string':
                return \PDO::PARAM_STR;
        }

        return null;
    }

    /**
     * Extracts the DreamFactory simple type from DB type.
     *
     * @param string $dbType DB type
     */
    public function extractType( $dbType )
    {
        $_simpleType = strstr( $dbType, '(', true );
        $_simpleType = strtolower( $_simpleType ?: $dbType );

        switch ( $_simpleType )
        {
            case 'bit':
            case ( false !== strpos( $_simpleType, 'bool' ) ):
                $this->type = 'boolean';
                break;

            case 'number': // Oracle for boolean, integers and decimals
                if ( $this->size == 1 )
                {
                    $this->type = 'boolean';
                }
                elseif ( empty( $this->scale ) )
                {
                    $this->type = 'integer';
                }
                else
                {
                    $this->type = 'decimal';
                }
                break;

            case 'decimal':
            case 'numeric':
            case 'percent':
                $this->type = 'decimal';
                break;

            case ( false !== strpos( $_simpleType, 'double' ) ):
                $this->type = 'double';
                break;

            case 'real':
            case ( false !== strpos( $_simpleType, 'float' ) ):
                if ( $this->size == 53 )
                {
                    $this->type = 'double';
                }
                else
                {
                    $this->type = 'float';
                }
                break;

            case ( false !== strpos( $_simpleType, 'money' ) ):
                $this->type = 'money';
                break;

            case 'tinyint':
            case 'smallint':
            case 'mediumint':
            case 'bigint':
            case 'int':
            case 'integer':
                // watch out for point here!
                if ( $this->size == 1 )
                {
                    $this->type = 'boolean';
                }
                else
                {
                    $this->type = 'integer';
                }
                break;

            case ( false !== strpos( $_simpleType, 'timestamp' ) ):
            case 'datetimeoffset': //  MSSQL
                $this->type = 'timestamp';
                break;

            case ( false !== strpos( $_simpleType, 'datetime' ) ):
                $this->type = 'datetime';
                break;

            case 'date':
                $this->type = 'date';
                break;

            case ( false !== strpos( $_simpleType, 'time' ) ):
                $this->type = 'time';
                break;

            case ( false !== strpos( $_simpleType, 'binary' ) ):
            case ( false !== strpos( $_simpleType, 'blob' ) ):
                $this->type = 'binary';
                break;

            //	String types
            case ( false !== strpos( $_simpleType, 'clob' ) ):
            case ( false !== strpos( $_simpleType, 'text' ) ):
                $this->type = 'text';
                break;

            case 'varchar':
                if ( $this->size == -1 )
                {
                    $this->type = 'text'; // varchar(max) in MSSQL
                }
                else
                {
                    $this->type = 'string';
                }
                break;

            case 'string':
            case ( false !== strpos( $_simpleType, 'char' ) ):
            default:
                $this->type = 'string';
                break;
        }

        $this->phpType = static::extractPhpType( $this->type );
        $this->pdoType = static::extractPdoType( $this->type );
    }

    /**
     * Extracts size, precision and scale information from column's DB type.
     *
     * @param string $dbType the column's DB type
     */
    public function extractLimit( $dbType )
    {
        if ( strpos( $dbType, '(' ) && preg_match( '/\((.*)\)/', $dbType, $matches ) )
        {
            $values = explode( ',', $matches[1] );
            $this->size = (int)$values[0];
            if ( isset( $values[1] ) )
            {
                $this->precision = (int)$values[0];
                $this->scale = (int)$values[1];
            }
        }
    }

    /**
     * Extracts the default value for the column.
     * The value is typecasted to correct PHP type.
     *
     * @param mixed $defaultValue the default value obtained from metadata
     */
    public function extractDefault( $defaultValue )
    {
        $this->defaultValue = $this->typecast( $defaultValue );
    }

    /**
     * Converts the input value to the type that this column is of.
     *
     * @param mixed $value input value
     *
     * @return mixed converted value
     */
    public function typecast( $value )
    {
        if ( gettype( $value ) === $this->phpType || $value === null || $value instanceof CDbExpression )
        {
            return $value;
        }
        if ( $value === '' && $this->allowNull )
        {
            return $this->phpType === 'string' ? '' : null;
        }
        switch ( $this->phpType )
        {
            case 'string':
                return (string)$value;
            case 'integer':
                return (integer)$value;
            case 'boolean':
                return (boolean)$value;
            case 'double':
            default:
                return $value;
        }
    }

    /**
     * @param $dbType
     *
     * @return bool
     */
    public function extractMultiByteSupport( $dbType )
    {
        switch ( $dbType )
        {
            case ( false !== strpos( $dbType, 'national' ) ):
            case ( false !== strpos( $dbType, 'nchar' ) ):
            case ( false !== strpos( $dbType, 'nvarchar' ) ):
                $this->supportsMultibyte = true;
                break;
        }
    }

    /**
     * @param $dbType
     *
     * @return bool
     */
    public function extractFixedLength( $dbType )
    {
        switch ( $dbType )
        {
            case ( ( false !== strpos( $dbType, 'char' ) ) && ( false === strpos( $dbType, 'var' ) ) ):
            case 'binary':
                $this->fixedLength = true;
                break;
        }
    }
}
