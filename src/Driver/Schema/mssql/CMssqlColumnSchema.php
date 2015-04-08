<?php
/**
 * CMssqlColumnSchema class file.
 *
 * @author    Qiang Xue <qiang.xue@gmail.com>
 * @author    Christophe Boulain <Christophe.Boulain@gmail.com>
 * @link      http://www.yiiframework.com/
 * @copyright Copyright &copy; 2008-2011 Yii Software LLC
 * @license   http://www.yiiframework.com/license/
 */

namespace DreamFactory\Rave\SqlDb\Driver\Schema\Mssql;

use DreamFactory\Rave\SqlDb\Driver\Schema\CDbColumnSchema;

/**
 * CMssqlColumnSchema class describes the column meta data of a MSSQL table.
 *
 * @author  Qiang Xue <qiang.xue@gmail.com>
 * @author  Christophe Boulain <Christophe.Boulain@gmail.com>
 * @package system.db.schema.mssql
 */
class CMssqlColumnSchema extends CDbColumnSchema
{
    /**
     * Extracts the PHP type from DB type.
     *
     * @param string $dbType DB type
     */
    public function extractType( $dbType )
    {
        parent::extractType( $dbType );

        if ( ( false !== strpos( $dbType, 'varchar' ) ) && ( null === $this->size ) )
        {
            $this->type = 'text';
        }
        // bigint too big to represent as number in php
        if ( 0 === strpos( $dbType, 'bigint' ) )
        {
            $this->phpType = 'string';
            $this->pdoType = 'string';
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
        if ( $defaultValue == '(NULL)' )
        {
            $this->defaultValue = null;
        }
        elseif ( $this->type === 'boolean' )
        {
            if ( '((1))' === $defaultValue )
            {
                $this->defaultValue = true;
            }
            elseif ( '((0))' === $defaultValue )
            {
                $this->defaultValue = false;
            }
            else
            {
                $this->defaultValue = null;
            }
        }
        elseif ( $this->type === 'timestamp' )
        {
            $this->defaultValue = null;
        }
        else
        {
            parent::extractDefault( str_replace( array( '(', ')', "'" ), '', $defaultValue ) );
        }
    }

    /**
     * Extracts size, precision and scale information from column's DB type.
     * We do nothing here, since sizes and precisions have been computed before.
     *
     * @param string $dbType the column's DB type
     */
    public function extractLimit( $dbType )
    {
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
        if ( $this->phpType === 'boolean' )
        {
            return $value ? 1 : 0;
        }
        else
        {
            return parent::typecast( $value );
        }
    }
}
