<?php
/**
 * CDbTableSchema class file.
 *
 * @author    Qiang Xue <qiang.xue@gmail.com>
 * @link      http://www.yiiframework.com/
 * @copyright Copyright &copy; 2008-2011 Yii Software LLC
 * @license   http://www.yiiframework.com/license/
 */
namespace DreamFactory\Rave\SqlDb\Driver\Schema;

/**
 * CDbTableSchema is the base class for representing the metadata of a database table.
 *
 * It may be extended by different DBMS driver to provide DBMS-specific table metadata.
 *
 * CDbTableSchema provides the following information about a table:
 * <ul>
 * <li>{@link name}</li>
 * <li>{@link rawName}</li>
 * <li>{@link columns}</li>
 * <li>{@link primaryKey}</li>
 * <li>{@link foreignKeys}</li>
 * <li>{@link sequenceName}</li>
 * </ul>
 *
 * @property array $columnNames List of column names.
 *
 * @author  Qiang Xue <qiang.xue@gmail.com>
 * @package system.db.schema
 * @since   1.0
 */
class CDbTableSchema
{
    /**
     * @var string name of the schema that this table belongs to.
     */
    public $schemaName;
    /**
     * @var string name of this table.
     */
    public $name;
    /**
     * @var string raw name of this table. This is the quoted version of table name with optional schema name. It can be directly used in SQLs.
     */
    public $rawName;
    /**
     * @var string public display name of this table. This is the table name with optional non-default schema name. It is to be used by clients.
     */
    public $displayName;
    /**
     * @var string|array primary key name of this table. If composite key, an array of key names is returned.
     */
    public $primaryKey;
    /**
     * @var string sequence name for the primary key. Null if no sequence.
     */
    public $sequenceName;
    /**
     * @var array foreign keys of this table. The array is indexed by column name. Each value is an array of foreign table name and foreign column name.
     */
    public $foreignKeys = array();
    /**
     * @var array foreign table references to this table. Each value is an array containing foreign table name, foreign column name, local column name, and reference type.
     */
    public $references = array();
    /**
     * @var array column metadata of this table. Each array element is a CDbColumnSchema object, indexed by column names.
     */
    public $columns = array();

    /**
     * Gets the named column metadata.
     * This is a convenient method for retrieving a named column even if it does not exist.
     *
     * @param string $name column name
     *
     * @return CDbColumnSchema metadata of the named column. Null if the named column does not exist.
     */
    public function getColumn( $name )
    {
        return isset( $this->columns[$name] ) ? $this->columns[$name] : null;
    }

    /**
     * @return array list of column names
     */
    public function getColumnNames()
    {
        return array_keys( $this->columns );
    }

    public function addReference( $type, $ref_table, $ref_field, $field, $join = null )
    {
        switch ( $type )
        {
            case 'belongs_to':
                $name = $ref_table . '_by_' . $field;
                break;
            case 'has_many':
                $name = $ref_table . '_by_' . $ref_field;
                break;
            case 'many_many':
                $name = $ref_table . '_by_' . substr( $join, 0, strpos( $join, '(' ) );
                break;
            default:
                $name = null;
                break;
        }

        $reference = [
            'name'      => $name,
            'type'      => $type,
            'ref_table' => $ref_table,
            'ref_field' => $ref_field,
            'field'     => $field,
            'join'      => $join
        ];

        $this->references[] = $reference;
    }
}
