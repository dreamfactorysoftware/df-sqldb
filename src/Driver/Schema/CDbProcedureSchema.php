<?php
/**
 * CDbProcedureSchema class file.
 *
 * @author    Lee Hicks <leehicks@dreamfactory.com>
 * @link      http://www.dreamfactory.com/
 * @copyright Copyright &copy; 2008-2014 DreamFactory Software LLC
 * @license   http://www.dreamfactory.com/license/
 */
namespace DreamFactory\Rave\SqlDb\Driver\Schema;

/**
 * CDbProcedureSchema is the base class for representing the metadata of a database procedure.
 *
 * It may be extended by different DBMS driver to provide DBMS-specific procedure metadata.
 *
 * CDbProcedureSchema provides the following information about a procedure:
 * <ul>
 * <li>{@link name}</li>
 * <li>{@link rawName}</li>
 * </ul>
 *
 * @author  Lee Hicks <leehicks@dreamfactory.com>
 * @package system.db.schema
 * @since   1.0
 */
class CDbProcedureSchema
{
    /**
     * @var string name of the schema that this procedure belongs to.
     */
    public $schemaName;
    /**
     * @var string name of this procedure.
     */
    public $name;
    /**
     * @var string raw name of this procedure. This is the quoted version of procedure name with optional schema name. It can be directly used in SQLs.
     */
    public $rawName;
    /**
     * @var string public display name of this procedure. This is the procedure name with optional non-default schema name. It is to be used by clients.
     */
    public $displayName;

}
