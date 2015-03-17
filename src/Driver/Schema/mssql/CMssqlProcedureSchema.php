<?php
/**
 * CMssqlProcedureSchema class file.
 *
 * @author    Qiang Xue <qiang.xue@gmail.com>
 * @link      http://www.yiiframework.com/
 * @copyright Copyright &copy; 2008-2011 Yii Software LLC
 * @license   http://www.yiiframework.com/license/
 */

/**
 * CMssqlProcedureSchema represents the metadata for a MSSQL stored procedure.
 *
 * @author  Qiang Xue <qiang.xue@gmail.com>
 * @package system.db.schema.mssql
 * @since   1.0
 */
class CMssqlProcedureSchema extends CDbProcedureSchema
{
    /**
     * @var string name of the schema (database) that this procedure belongs to.
     * Defaults to null, meaning no schema (or the current database).
     */
    public $schemaName;
}
